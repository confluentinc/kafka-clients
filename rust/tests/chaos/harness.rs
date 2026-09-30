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

use confluent_kafka::admin::{Admin, AdminClientConfig, CreateTopicsResult, KafkaAdminClient, NewTopic};
use confluent_kafka::common::{TopicCollection, Uuid};

use super::common::broker_control::BrokerControl;
use super::common::cluster_config::kip848_3_broker;
use super::common::kafka_cluster::KafkaCluster;
use super::config::SecurityProtocol;
use super::isolation::{ClusterTeardown, Heartbeat, WorkloadThreads};
use super::verifier::{ConservationVerifier, ExpectedLossHint, Verifier};
use super::workload::{CommitMode, Role, TopicIds, WorkloadContext, WorkloadSpec};
use super::workload_config::{record_size_limit, security_props};

/// Producer value size when the caller does not choose one (`--msg-size`'s
/// default).
const DEFAULT_MSG_SIZE: usize = 100;

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
    /// Broker listener / client `security.protocol` every client in the run
    /// (admin + workloads) connects through (`--security-protocol`).
    security_protocol: SecurityProtocol,
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
    /// unresolved (treated as `Uuid::zero()`). Shared with every workload (via
    /// [`WorkloadContext::topic_ids`]) so they stamp records with the live id.
    topic_ids: TopicIds,
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
    /// Every workload thread of the run (see [`WorkloadThreads`]), shared
    /// with [`RunningWorkloads`] and the [`WorkloadPool`].
    workload_threads: Arc<WorkloadThreads>,
    /// Beaten by [`RunningWorkloads::drive`] while it runs; see
    /// [`super::isolation::start_heartbeat_watchdog`].
    heartbeat: Arc<Heartbeat>,
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
        Self::start_with_topics(
            &[topic.to_string()],
            brokers,
            partitions,
            None,
            SecurityProtocol::Plaintext,
            DEFAULT_MSG_SIZE,
            verifier,
        )
        .await
    }

    /// The multi-topic entry point used by the flag-driven runner. Creates every
    /// topic in `topics` with `partitions` partitions at `replication` (or
    /// `min(brokers, 3)` when `None`). Panics (librdkafka's FATAL) if an explicit
    /// replication exceeds the broker count. Every client of the run — the
    /// harness's admin and the workloads — connects through the broker listener
    /// selected by `security_protocol` (`--security-protocol`). `msg_size` is
    /// the producer's value size (`--msg-size`): a value too large for the
    /// broker's default `message.max.bytes` raises it cluster-wide
    /// ([`record_size_limit`]).
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_topics(
        topics: &[String],
        brokers: u16,
        partitions: i32,
        replication: Option<i16>,
        security_protocol: SecurityProtocol,
        msg_size: usize,
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
        // The 3-broker preset pins the offsets topic to RF 3. With fewer brokers
        // the coordinator can never auto-create `__consumer_offsets`
        // (InvalidReplicationFactor), FindCoordinator answers
        // COORDINATOR_NOT_AVAILABLE forever, no consumer joins, and the verdict
        // is total loss blamed on the client. Size it to the cluster, and create
        // it before any broker is stopped (`ensure_offsets_topic`).
        config
            .server_properties
            .insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), brokers.min(3).to_string());
        // Disable broker-side auto topic creation: the topic-recreate action
        // deletes the topic while workloads keep running, and their metadata
        // requests would otherwise silently recreate it with the broker's
        // default partition count — racing (and conflicting with) our explicit
        // recreate. The chaos harness owns topic lifecycle explicitly.
        config
            .server_properties
            .insert("KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(), "false".to_string());
        // A 1 MiB record batch is just over the broker's default
        // `message.max.bytes`; without this every such produce is rejected
        // with RecordTooLarge. Replica fetches need no change: a follower
        // always receives at least one batch whatever `replica.fetch.max.bytes`
        // says (KIP-74), and so does a consumer fetch.
        if let Some(limit) = record_size_limit(msg_size) {
            eprintln!("chaos: raising broker message.max.bytes to {limit} for {msg_size}-byte records");
            config
                .server_properties
                .insert("KAFKA_MESSAGE_MAX_BYTES".to_string(), limit.to_string());
        }
        let cluster = KafkaCluster::start_with_config(&config).await;

        // The harness's own admin (create/delete topic, elect leaders,
        // reassign) stays on the PLAINTEXT listener regardless of
        // `security_protocol`: on this branch `KafkaAdminClient` hardcodes a
        // plaintext channel (`kafka_admin_client.rs` passes
        // `SecurityProtocol::Plaintext` to `client_channel_builder`) and
        // `AdminClientConfig` has no security keys, so an admin pointed at a
        // TLS / SASL listener never connects ("Timed out waiting for a node
        // assignment"). Newer branches wire `config.security_protocol()` /
        // `ssl_config()` / `sasl_config()` through; once this branch has that,
        // switch these props to `protocol_bootstrap` + `security_props` so the
        // control plane is exercised over the secured listener too. The
        // workloads — the clients under test — already use it.
        let admin: Box<dyn Admin> = {
            let props = HashMap::from([
                ("bootstrap.servers".to_string(), cluster.bootstrap_servers().to_string()),
                ("client.id".to_string(), "chaos-admin".to_string()),
            ]);
            Box::new(
                KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config"))
                    .expect("admin client"),
            )
        };
        if security_protocol != SecurityProtocol::Plaintext {
            eprintln!(
                "chaos: workloads connect over {} ({}); the harness admin stays on PLAINTEXT",
                security_protocol.config_value(),
                Self::listener_bootstrap(&cluster, security_protocol)
            );
        }

        let harness = Self {
            cluster,
            security_protocol,
            admin,
            topics: topics.to_vec(),
            partitions,
            replication,
            topic_ids: Arc::new(std::sync::Mutex::new(HashMap::new())),
            recreate_settle: Arc::new(RecreateSettle::default()),
            rps: std::sync::atomic::AtomicU32::new(0),
            msg_size: std::sync::atomic::AtomicU32::new(100),
            commit_mode: std::sync::Mutex::new(CommitMode::Sync),
            verifier,
            workload_threads: Arc::new(WorkloadThreads::default()),
            heartbeat: Heartbeat::new(),
        };
        for t in &harness.topics {
            harness.create_topic(t).await;
        }
        harness.ensure_offsets_topic(Duration::from_secs(60)).await;
        harness
    }

    /// Make the broker create `__consumer_offsets` now, while every broker is
    /// up, and wait until each of its partitions has a leader.
    ///
    /// The broker creates it lazily, on the first FindCoordinator for a group,
    /// at the configured RF. The runner stops the `--leave-broker-down` broker
    /// right after `start_with_topics` returns and before any consumer exists,
    /// so with the lazy creation the RF-`min(brokers, 3)` topic could need more
    /// brokers than were left unfenced: InvalidReplicationFactor,
    /// COORDINATOR_NOT_AVAILABLE forever, and a total-loss verdict blamed on the
    /// client. Created up front, the topic keeps its RF and only runs with one
    /// replica out of sync while that broker is down, as any other topic does.
    ///
    /// The describe of the run's (still empty) group is only the trigger: its
    /// FindCoordinator makes the broker create the topic, whatever the describe
    /// itself answers.
    async fn ensure_offsets_topic(&self, timeout: Duration) {
        const OFFSETS_TOPIC: &str = "__consumer_offsets";
        let group = format!("chaos-group-{}", self.primary_topic());
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                self.admin.describe_consumer_groups(std::slice::from_ref(&group)).all().get(),
            )
            .await;
            let described = tokio::time::timeout(
                Duration::from_secs(5),
                self.admin
                    .describe_topics_with_topics(TopicCollection::of_topic_names(vec![OFFSETS_TOPIC.to_string()]))
                    .all_topic_names()
                    .expect("describe by name yields a name-keyed result")
                    .get(),
            )
            .await;
            if let Ok(Ok(map)) = described
                && let Some(desc) = map.get(OFFSETS_TOPIC)
                && !desc.partitions().is_empty()
                && desc.partitions().iter().all(|p| p.leader().is_some())
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{OFFSETS_TOPIC} was not created within {timeout:?}; no consumer could find its group coordinator"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// The primary topic (first in the run) — the broker-roll / smoke-test
    /// leader-sampling target, and the `WorkloadContext::topic` fallback.
    fn primary_topic(&self) -> &str {
        &self.topics[0]
    }

    /// Host-loopback bootstrap for `cluster`'s listener of `protocol`. Every
    /// broker exposes all four listeners; this picks the one the run's clients
    /// use.
    fn listener_bootstrap(cluster: &KafkaCluster, protocol: SecurityProtocol) -> &str {
        match protocol {
            SecurityProtocol::Plaintext => cluster.bootstrap_servers(),
            SecurityProtocol::Ssl => cluster.ssl_bootstrap_servers(),
            SecurityProtocol::SaslPlaintext => cluster.sasl_plaintext_bootstrap_servers(),
            SecurityProtocol::SaslSsl => cluster.sasl_ssl_bootstrap_servers(),
        }
    }

    /// Host-loopback bootstrap for the run's `--security-protocol` listener —
    /// what in-process (Rust) clients connect to.
    pub fn protocol_bootstrap(&self) -> &str {
        Self::listener_bootstrap(&self.cluster, self.security_protocol)
    }

    /// Container-network bootstrap for the run's `--security-protocol`
    /// listener — what a gRPC backend's client connects to when its server runs
    /// in a sibling container on the cluster's network. `None` for
    /// SASL_PLAINTEXT, the one protocol with no CONTAINER listener;
    /// `ChaosConfig::from_env` rejects that combination up front.
    pub fn container_protocol_bootstrap(&self) -> Option<&str> {
        match self.security_protocol {
            SecurityProtocol::Plaintext => Some(self.cluster.container_bootstrap_servers()),
            SecurityProtocol::Ssl => Some(self.cluster.container_ssl_bootstrap_servers()),
            SecurityProtocol::SaslSsl => Some(self.cluster.container_sasl_ssl_bootstrap_servers()),
            SecurityProtocol::SaslPlaintext => None,
        }
    }

    /// Client-side security keys matching [`Self::protocol_bootstrap`]
    /// (`security.protocol`, the cluster CA truststore, SASL/PLAIN
    /// credentials); empty for PLAINTEXT.
    pub fn security_props(&self) -> HashMap<String, String> {
        security_props(self.security_protocol, self.cluster.ca_cert_pem())
    }

    /// Create `topic` and cache its id (see [`Self::cache_created_topic_id`]).
    async fn create_topic(&self, topic: &str) {
        let new_topic = NewTopic::with_num_partitions_replication_factor(
            topic.to_string(),
            Some(self.partitions),
            Some(self.replication),
        );
        let result = self.admin.create_topics(&[new_topic]);
        result.all().get().await.expect("chaos topic creation failed");
        self.cache_created_topic_id(topic, &result).await;
    }

    /// Cache the id the controller assigned to `topic`, taken from the
    /// create-topics response itself rather than a follow-up describe.
    ///
    /// The timing matters. The producer workload stamps each delivered record
    /// with the id in this map at acknowledgement time (`delivery_callback` in
    /// `workload.rs`). The create response returns as soon as the controller
    /// has registered the new topic; the producer cannot learn the new leaders,
    /// let alone be acknowledged by one, until its own metadata refresh sees the
    /// topic afterwards. Switching the map here therefore guarantees that no
    /// record of the new generation is stamped with the old id. A describe
    /// round-trip after the create (the previous approach) left a window in
    /// which records written to the new generation carried the destroyed
    /// generation's id and were silently excused.
    ///
    /// Falls back to `describe_topics` when the response carries no id (a
    /// broker too old to return one, or a create that raced a still-pending
    /// delete of the same name and reported the existing topic).
    async fn cache_created_topic_id(&self, topic: &str, result: &CreateTopicsResult) {
        match result.topic_id(topic).get().await {
            Ok(id) if id != Uuid::zero() => {
                self.topic_ids.lock().expect("topic_ids poisoned").insert(topic.to_string(), id);
            },
            other => {
                eprintln!(
                    "chaos: create-topics response carried no usable id for {topic} ({other:?}); \
                     resolving it through describe"
                );
                self.resolve_topic_id(topic).await;
            },
        }
    }

    /// Resolve the current topic id (for the physical verification key) via
    /// `describe_topics`, and cache it. **Retries until a non-zero id is
    /// resolved**, bounded by `timeout` — under a concurrent broker roll,
    /// `describe_topics` can transiently return a zero/absent id (metadata
    /// churn), and keying post-recreate records under `Uuid::zero()` corrupts the
    /// physical accounting. Logs loudly if it genuinely cannot resolve within the
    /// bound (leaving the previous cached value untouched). The fallback for a
    /// create-topics response without an id; see [`Self::cache_created_topic_id`].
    async fn resolve_topic_id(&self, topic: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let described = self
                .admin
                .describe_topics_with_topics(TopicCollection::of_topic_names(vec![topic.to_string()]))
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
        // What the delete destroys: for a single-topic run every delivered
        // record, for a multi-topic run only this topic's records. Snapshotted
        // once the topic is confirmed gone, below.
        let expected_lost_hint = || {
            if self.topics.len() == 1 {
                ExpectedLossHint::AllDeliveredSoFar
            } else {
                ExpectedLossHint::AllDeliveredForTopic(topic.to_string())
            }
        };
        let delete_started = std::time::Instant::now();

        self.admin
            .delete_topics(confluent_kafka::common::TopicCollection::of_topic_names(vec![
                topic.to_string(),
            ]))
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

        // Snapshot the topic's delivered records as expected-lost only now that
        // it is confirmed gone. The producer keeps running through the delete,
        // so a snapshot taken before it missed every record acked between the
        // snapshot and the delete: neither excused nor ever consumable, a false
        // loss. The old leader answers or fails its requests before it drops the
        // partition, so no old-generation ack lands after this point (see
        // `delivery_callback` in `workload.rs`); a late one would carry the old
        // id and fall to `DestroyedGeneration` below. The snapshot is what
        // decides only when the recreate reuses the id or cannot resolve it: for
        // identified generations the verifier judges by id (`excused`).
        self.verifier.note_expected_loss(expected_lost_hint());

        // Mark the topic as a recreate BLACKOUT now, while it does not exist:
        // the blackout floor (the highest index delivered so far) must be taken
        // before the new generation can acknowledge anything. Taken after the
        // create, as this once was, records the new generation acknowledged in
        // between fell at or below the floor, were classified as old-generation
        // and could never be excused (the floor race, Sep 2026 matrix F9).
        //
        // Why a blackout at all: the consumer can carry its old-generation
        // position or committed offset into the new generation, which starts
        // at offset 0 again, so it skips the new generation's low offsets.
        // The in-process producer's index does NOT reset across the recreate
        // (librdkafka restarts its per-topic producer; we cannot), so it keeps
        // acking records into that window. Rather than guess the window with a
        // wall-clock snapshot, the verifier computes it from observed data: a
        // delivered-but-unobserved new-generation record below where the
        // consumer started reading that partition was skipped and is
        // expected-lost; records above remain in the loss check. See
        // `ExpectedLossHint::RecreateBlackout` / `NewGeneration`.
        self.verifier
            .note_expected_loss(ExpectedLossHint::RecreateBlackout(topic.to_string()));

        if !dwell.is_zero() {
            tokio::time::sleep(dwell).await;
        }

        // Recreate with the same shape, retrying while the broker still reports
        // the old topic as not-yet-collected. This also switches the shared id
        // map to the new generation, from the create response, before the
        // producer can be acknowledged by it (see `cache_created_topic_id`).
        // (librdkafka restarts the topic's producer here; our in-process
        // producer keeps running against the same topic name.) From here on the
        // producer stamps acks and the consumer keys reads with the new id.
        self.create_topic_retrying(topic, Duration::from_secs(30)).await;
        let new_id = self.current_topic_id(topic);

        eprintln!("chaos: topic {topic} recreated: id {old_id} -> {new_id}");

        // The old generation's records are gone. Mark its topic_id destroyed —
        // this also covers acks that land AFTER the topic vanished from metadata
        // (the async-ack race the point-in-time snapshot above cannot close).
        // The verifier excuses only the records of it that were still unread
        // when the delete began; see `DestroyedGeneration`. And mark the new
        // generation, so its skipped head is judged by id rather than by the
        // index floor. Only when the recreate minted a genuinely new id (KRaft
        // always does); a recreate that reused the id or could not resolve it
        // relies on the snapshot and the blackout floor, since old and new
        // records then cannot be told apart by topic_id.
        if old_id != new_id && old_id != Uuid::zero() {
            self.verifier
                .note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: delete_started });
            if new_id != Uuid::zero() {
                self.verifier.note_expected_loss(ExpectedLossHint::NewGeneration(new_id));
            }
        }

        // Effect check — but only for recreate-DELAYED. With a dwell long enough
        // for the deletion to propagate, the recreate MUST be a genuinely new
        // generation (different topic id). recreate-IMMEDIATE (dwell 0) can
        // legitimately reuse the same id (in-place topic_id mutation — librdkafka
        // documents this as a valid mode), especially when the cluster metadata
        // is still churning from a concurrent broker roll, so we do not assert
        // there. Either way the map now holds the create response's id for keying.
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
                .describe_topics_with_topics(confluent_kafka::common::TopicCollection::of_topic_names(vec![
                    topic.to_string(),
                ]))
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
    /// generation as pending deletion (`TopicAlreadyExists`), and cache the new
    /// generation's id from the create response (see
    /// [`Self::cache_created_topic_id`]).
    async fn create_topic_retrying(&self, topic: &str, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let new_topic = NewTopic::with_num_partitions_replication_factor(
                topic.to_string(),
                Some(self.partitions),
                Some(self.replication),
            );
            let result = self.admin.create_topics(&[new_topic]);
            match result.all().get().await {
                Ok(()) => {
                    self.cache_created_topic_id(topic, &result).await;
                    return;
                },
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

    /// Every workload thread of the run. The runner stops and waits for them
    /// when it abandons a run ([`WorkloadThreads::stop_and_wait`]).
    pub fn workload_threads(&self) -> Arc<WorkloadThreads> {
        self.workload_threads.clone()
    }

    /// The scenario heartbeat [`RunningWorkloads::drive`] beats; hand it to
    /// [`super::isolation::start_heartbeat_watchdog`].
    pub fn heartbeat(&self) -> Arc<Heartbeat> {
        self.heartbeat.clone()
    }

    /// What removing this cluster takes, for a thread that must do it without
    /// the harness (the heartbeat watchdog). [`Drop`] uses the same.
    pub fn cluster_teardown(&self) -> ClusterTeardown {
        ClusterTeardown {
            containers: self.cluster.container_ids().to_vec(),
            network: self.cluster.network_name().to_string(),
        }
    }

    /// Build the immutable per-workload context from current cluster state,
    /// bound to `topic` as the producer's target topic. Consumers ignore
    /// `topic` and subscribe to the full `topics` set. Shared by
    /// `build_workloads` and the runtime `WorkloadPool`. The group is keyed on
    /// the primary topic so every consumer joins the same group.
    fn workload_ctx(&self, topic: &str) -> WorkloadContext {
        WorkloadContext {
            bootstrap: self.protocol_bootstrap().to_string(),
            container_bootstrap: self.container_protocol_bootstrap().unwrap_or_default().to_string(),
            security: self.security_props(),
            topic: topic.to_string(),
            topics: self.topics.clone(),
            topic_ids: self.topic_ids.clone(),
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

        // Every producer numbers its records from 0 per topic, and the verifier
        // identifies a record by `(topic, index)`: a second producer spec would
        // write the same keys into the same topics and corrupt the accounting
        // (false double writes, hidden loss). `ChaosConfig::from_env` rejects it
        // on the CLI; this catches any other caller.
        assert!(
            specs.iter().filter(|s| s.role == Role::Producer).count() <= 1,
            "at most one producer workload spec per run: the verifier keys records on (topic, index)"
        );
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
                            // topics -> 100, 101, 102).
                            s.instance = spec.instance * 100 + topic_idx as u32;
                            s
                        };
                        running.push((per_topic_spec, ctx));
                    }
                },
                // One consumer per spec, subscribing to ALL topics.
                Role::Consumer => {
                    running.push((spec.clone(), self.workload_ctx(self.primary_topic())));
                },
            }
        }

        RunningWorkloads {
            workloads: running,
            threads: self.workload_threads.clone(),
            heartbeat: self.heartbeat.clone(),
            verifier: self.verifier.clone(),
            broker_network: self.cluster.network_name().to_string(),
        }
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
        self.cluster_teardown().run();
    }
}

/// Shared inner state of a [`WorkloadPool`], held behind `Rc` so the scenario
/// future can capture a clone.
struct WorkloadPoolInner {
    /// Stop flags of consumers added at runtime, newest last — `remove` pops.
    added_consumer_stops: std::cell::RefCell<Vec<Arc<AtomicBool>>>,
    next_instance: std::cell::Cell<u32>,
}

/// Handle the scenario uses to add or remove **consumer** workloads mid-run,
/// forcing a group rebalance (librdkafka's `--rebalance-add-cycle` /
/// `--rebalance-remove-cycle`). `Rc`-cloneable and not `Send`/`Sync` — it lives
/// on the scenario task. Obtain one from [`ChaosHarness::workload_pool`] and
/// capture it in the scenario. Added consumers run on their own threads in the
/// harness's [`WorkloadThreads`], so `drive` stops them at the end of the drain
/// along with the base consumers.
#[derive(Clone)]
pub struct WorkloadPool<'h> {
    harness: &'h ChaosHarness,
    ctx: WorkloadContext,
    inner: std::rc::Rc<WorkloadPoolInner>,
}

impl<'h> WorkloadPool<'h> {
    /// Start a consumer workload of `backend` on its own thread. It joins the
    /// group on its first poll, triggering a rebalance.
    pub async fn add_consumer(&self, backend: super::workload::Backend) {
        let instance = self.inner.next_instance.get();
        self.inner.next_instance.set(instance + 1);
        let spec = WorkloadSpec { role: Role::Consumer, backend, instance };
        let label = spec.label();
        let stop = self
            .harness
            .workload_threads
            .spawn_workload(
                spec,
                self.ctx.clone(),
                self.harness.verifier.clone(),
                self.harness.cluster.network_name().to_string(),
            )
            .await;
        self.inner.added_consumer_stops.borrow_mut().push(stop);
        eprintln!("chaos: added consumer {label} (rebalance)");
    }

    /// Number of consumers added dynamically at runtime that are still live
    /// (i.e. removable). The churn loop reads this to stay within its bounds:
    /// the live consumer count is the fixed base plus this.
    pub fn dynamic_consumer_count(&self) -> usize {
        self.inner.added_consumer_stops.borrow().len()
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
    /// The workloads to start, with each one's context.
    workloads: Vec<(WorkloadSpec, WorkloadContext)>,
    threads: Arc<WorkloadThreads>,
    heartbeat: Arc<Heartbeat>,
    verifier: Arc<dyn Verifier>,
    broker_network: String,
}

/// How often the drive loop wakes to beat the heartbeat and check the
/// workload threads.
const DRIVE_TICK: Duration = Duration::from_millis(250);

impl RunningWorkloads {
    /// Start every workload on its own thread ([`WorkloadThreads`]), run
    /// `scenario` (the chaos timeline) on the caller's task, then perform the
    /// cooldown → drain: stop producers and wait for them to finish, give
    /// consumers `drain` to catch up on the tail, stop consumers (including
    /// any the scenario added through its [`WorkloadPool`]), and wait for every
    /// workload to finish. Mirrors the librdkafka cooldown→drain sequence.
    /// `drain` is the maximum cooldown wait; `idle_threshold` (if non-zero)
    /// ends the drain early once consumption has been quiet for that long
    /// (idle-based early drain — see `drain_wait`). `verifier` supplies the
    /// consume-progress signal.
    ///
    /// The workloads run on their own threads, so a client that never yields
    /// cannot stop the scenario from restarting brokers. This task only waits
    /// for them. A workload that panics aborts the drive with its panic
    /// message, after every workload has been told to stop.
    pub async fn drive<Fut>(
        self,
        drain: Duration,
        idle_threshold: Duration,
        verifier: Arc<dyn Verifier>,
        settle: Arc<RecreateSettle>,
        scenario: Fut,
    ) where
        Fut: std::future::Future<Output = ()>,
    {
        self.heartbeat.arm();
        for (spec, ctx) in self.workloads {
            self.threads
                .spawn_workload(spec, ctx, self.verifier.clone(), self.broker_network.clone())
                .await;
        }

        let threads = self.threads.clone();
        let control = async move {
            scenario.await;
            // (1) Stop producers, then wait for them to FINISH (not just be
            // signalled): a producer still flushes its backlog after the stop
            // flag is set (largest right after a recreate), and draining first
            // would deliver its tail with no live consumer — the flaky tail loss.
            threads.stop_role(Role::Producer);
            while !threads.all_finished(Some(Role::Producer)) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // (2) Drain until all delivered observed (or `drain` elapses); (3)
            // stop every consumer, including those added at runtime (consumer
            // churn / --rebalance-add-cycle): the registry holds them all.
            drain_wait(drain, idle_threshold, verifier.as_ref(), settle.as_ref()).await;
            threads.stop_role(Role::Consumer);
        };
        let mut control = std::pin::pin!(control);
        let mut control_done = false;
        let mut tick = tokio::time::interval(DRIVE_TICK);

        // Ends when the control future has finished AND every workload thread
        // has finished.
        loop {
            tokio::select! {
                biased;
                _ = &mut control, if !control_done => { control_done = true; }
                _ = tick.tick() => {}
            }
            self.heartbeat.beat();
            if let Some((label, message)) = self.threads.first_panic() {
                self.threads.stop_all();
                self.heartbeat.disarm();
                std::panic::panic_any(format!("workload {label} panicked: {message}"));
            }
            if control_done && self.threads.all_finished(None) {
                break;
            }
        }
        self.heartbeat.disarm();
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
