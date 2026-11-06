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

//! The clients of an integrity run and the [`run`] that drives them:
//!
//! - **writers** produce self-checking records, optionally in transactions,
//!   replacing their producer now and then (a new producer id each time);
//! - a **live group** reads every topic while the writers run, commits after
//!   every poll, now and then reads its commits back, and restarts its members,
//!   cycling through clean `close()`s and hard stops (the consumer abandoned,
//!   uncommitted records left behind, as a crash would);
//! - **copiers** run a consume-transform-produce loop with random aborts
//!   (Apache Kafka's `TransactionalMessageCopier`) and check their committed
//!   offsets;
//! - a **final read-back** reads every partition from the start with a fresh
//!   read_committed consumer once everything has stopped.
//!
//! Every client connects with the same base properties (bootstrap servers and
//! security), so a run works the same against a local cluster and an external
//! one. The run's [`Clients`] create them: the Rust client's, or a binding's.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig};
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::common::{Error, KafkaFuture, TopicPartition};
use confluent_kafka::consumer::{
    Consumer, ConsumerConfig, ConsumerHandle, ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata,
};
use confluent_kafka::producer::RecordMetadata;
use tokio::task::JoinHandle;

use super::checker::{Event, Ledger, Requirements, StreamKind, TxnOutcome, Verdict};
use super::clients::{BoxConsumer, BoxProducer, Clients};
use super::java::{JavaReader, java_final_read_back};
use super::record::{
    Identity, PayloadProfile, RecordGenerator, RecordId, SplitMix64, TxnIntent, build_record, copy_record,
};
use crate::common::test_utils;

/// Client properties.
pub type Props = HashMap<String, String>;

type Producer = dyn confluent_kafka::producer::DynProducer<Vec<u8>, Vec<u8>>;
type Reader = dyn confluent_kafka::consumer::DynConsumer<Vec<u8>, Vec<u8>>;

/// Records a writer may have in flight before it waits for the oldest.
const MAX_IN_FLIGHT: usize = 20_000;
/// The longest a delivery future may take once its producer was flushed,
/// committed or closed; one that takes longer is a hang.
const SETTLED_FUTURE_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_TIMEOUT: Duration = Duration::from_millis(200);
/// How long a failed client waits before it is created again.
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
/// A copier reads its committed offsets back after every this many commits.
const COPIER_READBACK_EVERY: u64 = 10;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_millis() as i64
}

/// A topic of the run.
#[derive(Clone, Debug)]
pub struct TopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replication: i16,
    pub kind: StreamKind,
}

impl TopicSpec {
    pub fn source(name: &str, partitions: i32, replication: i16) -> Self {
        Self { name: name.to_string(), partitions, replication, kind: StreamKind::Source }
    }

    /// The output topic of a copier reading `source`.
    pub fn mirror(name: &str, source: &str, partitions: i32, replication: i16) -> Self {
        Self {
            kind: StreamKind::Mirror(source.to_string()),
            ..Self::source(name, partitions, replication)
        }
    }
}

/// When a writer replaces its producer.
#[derive(Clone, Copy, Debug)]
pub enum Restart {
    EveryRecords(u64),
    Every(Duration),
}

#[derive(Clone, Debug)]
pub struct TxnSpec {
    pub transactional_id: String,
    pub records_per_txn: u32,
    /// One transaction in every this many is aborted ([`AbortPlan`]); 0
    /// never aborts.
    pub abort_one_in: u32,
}

/// One producer stream.
#[derive(Clone, Debug)]
pub struct WriterSpec {
    pub writer: u32,
    pub topic: String,
    pub profile: PayloadProfile,
    /// Producer properties on top of the base ones and the writer's own
    /// (`acks=all`, `enable.idempotence=true`).
    pub props: Props,
    pub transactions: Option<TxnSpec>,
    /// Stop after this many records; `None` writes until the run stops it.
    pub records: Option<u64>,
    /// Records per second; 0 writes as fast as the producer accepts them.
    pub rate_per_sec: u32,
    pub restart: Option<Restart>,
}

impl WriterSpec {
    pub fn new(writer: u32, topic: &str) -> Self {
        Self {
            writer,
            topic: topic.to_string(),
            profile: PayloadProfile::mixed(),
            props: Props::new(),
            transactions: None,
            records: None,
            rate_per_sec: 0,
            restart: None,
        }
    }

    pub fn profile(mut self, profile: PayloadProfile) -> Self {
        self.profile = profile;
        self
    }

    pub fn prop(mut self, key: &str, value: &str) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    pub fn transactions(mut self, transactional_id: &str, records_per_txn: u32, abort_one_in: u32) -> Self {
        self.transactions =
            Some(TxnSpec { transactional_id: transactional_id.to_string(), records_per_txn, abort_one_in });
        self
    }

    pub fn records(mut self, records: u64) -> Self {
        self.records = Some(records);
        self
    }

    pub fn rate(mut self, rate_per_sec: u32) -> Self {
        self.rate_per_sec = rate_per_sec;
        self
    }

    pub fn restart(mut self, restart: Restart) -> Self {
        self.restart = Some(restart);
        self
    }
}

/// How a live-group member ends before it is created again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestartMode {
    /// `close()`, after committing everything it read (a commit that fails is
    /// retried with the next one, or by the listener before the partitions
    /// move; only a retry that fails too leaves records the next owner reads
    /// again).
    Clean,
    /// Read one more batch, do not commit it, and stop polling without
    /// `close()`, as a stuck or crashed application does. Its partitions move
    /// once `max.poll.interval.ms` expires, and the next owner reads that batch
    /// again from the committed offset.
    ///
    /// An unclosed consumer cannot be killed in process: its background task
    /// runs on a thread of its own and keeps heartbeating, as Java's network
    /// thread does. So the member is abandoned instead, and closed only when
    /// the run stops.
    Hard,
}

#[derive(Clone, Debug)]
pub struct ConsumerRestart {
    pub every: Duration,
    /// A member's n-th restart is `modes[n % modes.len()]`.
    pub modes: Vec<RestartMode>,
}

/// The live group: every member subscribes to every topic of `topics`.
#[derive(Clone, Debug)]
pub struct GroupSpec {
    pub group_id: String,
    pub topics: Vec<String>,
    pub members: usize,
    pub restart: Option<ConsumerRestart>,
    /// How often a member reads its commit back (right after committing).
    pub readback_every: Duration,
    pub props: Props,
}

impl GroupSpec {
    pub fn new(group_id: &str, topics: Vec<String>, members: usize) -> Self {
        Self {
            group_id: group_id.to_string(),
            topics,
            members,
            restart: None,
            readback_every: Duration::from_secs(1),
            props: Props::new(),
        }
    }

    /// Restarts every member every `every`, cycling through `modes`. A hard
    /// stop holds its partitions for `max.poll.interval.ms` (five minutes by
    /// default), so a group with hard stops sets it lower with `prop`.
    pub fn restart(mut self, every: Duration, modes: &[RestartMode]) -> Self {
        assert!(!modes.is_empty(), "at least one restart mode");
        self.restart = Some(ConsumerRestart { every, modes: modes.to_vec() });
        self
    }

    pub fn prop(mut self, key: &str, value: &str) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }
}

/// A consume-transform-produce loop copying `input` into `output`, partition
/// for partition, in transactions.
#[derive(Clone, Debug)]
pub struct CopierSpec {
    pub input: String,
    pub output: String,
    pub group_id: String,
    pub transactional_id: String,
    /// One transaction in every this many is aborted ([`AbortPlan`]); 0
    /// never aborts.
    pub abort_one_in: u32,
    /// Copier consumer properties over the defaults, such as
    /// `max.poll.records`, which sets how many records a transaction carries.
    pub props: Props,
}

/// Which transactions to abort: exactly one in every `one_in` consecutive
/// ones, at a seeded position within each window. A coin flip per transaction
/// could abort none in a short run, which would leave the abort path untested.
struct AbortPlan {
    one_in: u64,
    rng: SplitMix64,
    index: u64,
    pick: u64,
}

impl AbortPlan {
    fn new(one_in: u32, seed: u64) -> Self {
        Self { one_in: u64::from(one_in), rng: SplitMix64::new(seed), index: 0, pick: 0 }
    }

    /// Whether the next transaction is to be aborted.
    fn next_aborts(&mut self) -> bool {
        if self.one_in == 0 {
            return false;
        }
        if self.index.is_multiple_of(self.one_in) {
            self.pick = self.rng.below(self.one_in);
        }
        let abort = self.index % self.one_in == self.pick;
        self.index += 1;
        abort
    }
}

/// Everything a run does.
#[derive(Clone, Debug)]
pub struct RunSpec {
    pub run_id: u64,
    pub seed: u64,
    pub topics: Vec<TopicSpec>,
    pub writers: Vec<WriterSpec>,
    pub copiers: Vec<CopierSpec>,
    pub group: Option<GroupSpec>,
    /// How long the writers run; `None` waits until every writer has written
    /// its `records`.
    pub duration: Option<Duration>,
    /// Once the writers are told to stop: how long each writer, copier and
    /// member gets to stop, and the live group to catch up.
    pub drain_timeout: Duration,
    /// How long the final read-back may go without moving forward: setting
    /// up, or a partition's position advancing.
    pub final_timeout: Duration,
    /// The run is stopped and fails when nothing is sent or read for this long.
    pub watchdog: Duration,
    /// Print a status line this often.
    pub status_every: Option<Duration>,
    /// When set, Apache Kafka's Java console consumer does the final
    /// read-back instead of the Rust consumer.
    pub java_reader: Option<JavaReader>,
    /// The clients the writers, the live group, the copiers and the final
    /// read-back use; the Rust client's by default.
    pub clients: Clients,
    pub requirements: Requirements,
}

impl RunSpec {
    pub fn new(run_id: u64, seed: u64) -> Self {
        Self {
            run_id,
            seed,
            topics: Vec::new(),
            writers: Vec::new(),
            copiers: Vec::new(),
            group: None,
            duration: None,
            drain_timeout: Duration::from_secs(120),
            final_timeout: Duration::from_secs(120),
            watchdog: Duration::from_secs(120),
            status_every: None,
            java_reader: None,
            clients: Clients::rust(),
            requirements: Requirements::default(),
        }
    }
}

fn merged(base: &Props, layers: &[(&str, String)], extra: &Props) -> Props {
    let mut props = base.clone();
    props.extend(layers.iter().map(|(k, v)| (k.to_string(), v.clone())));
    props.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
    props
}

pub fn admin(base: &Props) -> Box<dyn Admin> {
    AdminClient::create(AdminClientConfig::new(base).expect("valid admin config")).expect("admin client")
}

/// Creates every topic of the run and waits for their leaders.
pub async fn create_topics(base: &Props, topics: &[TopicSpec]) {
    let admin = admin(base);
    for topic in topics {
        // Records are checked against the timestamp their writer set.
        let configs = BTreeMap::from([("message.timestamp.type".to_string(), "CreateTime".to_string())]);
        test_utils::create_topic_with_configs(
            admin.as_ref(),
            &topic.name,
            topic.partitions,
            topic.replication,
            configs,
        )
        .await;
        test_utils::wait_for_partition_leaders(admin.as_ref(), &topic.name, 0..topic.partitions).await;
    }
    admin.close().await;
}

/// Commits offset 0 for every partition of `topics` in `group_id`, so its
/// members can run with `auto.offset.reset=none`: a member that ever needs a
/// reset policy has lost its position, and that fails the call instead of
/// silently skipping records (as Apache Kafka's trogdor verifiers do).
async fn seed_group_offsets(base: &Props, group_id: &str, topics: &[&TopicSpec]) {
    let props = merged(
        base,
        &[
            ("group.id", group_id.to_string()),
            ("group.protocol", "consumer".to_string()),
            ("enable.auto.commit", "false".to_string()),
        ],
        &Props::new(),
    );
    let mut consumer = KafkaConsumer::<Vec<u8>, Vec<u8>>::new(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        ByteArrayDeserializer::default(),
        ByteArrayDeserializer::default(),
    )
    .expect("seed consumer");
    let partitions: Vec<TopicPartition> = topics
        .iter()
        .flat_map(|t| (0..t.partitions).map(|p| TopicPartition::new(t.name.clone(), p)))
        .collect();
    consumer.assign(partitions.clone()).await.expect("assign");
    let offsets = partitions
        .into_iter()
        .map(|tp| (tp, OffsetAndMetadata::new(0).expect("offset 0")))
        .collect();
    consumer
        .commit_sync_with_offsets(offsets)
        .await
        .expect("seed the group's offsets");
    consumer.close().await.expect("close the seed consumer");
}

/// Holds a writer to at most `rate_per_sec`: by the end of each 10 ms tick it
/// may have sent `ticks * rate_per_sec / 100` records, so a rate that is not a
/// multiple of 100 is not rounded away.
struct Pacer {
    interval: Option<tokio::time::Interval>,
    rate_per_sec: u64,
    ticks: u64,
    sent: u64,
}

impl Pacer {
    fn new(rate_per_sec: u32) -> Self {
        let interval = (rate_per_sec > 0).then(|| {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            // After a stall (a restart, a slow broker), carry on at the rate
            // rather than sending the missed records in one burst.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });
        Self { interval, rate_per_sec: u64::from(rate_per_sec), ticks: 0, sent: 0 }
    }

    async fn wait(&mut self) {
        if let Some(interval) = &mut self.interval {
            while self.sent >= self.ticks * self.rate_per_sec / 100 {
                interval.tick().await;
                self.ticks += 1;
            }
            self.sent += 1;
        }
    }
}

struct OpenTxn {
    intent: TxnIntent,
    futures: Vec<(u64, KafkaFuture<RecordMetadata>)>,
}

async fn settle(future: KafkaFuture<RecordMetadata>) -> Result<RecordMetadata, Error> {
    match tokio::time::timeout(SETTLED_FUTURE_TIMEOUT, future.get()).await {
        Ok(result) => result,
        Err(_) => panic!("a delivery future did not complete {SETTLED_FUTURE_TIMEOUT:?} after its producer settled it"),
    }
}

/// How often a timed-out `commit_transaction` is retried before giving up.
const COMMIT_TIMEOUT_RETRIES: u32 = 5;

/// Commits the open transaction. A commit that times out has an unknown
/// outcome and the producer then accepts only the same call again (Java's
/// `commitTransaction` contract: "the user can retry by calling
/// commitTransaction again"), so a timeout is retried rather than aborted.
async fn commit_transaction(producer: &Producer, who: &str, ledger: &Ledger) -> Result<(), Error> {
    let mut timeouts = 0;
    loop {
        match producer.commit_transaction().await {
            Err(e) if e.is_timeout_error() && timeouts < COMMIT_TIMEOUT_RETRIES => {
                timeouts += 1;
                ledger.event(
                    Event::ProducerError,
                    format!("{who} commit_transaction timed out, retrying: {e}"),
                );
            },
            result => return result,
        }
    }
}

/// Ends `txn` as it intends, or aborts it when one of its sends `failed`, and
/// reports it. Returns `false` when the producer can no longer be used.
async fn end_txn(producer: &Producer, txn: OpenTxn, failed: bool, spec: &WriterSpec, ledger: &Ledger) -> bool {
    let (outcome, healthy) = if txn.intent == TxnIntent::Abort || failed {
        // Send the records of a planned abort first, so the broker writes and
        // then aborts them and read_committed readers must skip them; an abort
        // of unsent batches would only drop them in the client.
        let mut planned = !failed;
        if planned && let Err(e) = producer.flush().await {
            ledger.event(Event::ProducerError, format!("writer {} flush: {e}", spec.writer));
            planned = false;
        }
        match producer.abort_transaction().await {
            Ok(()) if planned => (TxnOutcome::Aborted, true),
            Ok(()) => (TxnOutcome::Failed, true),
            Err(e) => {
                // Never committed: the coordinator aborts it when it times out.
                ledger.event(Event::ProducerError, format!("writer {} abort_transaction: {e}", spec.writer));
                (TxnOutcome::Failed, false)
            },
        }
    } else {
        match commit_transaction(producer, &format!("writer {}", spec.writer), ledger).await {
            Ok(()) => (TxnOutcome::Committed, true),
            Err(e) => {
                ledger.event(Event::ProducerError, format!("writer {} commit_transaction: {e}", spec.writer));
                match producer.abort_transaction().await {
                    Ok(()) => (TxnOutcome::Failed, true),
                    Err(e) => {
                        ledger.event(Event::ProducerError, format!("writer {} abort_transaction: {e}", spec.writer));
                        (TxnOutcome::Unknown, false)
                    },
                }
            },
        }
    };
    let mut results = Vec::with_capacity(txn.futures.len());
    for (seq, future) in txn.futures {
        results.push((seq, settle(future).await));
    }
    ledger.resolve_txn(&spec.topic, spec.writer, &results, outcome);
    healthy
}

async fn create_producer(
    clients: &Clients,
    base: &Props,
    spec: &WriterSpec,
    incarnation: u32,
) -> Result<BoxProducer, Error> {
    let mut layers = vec![
        ("client.id", format!("integrity-writer-{}-{incarnation}", spec.writer)),
        ("acks", "all".to_string()),
        ("enable.idempotence", "true".to_string()),
    ];
    if let Some(txn) = &spec.transactions {
        layers.push(("transactional.id", txn.transactional_id.clone()));
    }
    clients.producer(merged(base, &layers, &spec.props)).await
}

/// One writer, until `stop` is set or it has written `spec.records`.
async fn run_writer(
    spec: WriterSpec,
    clients: Clients,
    base: Arc<Props>,
    ledger: Arc<Ledger>,
    generator: RecordGenerator,
    seed: u64,
    stop: Arc<AtomicBool>,
) {
    let mut aborts = AbortPlan::new(
        spec.transactions.as_ref().map_or(0, |t| t.abort_one_in),
        seed ^ u64::from(spec.writer).rotate_left(32),
    );
    let mut pacer = Pacer::new(spec.rate_per_sec);
    let mut written = 0u64;
    let mut incarnation = 0u32;
    let finished = |written: u64| stop.load(Ordering::SeqCst) || spec.records.is_some_and(|n| written >= n);
    while !finished(written) {
        let producer = match create_producer(&clients, &base, &spec, incarnation).await {
            Ok(producer) => producer,
            Err(e) => {
                ledger.event(Event::ProducerError, format!("writer {} create: {e}", spec.writer));
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            },
        };
        let mut healthy = true;
        if spec.transactions.is_some()
            && let Err(e) = producer.init_transactions().await
        {
            ledger.event(Event::ProducerError, format!("writer {} init_transactions: {e}", spec.writer));
            healthy = false;
        }
        let started = Instant::now();
        let mut written_here = 0u64;
        let mut in_flight: VecDeque<(u64, KafkaFuture<RecordMetadata>)> = VecDeque::new();
        let mut txn: Option<OpenTxn> = None;
        let mut restart_due = false;
        while healthy && !finished(written) {
            restart_due = match spec.restart {
                Some(Restart::EveryRecords(n)) => written_here >= n,
                Some(Restart::Every(every)) => started.elapsed() >= every,
                None => false,
            };
            if restart_due {
                break;
            }
            pacer.wait().await;
            if spec.transactions.is_some() && txn.is_none() {
                if let Err(e) = producer.begin_transaction() {
                    ledger.event(Event::ProducerError, format!("writer {} begin_transaction: {e}", spec.writer));
                    healthy = false;
                    break;
                }
                let intent = if aborts.next_aborts() {
                    TxnIntent::Abort
                } else {
                    TxnIntent::Commit
                };
                txn = Some(OpenTxn { intent, futures: Vec::new() });
            }
            let intent = txn.as_ref().map_or(TxnIntent::None, |t| t.intent);
            let seq = ledger.next_seq(&spec.topic, spec.writer);
            let identity = Identity {
                run_id: ledger.run_id(),
                id: RecordId { writer: spec.writer, seq },
                incarnation,
                intent,
                timestamp: now_ms(),
            };
            let record = build_record(&spec.topic, None, &identity, generator.generate(spec.writer, seq));
            match producer.send(record).await {
                Ok(future) => match txn.as_mut() {
                    Some(open) => open.futures.push((seq, future)),
                    None => in_flight.push_back((seq, future)),
                },
                Err(e) => {
                    ledger.send_rejected(&spec.topic, spec.writer, seq, &e);
                    // The transaction can no longer commit (Java's producer is
                    // then in an abortable error), so end it now.
                    if let Some(open) = txn.take() {
                        healthy = end_txn(&*producer, open, true, &spec, &ledger).await;
                    }
                },
            }
            written += 1;
            written_here += 1;
            while let Some((_, future)) = in_flight.front()
                && (future.is_done() || in_flight.len() > MAX_IN_FLIGHT)
            {
                let (seq, future) = in_flight.pop_front().expect("front exists");
                let result = future.get().await;
                ledger.delivered(&spec.topic, spec.writer, seq, &result);
            }
            if let Some(txn_spec) = &spec.transactions
                && txn
                    .as_ref()
                    .is_some_and(|t| t.futures.len() >= txn_spec.records_per_txn as usize)
            {
                healthy = end_txn(&*producer, txn.take().expect("open"), false, &spec, &ledger).await;
            }
        }
        if let Some(open) = txn.take()
            && !end_txn(&*producer, open, false, &spec, &ledger).await
        {
            healthy = false;
        }
        if let Err(e) = producer.close().await {
            ledger.event(Event::ProducerError, format!("writer {} close: {e}", spec.writer));
        }
        for (seq, future) in in_flight {
            let result = settle(future).await;
            ledger.delivered(&spec.topic, spec.writer, seq, &result);
        }
        incarnation += 1;
        // Only the planned restarts count: a producer replaced after a failure
        // is reported by the failure.
        if restart_due {
            ledger.event(
                Event::ProducerRestart,
                format!("writer {} incarnation {incarnation}", spec.writer),
            );
        }
        if !healthy && !finished(written) {
            tokio::time::sleep(RETRY_BACKOFF).await;
        }
    }
}

/// A rebalance listener that does nothing. Registering one makes a revocation
/// wait for the member's next poll, so the commit of everything it read lands
/// before another member takes the partition over (without a listener the
/// background task completes the revocation on its own).
struct WaitForPoll;

#[async_trait]
impl ConsumerRebalanceListener for WaitForPoll {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        Ok(())
    }
}

/// Offsets a member has read but not yet committed: what its last commit
/// failed to commit (`STALE_MEMBER_EPOCH` while a rebalance bumps its epoch:
/// "the member must retry after receiving its updated member epoch").
type Pending = Arc<Mutex<HashMap<TopicPartition, OffsetAndMetadata>>>;

/// A live-group member's listener: before a partition moves, it commits what
/// a failed commit left pending on it, as Java applications commit in
/// `onPartitionsRevoked`, so the next owner does not read it again. Like
/// [`WaitForPoll`], it also makes the revocation wait for the member's poll.
struct CommitOnRevoke {
    member: usize,
    handle: ConsumerHandle,
    pending: Pending,
    ledger: Arc<Ledger>,
}

#[async_trait]
impl ConsumerRebalanceListener for CommitOnRevoke {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = {
            let mut pending = self.pending.lock().unwrap();
            partitions.iter().filter_map(|tp| pending.remove_entry(tp)).collect()
        };
        if offsets.is_empty() {
            return Ok(());
        }
        match self.handle.commit_sync_with_offsets(offsets.clone()).await {
            Ok(()) => self.ledger.live_committed(&offsets),
            Err(e) => self
                .ledger
                .event(Event::CommitError, format!("member {} on revoke: {e}", self.member)),
        }
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        Ok(())
    }

    /// Lost partitions already have another owner: nothing can be committed.
    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        let mut pending = self.pending.lock().unwrap();
        for tp in partitions {
            pending.remove(tp);
        }
        Ok(())
    }
}

/// The properties of a live-group member or a copier's consumer.
fn consumer_props(base: &Props, group_id: &str, client_id: String, extra: &Props) -> Props {
    merged(
        base,
        &[
            ("group.id", group_id.to_string()),
            ("group.protocol", "consumer".to_string()),
            ("client.id", client_id),
            ("enable.auto.commit", "false".to_string()),
            ("auto.offset.reset", "none".to_string()),
            ("isolation.level", "read_committed".to_string()),
            ("allow.auto.create.topics", "false".to_string()),
        ],
        extra,
    )
}

/// The next offset to read after every record of `records`, per partition.
fn next_offsets<'a>(
    records: impl IntoIterator<Item = &'a confluent_kafka::consumer::ConsumerRecord<Vec<u8>, Vec<u8>>>,
) -> HashMap<TopicPartition, OffsetAndMetadata> {
    let mut next: HashMap<TopicPartition, i64> = HashMap::new();
    for record in records {
        let tp = TopicPartition::new(record.topic().to_string(), record.partition());
        let offset = next.entry(tp).or_insert(0);
        *offset = (*offset).max(record.offset() + 1);
    }
    next.into_iter()
        .map(|(tp, o)| (tp, OffsetAndMetadata::new(o).expect("valid offset")))
        .collect()
}

/// One member of the live group, created again after each restart until
/// `stop` is set. It restarts only while the writers run: once they stop, the
/// group settles and reads the rest before the drain ends.
async fn run_member(
    member: usize,
    spec: GroupSpec,
    clients: Clients,
    base: Arc<Props>,
    ledger: Arc<Ledger>,
    writers_stopped: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) {
    let mut generation = 0u32;
    // Hard-stopped consumers, closed when the run stops (`RestartMode::Hard`).
    let mut abandoned = Vec::new();
    while !stop.load(Ordering::SeqCst) {
        let client_id = format!("integrity-reader-{member}-{generation}");
        generation += 1;
        let pending = Pending::default();
        let props = consumer_props(&base, &spec.group_id, client_id, &spec.props);
        let listener = |handle| -> Arc<dyn ConsumerRebalanceListener> {
            Arc::new(CommitOnRevoke { member, handle, pending: pending.clone(), ledger: ledger.clone() })
        };
        let mut consumer = match clients.subscribed_consumer(props, spec.topics.clone(), listener).await {
            Ok(consumer) => consumer,
            Err(e) => {
                ledger.event(Event::ConsumerError, format!("member {member} create or subscribe: {e}"));
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            },
        };
        // Members restart in turn, not all at once.
        let deadline = spec.restart.as_ref().map(|r| {
            let stagger = if generation == 1 {
                r.every.mul_f64(member as f64 / spec.members as f64)
            } else {
                Duration::ZERO
            };
            Instant::now() + r.every + stagger
        });
        let restart_due = || deadline.is_some_and(|d| Instant::now() >= d) && !writers_stopped.load(Ordering::SeqCst);
        let mut last_readback = Instant::now();
        while !stop.load(Ordering::SeqCst) && !restart_due() {
            let records = match consumer.poll(POLL_TIMEOUT).await {
                Ok(records) => records,
                Err(e) => {
                    ledger.event(Event::ConsumerError, format!("member {member} poll: {e}"));
                    tokio::time::sleep(RETRY_BACKOFF).await;
                    continue;
                },
            };
            let records: Vec<_> = records.into_iter().collect();
            ledger.live_batch(&records);
            let mut offsets = next_offsets(&records);
            if offsets.is_empty() {
                continue;
            }
            // Retry what the last commit failed to commit, on partitions this
            // batch did not reach.
            for (tp, offset) in pending.lock().unwrap().drain() {
                offsets.entry(tp).or_insert(offset);
            }
            match consumer.commit_sync_with_offsets(offsets.clone()).await {
                Ok(()) => {
                    ledger.live_committed(&offsets);
                    if last_readback.elapsed() >= spec.readback_every {
                        last_readback = Instant::now();
                        let partitions: Vec<TopicPartition> = offsets.keys().cloned().collect();
                        match consumer.committed(&partitions).await {
                            Ok(read) => ledger.readback(&offsets, &read),
                            Err(e) => ledger.event(Event::ConsumerError, format!("member {member} committed: {e}")),
                        }
                    }
                },
                Err(e) => {
                    ledger.event(Event::CommitError, format!("member {member}: {e}"));
                    pending.lock().unwrap().extend(offsets);
                },
            }
        }
        if stop.load(Ordering::SeqCst) {
            if let Err(e) = consumer.close().await {
                ledger.event(Event::ConsumerError, format!("member {member} close: {e}"));
            }
            break;
        }
        let mode = spec
            .restart
            .as_ref()
            .map(|r| r.modes[(generation as usize - 1) % r.modes.len()]);
        if mode == Some(RestartMode::Hard) {
            // Fetch once more and crash before processing it: those records do
            // not count as read, so the next owner must read them from the
            // committed offset or they are reported lost.
            match consumer.poll(POLL_TIMEOUT).await {
                Ok(records) => ledger.abandoned_batch(&records.into_iter().collect::<Vec<_>>()),
                Err(e) => ledger.event(Event::ConsumerError, format!("member {member} poll: {e}")),
            }
            // A crash commits nothing more, not even at the final close.
            pending.lock().unwrap().clear();
            abandoned.push(consumer);
            ledger.event(Event::HardStop, format!("member {member}"));
        } else {
            if let Err(e) = consumer.close().await {
                ledger.event(Event::ConsumerError, format!("member {member} close: {e}"));
            }
            ledger.event(Event::CleanRestart, format!("member {member}"));
        }
    }
    for mut consumer in abandoned {
        if let Err(e) = consumer.close().await {
            ledger.event(Event::ConsumerError, format!("member {member} close after a hard stop: {e}"));
        }
    }
}

/// Seeks every partition of `partitions` back to its committed offset, and
/// returns those offsets: Apache Kafka's
/// `TransactionalMessageCopier.resetToLastCommittedPositions`.
async fn rewind(
    consumer: &mut Reader,
    partitions: &[TopicPartition],
) -> Result<HashMap<TopicPartition, Option<OffsetAndMetadata>>, Error> {
    let committed = consumer.committed(partitions).await?;
    for tp in partitions {
        let offset = committed.get(tp).and_then(Option::as_ref).map_or(0, OffsetAndMetadata::offset);
        consumer.seek_with_offset(tp.clone(), offset).await?;
    }
    Ok(committed)
}

/// Whether the copier owns every input partition and has read each to its end.
async fn copied_everything(consumer: &mut Reader, input: &str) -> Result<bool, Error> {
    let partitions = consumer.partitions_for(input).await?.len();
    let assigned: Vec<TopicPartition> = consumer.assignment().into_iter().collect();
    if assigned.len() < partitions {
        return Ok(false);
    }
    let end = consumer.end_offsets(&assigned).await?;
    for tp in &assigned {
        if consumer.position(tp).await? < end.get(tp).copied().unwrap_or(0) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The copier's consumer, subscribed to its input. `None` once `stop` is set
/// without one.
async fn start_copier_consumer(
    spec: &CopierSpec,
    clients: &Clients,
    base: &Props,
    ledger: &Ledger,
    stop: &AtomicBool,
) -> Option<BoxConsumer> {
    loop {
        let props = consumer_props(base, &spec.group_id, "integrity-copier".to_string(), &spec.props);
        let listener = |_| -> Arc<dyn ConsumerRebalanceListener> { Arc::new(WaitForPoll) };
        match clients.subscribed_consumer(props, vec![spec.input.clone()], listener).await {
            Ok(consumer) => return Some(consumer),
            Err(e) => ledger.event(Event::ConsumerError, format!("copier consumer create or subscribe: {e}")),
        }
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

/// The copier's transactional producer, with its transactions initialised.
/// `None` once `stop` is set without one having started.
async fn start_copier_producer(
    spec: &CopierSpec,
    clients: &Clients,
    base: &Props,
    ledger: &Ledger,
    stop: &AtomicBool,
) -> Option<BoxProducer> {
    let props = merged(
        base,
        &[
            ("client.id", "integrity-copier".to_string()),
            ("transactional.id", spec.transactional_id.clone()),
            ("enable.idempotence", "true".to_string()),
            ("acks", "all".to_string()),
        ],
        &Props::new(),
    );
    loop {
        match clients.producer(props.clone()).await {
            Ok(producer) => match producer.init_transactions().await {
                Ok(()) => return Some(producer),
                Err(e) => {
                    ledger.event(Event::ProducerError, format!("copier init_transactions: {e}"));
                    if let Err(e) = producer.close().await {
                        ledger.event(Event::ProducerError, format!("copier close: {e}"));
                    }
                },
            },
            Err(e) => ledger.event(Event::ProducerError, format!("copier create: {e}")),
        }
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

/// Seeks the partitions of `partitions` the copier still owns back to their
/// committed offsets, retrying until it succeeds (returning those offsets) or
/// `stop` is set. A partition it lost meanwhile is left to its new owner,
/// which starts from the same committed offset.
async fn rewind_owned(
    consumer: &mut Reader,
    partitions: &[TopicPartition],
    ledger: &Ledger,
    stop: &AtomicBool,
) -> Option<HashMap<TopicPartition, Option<OffsetAndMetadata>>> {
    loop {
        let assigned = consumer.assignment();
        let owned: Vec<TopicPartition> = partitions.iter().filter(|tp| assigned.contains(tp)).cloned().collect();
        match rewind(consumer, &owned).await {
            Ok(committed) => return Some(committed),
            Err(e) => ledger.event(Event::ConsumerError, format!("copier rewind: {e}")),
        }
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

/// The consume-transform-produce loop, until the writers have stopped and it
/// has copied everything. A producer that can no longer end its transaction is
/// replaced: the new one's `init_transactions` fences it and completes or
/// aborts what it left open, and the consumer goes back to the committed
/// offsets, which then say which of the two happened.
async fn run_copier(
    spec: CopierSpec,
    clients: Clients,
    base: Arc<Props>,
    ledger: Arc<Ledger>,
    seed: u64,
    writers_done: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) {
    let mut aborts = AbortPlan::new(spec.abort_one_in, seed ^ 0xC0_FFEE);
    let Some(mut consumer) = start_copier_consumer(&spec, &clients, &base, &ledger, &stop).await else {
        return;
    };
    let mut producer = start_copier_producer(&spec, &clients, &base, &ledger, &stop).await;
    // The offsets the copier committed, as far as it knows; the group's
    // committed offsets must match them (a partition missing here holds its
    // seeded 0).
    let mut expected: HashMap<TopicPartition, i64> = HashMap::new();
    let mut commits = 0u64;
    while let Some(active) = producer.as_ref()
        && !stop.load(Ordering::SeqCst)
    {
        let records: Vec<_> = match consumer.poll(POLL_TIMEOUT).await {
            Ok(records) => records.into_iter().collect(),
            Err(e) => {
                ledger.event(Event::ConsumerError, format!("copier poll: {e}"));
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            },
        };
        if records.is_empty() {
            if writers_done.load(Ordering::SeqCst) {
                match copied_everything(&mut *consumer, &spec.input).await {
                    Ok(true) => break,
                    Ok(false) => {},
                    Err(e) => ledger.event(Event::ConsumerError, format!("copier progress: {e}")),
                }
            }
            continue;
        }
        let offsets = next_offsets(&records);
        let partitions: Vec<TopicPartition> = offsets.keys().cloned().collect();
        let mut futures = Vec::with_capacity(records.len());
        let mut healthy = true;
        let outcome = match active.begin_transaction() {
            Err(e) => {
                ledger.event(Event::ProducerError, format!("copier begin_transaction: {e}"));
                healthy = false;
                TxnOutcome::Failed
            },
            Ok(()) => {
                let mut failed = None;
                for record in &records {
                    match active.send(copy_record(record, &spec.output)).await {
                        Ok(future) => futures.push((record.partition(), record.offset(), future)),
                        Err(e) => failed = Some(e),
                    }
                }
                if failed.is_none() {
                    let group = consumer.group_metadata();
                    if let Err(e) = active.send_offsets_to_transaction(offsets.clone(), group.as_ref()).await {
                        failed = Some(e);
                    }
                }
                let mut planned_abort = failed.is_none() && aborts.next_aborts();
                // Send the copies of a planned abort first, so the broker
                // aborts them and read_committed readers must skip them.
                if planned_abort && let Err(e) = active.flush().await {
                    failed = Some(e);
                    planned_abort = false;
                }
                let committed = failed.is_none()
                    && !planned_abort
                    && match commit_transaction(&**active, "copier", &ledger).await {
                        Ok(()) => true,
                        Err(e) => {
                            failed = Some(e);
                            false
                        },
                    };
                if let Some(e) = &failed {
                    ledger.event(Event::ProducerError, format!("copier: {e}"));
                }
                if committed {
                    TxnOutcome::Committed
                } else {
                    match active.abort_transaction().await {
                        Ok(()) if planned_abort => TxnOutcome::Aborted,
                        Ok(()) => TxnOutcome::Failed,
                        Err(e) => {
                            ledger.event(Event::ProducerError, format!("copier abort_transaction: {e}"));
                            healthy = false;
                            TxnOutcome::Failed
                        },
                    }
                }
            },
        };
        ledger.copier_txn(outcome);
        let committed = outcome == TxnOutcome::Committed;
        for (partition, offset, future) in futures {
            if let Err(e) = settle(future).await
                && committed
            {
                let source = format!("{}-{partition}@{offset}", spec.input);
                ledger.copier_failed_in_committed_txn(&spec.output, &source, &e);
            }
        }
        if !healthy {
            if let Some(old) = producer.take()
                && let Err(e) = old.close().await
            {
                ledger.event(Event::ProducerError, format!("copier close: {e}"));
            }
            producer = start_copier_producer(&spec, &clients, &base, &ledger, &stop).await;
        }
        if committed {
            expected.extend(offsets.iter().map(|(tp, o)| (tp.clone(), o.offset())));
            commits += 1;
            if commits.is_multiple_of(COPIER_READBACK_EVERY) {
                match consumer.committed(&partitions).await {
                    Ok(read) => ledger.copier_readback(&expected, &read),
                    Err(e) => ledger.event(Event::ConsumerError, format!("copier committed: {e}")),
                }
            }
        } else if let Some(read) = rewind_owned(&mut *consumer, &partitions, &ledger, &stop).await {
            if outcome == TxnOutcome::Aborted {
                // A planned abort leaves the offsets where the last commit put
                // them.
                ledger.copier_readback(&expected, &read);
            } else {
                // A failed transaction may still have committed (a replacement
                // producer completes a commit in flight): what was read back
                // says whether it did.
                expected.extend(
                    read.iter()
                        .map(|(tp, o)| (tp.clone(), o.as_ref().map_or(0, OffsetAndMetadata::offset))),
                );
            }
        }
    }
    if let Some(producer) = producer
        && let Err(e) = producer.close().await
    {
        ledger.event(Event::ProducerError, format!("copier close: {e}"));
    }
    if let Err(e) = consumer.close().await {
        ledger.event(Event::ConsumerError, format!("copier consumer close: {e}"));
    }
}

/// Reads every partition of `topics` from the start with a fresh read_committed
/// consumer (no group) and reports each record. Returns whether it reached the
/// end of every partition; it gives up once `timeout` passes without moving
/// forward.
pub async fn final_read_back(
    clients: &Clients,
    base: &Props,
    topics: &[TopicSpec],
    ledger: &Ledger,
    timeout: Duration,
) -> bool {
    let props = merged(
        base,
        &[
            ("client.id", "integrity-final-read-back".to_string()),
            // No group, but the classic default is rejected at construction.
            ("group.protocol", "consumer".to_string()),
            ("enable.auto.commit", "false".to_string()),
            ("auto.offset.reset", "earliest".to_string()),
            ("isolation.level", "read_committed".to_string()),
        ],
        &Props::new(),
    );
    let deadline = Instant::now() + timeout;
    let mut consumer = loop {
        match clients.consumer(props.clone()).await {
            Ok(consumer) => break consumer,
            Err(e) => {
                ledger.event(Event::ConsumerError, format!("final read-back create: {e}"));
                if Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(RETRY_BACKOFF).await;
            },
        }
    };
    let partitions: Vec<TopicPartition> = topics
        .iter()
        .flat_map(|t| (0..t.partitions).map(|p| TopicPartition::new(t.name.clone(), p)))
        .collect();
    // A connection can drop under any of these calls (a disconnect fails a
    // ListOffsets as Java's does), so retry until the deadline rather than
    // lose the verdict of a long run to one transient error.
    let end = loop {
        let setup = async {
            consumer.assign(partitions.clone()).await?;
            consumer.seek_to_beginning(&partitions).await?;
            consumer.end_offsets(&partitions).await
        };
        match setup.await {
            Ok(end) => break end,
            Err(e) => {
                ledger.event(Event::ConsumerError, format!("final read-back setup: {e}"));
                if Instant::now() >= deadline {
                    close_final_read_back(consumer, ledger).await;
                    return false;
                }
                tokio::time::sleep(RETRY_BACKOFF).await;
            },
        }
    };
    let mut remaining: HashSet<TopicPartition> = partitions
        .into_iter()
        .filter(|tp| end.get(tp).copied().unwrap_or(0) > 0)
        .collect();
    // A long run reads back tens of millions of records, so the deadline is
    // how long it may go without moving forward, not a bound on the whole
    // pass. Aborted ranges move the position without returning records.
    let mut deadline = Instant::now() + timeout;
    let mut positions: HashMap<TopicPartition, i64> = HashMap::new();
    while !remaining.is_empty() && Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(500)).await {
            Ok(records) => records.into_iter().for_each(|record| ledger.final_record(&record)),
            Err(e) => {
                ledger.event(Event::ConsumerError, format!("final read-back poll: {e}"));
                tokio::time::sleep(RETRY_BACKOFF).await;
            },
        }
        let mut done = Vec::new();
        for tp in &remaining {
            let Ok(position) = consumer.position(tp).await else {
                continue;
            };
            if positions.insert(tp.clone(), position) != Some(position) {
                deadline = Instant::now() + timeout;
            }
            if position >= end[tp] {
                done.push(tp.clone());
            }
        }
        for tp in done {
            remaining.remove(&tp);
        }
    }
    close_final_read_back(consumer, ledger).await;
    remaining.is_empty()
}

async fn close_final_read_back(mut consumer: BoxConsumer, ledger: &Ledger) {
    if let Err(e) = consumer.close().await {
        ledger.event(Event::ConsumerError, format!("final read-back close: {e}"));
    }
}

/// Waits for `handle`, or gives up after `timeout`. `None` when it finished,
/// otherwise what went wrong: a panicking task fails the run with its report
/// rather than taking the report down with it.
async fn join(mut handle: JoinHandle<()>, timeout: Duration) -> Option<String> {
    match tokio::time::timeout(timeout, &mut handle).await {
        Ok(Ok(())) => None,
        Ok(Err(e)) if e.is_panic() => {
            let panic = e.into_panic();
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            Some(format!("panicked: {message}"))
        },
        Ok(Err(e)) => Some(format!("failed: {e}")),
        Err(_) => {
            handle.abort();
            Some(format!("did not stop within {timeout:?}"))
        },
    }
}

/// Creates the topics, runs every client of `spec`, waits for the readers to
/// catch up, reads everything back and scores the run.
pub async fn run(spec: RunSpec, base: Props) -> Verdict {
    eprintln!(
        "integrity: run {:#x}, seed {} (set INTEGRITY_SEED to replay)",
        spec.run_id, spec.seed
    );
    let base = Arc::new(base);
    // The clients under test may reach the brokers at other addresses than
    // the run's own setup clients.
    let client_base = spec.clients.base(&base);
    eprintln!("integrity: clients {}", spec.clients.name());
    let ledger = Arc::new(Ledger::new(spec.run_id, spec.seed));
    for topic in &spec.topics {
        ledger.register_topic(&topic.name, topic.partitions, topic.kind.clone());
    }
    create_topics(&base, &spec.topics).await;

    let stop_writers = Arc::new(AtomicBool::new(false));
    let stop_readers = Arc::new(AtomicBool::new(false));
    // Set once every writer has stopped, so a copier does not finish before
    // the last records it must copy are written.
    let writers_done = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    if let Some(group) = &spec.group {
        let topics: Vec<&TopicSpec> = spec.topics.iter().filter(|t| group.topics.contains(&t.name)).collect();
        seed_group_offsets(&base, &group.group_id, &topics).await;
        for member in 0..group.members {
            readers.push(tokio::spawn(run_member(
                member,
                group.clone(),
                spec.clients.clone(),
                client_base.clone(),
                ledger.clone(),
                stop_writers.clone(),
                stop_readers.clone(),
            )));
        }
    }
    let mut copiers = Vec::new();
    for copier in &spec.copiers {
        let input: Vec<&TopicSpec> = spec.topics.iter().filter(|t| t.name == copier.input).collect();
        seed_group_offsets(&base, &copier.group_id, &input).await;
        copiers.push(tokio::spawn(run_copier(
            copier.clone(),
            spec.clients.clone(),
            client_base.clone(),
            ledger.clone(),
            spec.seed,
            writers_done.clone(),
            stop_readers.clone(),
        )));
    }
    let writers: Vec<JoinHandle<()>> = spec
        .writers
        .iter()
        .map(|writer| {
            let generator = ledger.register_writer(writer.writer, writer.profile.clone());
            tokio::spawn(run_writer(
                writer.clone(),
                spec.clients.clone(),
                client_base.clone(),
                ledger.clone(),
                generator,
                spec.seed,
                stop_writers.clone(),
            ))
        })
        .collect();

    let started = Instant::now();
    let mut aborted = None;
    let mut last_progress = (ledger.progress(), Instant::now());
    let mut last_status = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let elapsed = started.elapsed();
        let done = match spec.duration {
            Some(duration) => elapsed >= duration,
            None => writers.iter().all(JoinHandle::is_finished),
        };
        if done {
            break;
        }
        let progress = ledger.progress();
        if progress != last_progress.0 {
            last_progress = (progress, Instant::now());
        } else if last_progress.1.elapsed() >= spec.watchdog {
            aborted = Some(format!("nothing was sent or read for {:?}", spec.watchdog));
            break;
        }
        if let Some(every) = spec.status_every
            && last_status.elapsed() >= every
        {
            last_status = Instant::now();
            let c = ledger.counts();
            eprintln!(
                "integrity: {:>6}s sent {} acked {} read {} redelivered {} failures {}",
                elapsed.as_secs(),
                c.sent,
                c.acked,
                c.live_consumed,
                c.redelivered,
                c.failures().iter().map(|(_, n)| n).sum::<u64>()
            );
        }
    }

    eprintln!("integrity: stopping the writers");
    stop_writers.store(true, Ordering::SeqCst);
    for writer in writers {
        if let Some(why) = join(writer, spec.drain_timeout).await
            && aborted.is_none()
        {
            aborted = Some(format!("a writer {why}"));
        }
    }
    writers_done.store(true, Ordering::SeqCst);
    for copier in copiers {
        if let Some(why) = join(copier, spec.drain_timeout).await
            && aborted.is_none()
        {
            aborted = Some(format!("a copier {why}"));
        }
    }
    if spec.group.is_some() {
        eprintln!("integrity: waiting for the live group to catch up");
        let deadline = Instant::now() + spec.drain_timeout;
        while ledger.live_outstanding() > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    stop_readers.store(true, Ordering::SeqCst);
    for reader in readers {
        if let Some(why) = join(reader, spec.drain_timeout).await
            && aborted.is_none()
        {
            aborted = Some(format!("a live-group member {why}"));
        }
    }
    eprintln!("integrity: final read-back");
    let complete = match &spec.java_reader {
        Some(reader) => java_final_read_back(reader, &spec.topics, &ledger).await,
        None => final_read_back(&spec.clients, &client_base, &spec.topics, &ledger, spec.final_timeout).await,
    };
    let mut verdict = ledger.verdict(spec.group.is_some(), complete, &spec.requirements);
    verdict.aborted = aborted;
    verdict
}
