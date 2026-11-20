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

//! Shared fixture of the share consumer integration suites, translated from
//! `org.apache.kafka.clients.consumer.ShareConsumerTestBase` (Apache Kafka
//! 4.4.0), plus the share-group cluster configurations and broker-version
//! gates the suites need.
//!
//! # Deviations from Java, for every suite
//!
//! - **Pooled clusters and per-test names.** Java's `ClusterTestExtensions`
//!   starts a fresh cluster per test invocation, so the suites use the fixed
//!   names `topic`, `topic2`, `warmup` and `group1`. Here clusters are pooled
//!   by configuration ([`crate::common::cluster_pool`]) and tests sharing one
//!   run concurrently, so every topic and group name is prefixed per test
//!   ([`ShareConsumerTestBase::group`], [`TestContext::topic`]). Tests that
//!   stop brokers get a dedicated cluster, as Java's do.
//! - **`verifyShareGroupStateTopicRecordsProduced`** waits for more than two
//!   `__share_group_state` records, the two the warmup group writes on a fresh
//!   cluster. On a pooled cluster the topic already holds other tests' records,
//!   which would make that check pass at once, so
//!   [`ShareConsumerTestBase::verify_share_group_state_topic_records_produced`]
//!   waits for a record of one of this test's own share groups instead (the
//!   record key carries the group id). It checks the same thing, that the test's
//!   share groups wrote share state.
//! - **`waitForMetadataCache`** reads the broker's metadata cache in process;
//!   here it waits for `describeCluster` to list a broker.
//! - **`tearDown`**'s `ServerTestUtils.clearYammerMetrics()` clears broker
//!   metrics in the test JVM; there is nothing to clear here.
//! - The consumers, producers and admin clients Java closes with
//!   try-with-resources are closed explicitly, and a close failure fails the
//!   test, as a throwing `close` does in Java.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::AlterConfigOp;
use confluent_kafka::admin::AlterConfigsOptions;
use confluent_kafka::admin::AlterShareGroupOffsetsOptions;
use confluent_kafka::admin::ConfigEntry;
use confluent_kafka::admin::DeleteShareGroupOffsetsOptions;
use confluent_kafka::admin::DescribeShareGroupsOptions;
use confluent_kafka::admin::ListShareGroupOffsetsOptions;
use confluent_kafka::admin::ListShareGroupOffsetsSpec;
use confluent_kafka::admin::NewTopic;
use confluent_kafka::admin::OpType;
use confluent_kafka::admin::SharePartitionOffsetInfo;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicIdPartition;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::Uuid;
use confluent_kafka::common::config::ConfigResource;
use confluent_kafka::common::config::config_resource;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::serialization::Serializer;
use confluent_kafka::consumer::AcknowledgeType;
use confluent_kafka::consumer::AcknowledgementCommitCallback;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerRecord;
use confluent_kafka::consumer::ConsumerRecords;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::consumer::KafkaShareConsumer;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::cluster_config::Type;
use crate::common::kafka_cluster;
use crate::common::test_context::TestContext;
use crate::common::test_utils;

// ---------------------------------------------------------------------------
// Cluster configurations and broker-version gates
// ---------------------------------------------------------------------------

/// The `@ClusterTestDefaults` `serverProperties` shared by
/// `ShareConsumerTest`, `ShareConsumerDeliveryTest`, `ShareConsumerLagTest`,
/// `ShareConsumerCallbackTest` and `ShareConsumerRenewTest`, as `KAFKA_*`
/// environment variables.
///
/// `group.share.max.partition.max.record.locks` does not exist on a 4.2
/// broker, which ignores it; a 4.3+ broker needs it next to
/// `group.share.partition.max.record.locks` (`ShareGroupConfig` checks the
/// maximum is at least the value).
pub const SHARE_CLUSTER_DEFAULTS: &[(&str, &str)] = &[
    ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
    ("KAFKA_GROUP_SHARE_MAX_PARTITION_MAX_RECORD_LOCKS", "10000"),
    ("KAFKA_GROUP_SHARE_PARTITION_MAX_RECORD_LOCKS", "10000"),
    ("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS", "15000"),
    ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
    ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1"),
    ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS", "3"),
    ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1"),
    ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
    ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
];

/// A pooled cluster with Java's `@ClusterTestDefaults(types = KRAFT)` shape (one
/// broker, one isolated controller) and the given server properties.
pub fn share_cluster_config(properties: &[(&str, &str)]) -> ClusterConfig {
    ClusterConfig {
        brokers: 1,
        cluster_type: Type::Kraft,
        controllers: 1,
        server_properties: to_server_properties(properties),
        ..ClusterConfig::default()
    }
}

/// [`share_cluster_config`] over [`SHARE_CLUSTER_DEFAULTS`], with `overrides`
/// (a per-test `@ClusterTest(serverProperties = ...)`) added or replacing.
pub fn share_cluster_config_with(overrides: &[(&str, &str)]) -> ClusterConfig {
    let mut config = share_cluster_config(SHARE_CLUSTER_DEFAULTS);
    config.server_properties.extend(to_server_properties(overrides));
    config
}

/// The class-level defaults of the five suites that share them.
pub fn default_share_cluster_config() -> ClusterConfig {
    share_cluster_config_with(&[])
}

fn to_server_properties(properties: &[(&str, &str)]) -> BTreeMap<String, String> {
    properties
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

/// The broker release that added `group.share.assignment.interval.ms`
/// (`GroupCoordinatorConfig#SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG`, 4.3).
/// A 4.2 broker ignores the key, so the two `@ClusterTests` arms that set it to
/// `0` and `1000` would run identical brokers there.
pub const SHARE_ASSIGNMENT_INTERVAL_RELEASE: (u32, u32) = (4, 3);

/// The broker release whose `GroupConfig` has the dynamic share group configs
/// `share.delivery.count.limit`, `share.partition.max.record.locks` and
/// `share.renew.acknowledge.enable` (4.3). An older broker rejects an unknown
/// group config (`GroupConfig#validateNames`).
pub const SHARE_GROUP_CONFIGS_RELEASE: (u32, u32) = (4, 3);

/// The broker release with `share.version=2`, the share group dead letter queue
/// (`ShareVersion#supportsShareGroupDLQ`, bootstrap metadata version
/// `IBP_4_4_IV0`), and its group, topic and broker configs (4.4).
pub const SHARE_DLQ_RELEASE: (u32, u32) = (4, 4);

/// `true` (and the reason printed) when the broker image of this run is older
/// than `release`, so the calling test must return without running its body.
pub fn skip_below(release: (u32, u32), what: &str) -> bool {
    let broker = kafka_cluster::broker_release();
    if broker < release {
        eprintln!(
            "SKIP: {what} needs a {}.{} broker; this run uses {}.{} (INTEGRATION_TEST_BROKER_TAG)",
            release.0, release.1, broker.0, broker.1
        );
        return true;
    }
    false
}

/// `true` (and the reason printed) when the broker image of this run is
/// `release` or later. The single default-behaviour test of a two-arm
/// assignment-interval method uses it: from `release` on, the two Java arms
/// run instead (the `consumer_bounce_test.rs` precedent).
pub fn skip_from(release: (u32, u32), what: &str) -> bool {
    let broker = kafka_cluster::broker_release();
    if broker >= release {
        eprintln!(
            "SKIP: {what} runs as its two Java arms on a {}.{}+ broker; this run uses {}.{}",
            release.0, release.1, broker.0, broker.1
        );
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Java's `KEY` header key.
pub const KEY: &str = "content-type";
/// Java's `VALUE` header value.
pub const VALUE: &str = "application/octet-stream";
/// Java's `EXPLICIT` (`share.acknowledgement.mode`).
pub const EXPLICIT: &str = "explicit";
/// Java's `IMPLICIT` (`share.acknowledgement.mode`).
pub const IMPLICIT: &str = "implicit";
/// Java's `RECORD_LIMIT` (`share.acquire.mode`).
pub const RECORD_LIMIT: &str = "record_limit";
/// Java's `BATCH_OPTIMIZED` (`share.acquire.mode`).
pub const BATCH_OPTIMIZED: &str = "batch_optimized";

/// `org.apache.kafka.test.TestUtils#DEFAULT_MAX_WAIT_MS`.
pub const DEFAULT_MAX_WAIT_MS: u64 = test_utils::DEFAULT_MAX_WAIT_MS;
/// `org.apache.kafka.test.TestUtils#DEFAULT_POLL_INTERVAL_MS`.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = test_utils::DEFAULT_PAUSE_MS;

/// The name of the share-group state topic, `Topic#SHARE_GROUP_STATE_TOPIC_NAME`.
pub const SHARE_GROUP_STATE_TOPIC_NAME: &str = "__share_group_state";

/// `ShareGroupConfig#SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT`.
pub const SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT: i64 = 5;

/// How long the fixture lets a client close before failing the test.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(60);

/// The share consumer of the tests: byte-array keys and values.
pub type BytesShareConsumer = KafkaShareConsumer<Vec<u8>, Vec<u8>>;
/// The producer of the tests: byte-array keys and values.
pub type BytesProducer = KafkaProducer<Vec<u8>, Vec<u8>>;
/// A byte-array record.
pub type BytesRecord = ConsumerRecord<Vec<u8>, Vec<u8>>;

// ---------------------------------------------------------------------------
// TestableAcknowledgementCommitCallback, ClientState
// ---------------------------------------------------------------------------

/// The offsets per partition a [`TestableAcknowledgementCommitCallback`] saw.
pub type PartitionOffsetsMap = Arc<Mutex<HashMap<TopicPartition, HashSet<i64>>>>;
/// The first error per partition a [`TestableAcknowledgementCommitCallback`] saw.
pub type PartitionErrorMap = Arc<Mutex<HashMap<TopicPartition, Error>>>;

/// Java's `TestableAcknowledgementCommitCallback`: tracks the completed
/// acknowledgements. `partition_offsets_map` collects the offsets acknowledged
/// for each partition, `partition_error_map` the error encountered for each
/// partition, if any. Note - multiple calls to `on_complete` will not update
/// `partition_error_map` for a partition already in it, so clear it after every
/// call to `on_complete` in a single test.
pub struct TestableAcknowledgementCommitCallback {
    partition_offsets_map: PartitionOffsetsMap,
    partition_error_map: PartitionErrorMap,
}

impl TestableAcknowledgementCommitCallback {
    /// Java's constructor.
    pub fn new(partition_offsets_map: PartitionOffsetsMap, partition_error_map: PartitionErrorMap) -> Arc<Self> {
        Arc::new(Self { partition_offsets_map, partition_error_map })
    }
}

#[async_trait]
impl AcknowledgementCommitCallback for TestableAcknowledgementCommitCallback {
    async fn on_complete(&self, offsets_map: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>) {
        let mut offsets_by_partition = self.partition_offsets_map.lock().expect("offsets map lock");
        let mut errors_by_partition = self.partition_error_map.lock().expect("error map lock");
        for (partition, offsets) in offsets_map {
            offsets_by_partition
                .entry(partition.topic_partition().clone())
                .or_default()
                .extend(offsets.iter().copied());
            if let Some(error) = error {
                errors_by_partition
                    .entry(partition.topic_partition().clone())
                    .or_insert_with(|| error.clone());
            }
        }
    }
}

/// Java's `ClientState`: the state of a consumer or producer run on another
/// task.
#[derive(Default)]
pub struct ClientState {
    done: AtomicBool,
    count: AtomicUsize,
}

impl ClientState {
    /// Java's `done()`.
    pub fn done(&self) -> &AtomicBool {
        &self.done
    }

    /// Java's `count()`.
    pub fn count(&self) -> &AtomicUsize {
        &self.count
    }
}

// ---------------------------------------------------------------------------
// SerializerImpl, DeserializerImpl
// ---------------------------------------------------------------------------

/// Java's `SerializerImpl`: adds the [`KEY`] / [`VALUE`] header and returns the
/// data unchanged. The header-less form must not be called.
#[derive(Default)]
pub struct SerializerImpl;

impl Serializer<Vec<u8>> for SerializerImpl {
    async fn serialize<'a>(
        &'a self,
        _topic: &'a str,
        _data: Option<&'a Vec<u8>>,
    ) -> Result<Option<std::borrow::Cow<'a, [u8]>>, Error> {
        panic!("method should not be invoked");
    }

    fn serialize_with_headers<'a, 'h>(
        &'a self,
        _topic: &'a str,
        headers: &'h mut RecordHeaders,
        data: Option<&'a Vec<u8>>,
    ) -> impl Future<Output = Result<Option<std::borrow::Cow<'a, [u8]>>, Error>> + Send + use<'a, 'h>
    where
        'a: 'h,
    {
        headers
            .add_with_key_value(KEY, Some(VALUE.as_bytes()))
            .expect("headers are writable while serializing");
        std::future::ready(Ok(data.map(|bytes| std::borrow::Cow::Borrowed(bytes.as_slice()))))
    }
}

/// Java's `DeserializerImpl`: asserts the [`KEY`] header holds [`VALUE`] and
/// returns the data unchanged. The header-less form must not be called.
#[derive(Default)]
pub struct DeserializerImpl;

impl Deserializer<Vec<u8>> for DeserializerImpl {
    async fn deserialize<'a>(&'a self, _topic: &'a str, _data: &'a [u8]) -> Result<Vec<u8>, Error> {
        panic!("method should not be invoked");
    }

    async fn deserialize_with_headers<'a>(
        &'a self,
        _topic: &'a str,
        headers: &'a mut RecordHeaders,
        data: &'a [u8],
    ) -> Result<Vec<u8>, Error> {
        let header = headers.last_header(KEY).and_then(|header| header.value()).map(<[u8]>::to_vec);
        assert_eq!(Some(VALUE.as_bytes().to_vec()), header);
        Ok(data.to_vec())
    }
}

// ---------------------------------------------------------------------------
// ShareConsumerTestBase
// ---------------------------------------------------------------------------

/// Java's `ShareConsumerTestBase`, one per test: the cluster, the test's
/// topics (`tp`, `tp2`, `warmupTp`) and the helpers. [`Self::setup`] is
/// Java's `@BeforeEach`.
pub struct ShareConsumerTestBase {
    ctx: TestContext,
    /// Java's `tp`: partition 0 of the test's `topic`.
    pub tp: TopicPartition,
    /// Java's `tpId`: the id of `topic`.
    pub tp_id: Uuid,
    /// Java's `tp2`: partition 0 of the test's `topic2`.
    pub tp2: TopicPartition,
    /// Java's `warmupTp`: partition 0 of the test's `warmup`.
    pub warmup_tp: TopicPartition,
    /// Java's `sgsTopicPartitions`: the three partitions of `__share_group_state`.
    pub sgs_topic_partitions: Vec<TopicPartition>,
    /// The share groups this test used, for
    /// [`Self::verify_share_group_state_topic_records_produced`].
    groups: Mutex<HashSet<String>>,
}

impl ShareConsumerTestBase {
    /// Java's `@BeforeEach setup()`: waits for the brokers, creates `topic`
    /// and `topic2`, and runs [`Self::warmup`].
    pub async fn setup(config: ClusterConfig) -> Self {
        let mut ctx = TestContext::new(config).await;
        if ctx.cluster().config().dedicated {
            ctx.cluster().wait_for_ready_brokers().await;
        }
        let tp = TopicPartition::new(ctx.topic("topic"), 0);
        let tp2 = TopicPartition::new(ctx.topic("topic2"), 0);
        let warmup_tp = TopicPartition::new(ctx.topic("warmup"), 0);
        let sgs_topic_partitions = (0..3)
            .map(|part| TopicPartition::new(SHARE_GROUP_STATE_TOPIC_NAME.to_string(), part))
            .collect();
        let mut base = Self {
            ctx,
            tp,
            tp_id: Uuid::ZERO_UUID,
            tp2,
            warmup_tp,
            sgs_topic_partitions,
            groups: Mutex::new(HashSet::new()),
        };
        base.tp_id = base.create_topic(base.tp.topic()).await;
        base.create_topic(base.tp2.topic()).await;
        base.warmup().await;
        base
    }

    /// The test's context, for the cluster and the bootstrap servers.
    pub fn ctx(&self) -> &TestContext {
        &self.ctx
    }

    /// The per-test name of the group Java calls `name`.
    pub fn group(&self, name: &str) -> String {
        self.ctx.group_id(name)
    }

    /// The per-test name of the topic Java calls `name`.
    pub fn topic(&mut self, name: &str) -> String {
        self.ctx.topic(name)
    }

    /// The bootstrap servers of the test's listener (Java's
    /// `cluster.bootstrapServers()`).
    pub fn bootstrap_servers(&self) -> String {
        self.ctx.protocol_bootstrap_servers().to_string()
    }

    /// The `TopicIdPartition` of `tp`.
    pub fn tip(&self) -> TopicIdPartition {
        TopicIdPartition::new(self.tp_id, self.tp.clone())
    }

    // -- producing ---------------------------------------------------------

    /// Java's `produceMessages`: sends `message_count` copies of a
    /// `key`/`value` record to `tp` and flushes.
    pub async fn produce_messages(&self, message_count: usize) -> usize {
        let producer = self.create_producer().await;
        for _ in 0..message_count {
            let record = producer_record(
                self.tp.topic().to_string(),
                Some(self.tp.partition()),
                Some(b"key".to_vec()),
                Some(b"value".to_vec()),
            );
            send_record(&producer, record).await.expect("send");
        }
        producer.flush().await.expect("flush");
        close_producer(producer).await;
        message_count
    }

    /// Java's `produceMessagesWithTimestamp`.
    pub async fn produce_messages_with_timestamp(&self, message_count: usize, starting_timestamp: i64) {
        let producer = self.create_producer().await;
        for i in 0..message_count {
            let record = producer_record_with_timestamp(
                self.tp.topic().to_string(),
                Some(self.tp.partition()),
                Some(starting_timestamp + i as i64),
                Some(format!("key {i}").into_bytes()),
                Some(format!("value {i}").into_bytes()),
            );
            send_record(&producer, record).await.expect("send");
        }
        producer.flush().await.expect("flush");
        close_producer(producer).await;
    }

    /// Java's `produceCommittedTransaction`.
    pub async fn produce_committed_transaction(&self, transactional_producer: &BytesProducer, message: &str) {
        let result: Result<(), Error> = async {
            transactional_producer.begin_transaction()?;
            let future = send_record(transactional_producer, self.tp_record(message)).await?;
            transactional_producer.flush().await?;
            // Ensure producer send is complete before committing
            future.get().await?;
            transactional_producer.commit_transaction().await
        }
        .await;
        if result.is_err() {
            let _ = transactional_producer.abort_transaction().await;
        }
    }

    /// Java's `produceAbortedTransaction`.
    pub async fn produce_aborted_transaction(&self, transactional_producer: &BytesProducer, message: &str) {
        let result: Result<(), Error> = async {
            transactional_producer.begin_transaction()?;
            let future = send_record(transactional_producer, self.tp_record(message)).await?;
            transactional_producer.flush().await?;
            // Ensure producer send is complete before aborting
            future.get().await?;
            transactional_producer.abort_transaction().await
        }
        .await;
        if result.is_err() {
            let _ = transactional_producer.abort_transaction().await;
        }
    }

    /// Java's `produceCommittedAndAbortedTransactionsInInterval`: aborts every
    /// `interval_aborted_transactions`-th transaction, commits the others, and
    /// closes the producer.
    pub async fn produce_committed_and_aborted_transactions_in_interval(
        &self,
        transactional_producer: BytesProducer,
        message_count: usize,
        interval_aborted_transactions: usize,
    ) {
        transactional_producer.init_transactions().await.expect("init transactions");
        let mut transaction_count = 0;
        let result: Result<(), Error> = async {
            for i in 0..message_count {
                transactional_producer.begin_transaction()?;
                let record_message = format!("Message {}", i + 1);
                let future = send_record(&transactional_producer, self.tp_record(&record_message)).await?;
                transactional_producer.flush().await?;
                // Increment transaction count
                transaction_count += 1;
                if transaction_count % interval_aborted_transactions == 0 {
                    // Aborts every intervalAbortedTransactions transaction
                    transactional_producer.abort_transaction().await?;
                } else {
                    // Commits other transactions
                    future.get().await?; // Ensure producer send is complete before committing
                    transactional_producer.commit_transaction().await?;
                }
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = transactional_producer.abort_transaction().await;
        }
        close_producer(transactional_producer).await;
    }

    fn tp_record(&self, message: &str) -> ProducerRecord<Vec<u8>, Vec<u8>> {
        producer_record(
            self.tp.topic().to_string(),
            Some(self.tp.partition()),
            Some(message.as_bytes().to_vec()),
            Some(message.as_bytes().to_vec()),
        )
    }

    // -- consuming ---------------------------------------------------------

    /// Java's `consumeMessages(AtomicInteger, int, String, int, int, boolean)`:
    /// a new share consumer of `group_id` subscribed to `tp`'s topic.
    pub async fn consume_messages(
        &self,
        total_messages_consumed: &AtomicUsize,
        total_messages: usize,
        group_id: &str,
        consumer_number: usize,
        max_polls: usize,
        commit: bool,
    ) -> usize {
        self.consume_messages_with_max_fetch_bytes_option(
            total_messages_consumed,
            total_messages,
            group_id,
            consumer_number,
            max_polls,
            commit,
            None,
        )
        .await
    }

    /// Java's `consumeMessages(AtomicInteger, int, String, int, int, boolean,
    /// int)`: as [`Self::consume_messages`], with `max.partition.fetch.bytes`.
    #[expect(clippy::too_many_arguments, reason = "the Java helper's parameters")]
    pub async fn consume_messages_with_max_fetch_bytes(
        &self,
        total_messages_consumed: &AtomicUsize,
        total_messages: usize,
        group_id: &str,
        consumer_number: usize,
        max_polls: usize,
        commit: bool,
        max_fetch_bytes: usize,
    ) -> usize {
        self.consume_messages_with_max_fetch_bytes_option(
            total_messages_consumed,
            total_messages,
            group_id,
            consumer_number,
            max_polls,
            commit,
            Some(max_fetch_bytes),
        )
        .await
    }

    #[expect(clippy::too_many_arguments, reason = "the Java helper's parameters")]
    async fn consume_messages_with_max_fetch_bytes_option(
        &self,
        total_messages_consumed: &AtomicUsize,
        total_messages: usize,
        group_id: &str,
        consumer_number: usize,
        max_polls: usize,
        commit: bool,
        max_fetch_bytes: Option<usize>,
    ) -> usize {
        let mut properties = HashMap::new();
        if let Some(max_fetch_bytes) = max_fetch_bytes {
            properties.insert("max.partition.fetch.bytes".to_string(), max_fetch_bytes.to_string());
        }
        let mut share_consumer = self.create_share_consumer_with_properties(group_id, &properties);
        share_consumer
            .subscribe(vec![self.tp.topic().to_string()])
            .await
            .unwrap_or_else(|e| panic!("Consumer {consumer_number} failed with exception: {e}"));
        let consumed = consume_messages_with_consumer(
            &mut share_consumer,
            total_messages_consumed,
            total_messages,
            consumer_number,
            max_polls,
            commit,
        )
        .await;
        close_share_consumer(share_consumer).await;
        consumed
    }

    // -- topics ------------------------------------------------------------

    /// Java's `createTopic(String)`: one partition, replication factor 1.
    pub async fn create_topic(&self, topic_name: &str) -> Uuid {
        self.create_topic_with_partitions_replication_factor(topic_name, 1, 1).await
    }

    /// Java's `createTopic(String, int, int)`. Returns the topic id.
    pub async fn create_topic_with_partitions_replication_factor(
        &self,
        topic_name: &str,
        num_partitions: i32,
        replication_factor: i16,
    ) -> Uuid {
        let admin = self.create_admin_client();
        let result = admin.create_topics(&[NewTopic::with_num_partitions_replication_factor(
            topic_name.to_string(),
            Some(num_partitions),
            Some(replication_factor),
        )]);
        result
            .all()
            .get()
            .await
            .unwrap_or_else(|e| panic!("Failed to create topic: {e}"));
        let topic_id = result
            .topic_id(topic_name)
            .get()
            .await
            .unwrap_or_else(|e| panic!("Failed to create topic: {e}"));
        close_admin(admin).await;
        topic_id
    }

    /// Java's `deleteTopic`.
    pub async fn delete_topic(&self, topic_name: &str) {
        let admin = self.create_admin_client();
        admin
            .delete_topics_with_string_slice(&[topic_name.to_string()])
            .all()
            .get()
            .await
            .unwrap_or_else(|e| panic!("Failed to delete topic: {e}"));
        close_admin(admin).await;
    }

    // -- clients -----------------------------------------------------------

    /// Java's `createAdminClient` (`cluster.admin()`).
    pub fn create_admin_client(&self) -> Box<dyn Admin> {
        let mut props = HashMap::new();
        self.ctx.configure(&mut props);
        AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
    }

    /// Java's `createProducer()`.
    pub async fn create_producer(&self) -> BytesProducer {
        self.create_producer_with_config(&HashMap::new())
    }

    /// Java's `createProducer(Map)` (`cluster.producer(config)`): byte-array
    /// serializers.
    pub fn create_producer_with_config(&self, config: &HashMap<String, String>) -> BytesProducer {
        let mut props = HashMap::new();
        self.ctx.configure(&mut props);
        props.extend(config.iter().map(|(k, v)| (k.clone(), v.clone())));
        KafkaProducer::new(
            ProducerConfig::new(&props).expect("valid producer config"),
            ByteArraySerializer::new(),
            ByteArraySerializer::new(),
        )
        .expect("KafkaProducer::new")
    }

    /// Java's `createProducer(String transactionalId)`.
    pub fn create_producer_with_transactional_id(&self, transactional_id: &str) -> BytesProducer {
        self.create_producer_with_config(&HashMap::from([(
            "transactional.id".to_string(),
            transactional_id.to_string(),
        )]))
    }

    /// Java's `createShareConsumer(String)`.
    pub fn create_share_consumer(&self, group_id: &str) -> BytesShareConsumer {
        self.create_share_consumer_with_properties(group_id, &HashMap::new())
    }

    /// Java's `createShareConsumer(String, Map)`.
    pub fn create_share_consumer_with_properties(
        &self,
        group_id: &str,
        additional_properties: &HashMap<String, String>,
    ) -> BytesShareConsumer {
        self.create_share_consumer_with_deserializers(
            group_id,
            additional_properties,
            ByteArrayDeserializer::new(),
            ByteArrayDeserializer::new(),
        )
    }

    /// Java's `createShareConsumer(String, Map, Deserializer, Deserializer)`
    /// (`cluster.shareConsumer(conf, keyDeserializer, valueDeserializer)`).
    pub fn create_share_consumer_with_deserializers<K, V>(
        &self,
        group_id: &str,
        additional_properties: &HashMap<String, String>,
        key_deserializer: impl Deserializer<K>,
        value_deserializer: impl Deserializer<V>,
    ) -> KafkaShareConsumer<K, V>
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        self.groups.lock().expect("groups lock").insert(group_id.to_string());
        let mut props = HashMap::new();
        self.ctx.configure(&mut props);
        props.extend(additional_properties.iter().map(|(k, v)| (k.clone(), v.clone())));
        props.insert("group.id".to_string(), group_id.to_string());
        KafkaShareConsumer::new(&props, key_deserializer, value_deserializer).expect("KafkaShareConsumer::new")
    }

    /// Records that this test uses the share group `group_id` through a client
    /// the fixture did not build (a `ComplexShareConsumer`), for
    /// [`Self::verify_share_group_state_topic_records_produced`].
    pub fn register_group(&self, group_id: &str) {
        self.groups.lock().expect("groups lock").insert(group_id.to_string());
    }

    // -- warmup and verification ---------------------------------------------

    /// Java's `warmup()`: creates `warmup`, waits for the metadata, and consumes
    /// one record through a share group, so the share coordinator is loaded
    /// before the test starts.
    pub async fn warmup(&self) {
        self.create_topic(self.warmup_tp.topic()).await;
        self.wait_for_metadata_cache().await;
        let record = producer_record(
            self.warmup_tp.topic().to_string(),
            Some(self.warmup_tp.partition()),
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
        );
        let subscription = vec![self.warmup_tp.topic().to_string()];
        let warmup_group = self.group("warmupgroup1");
        self.alter_share_auto_offset_reset(&warmup_group, "earliest").await;
        let producer = self.create_producer().await;
        // Not a test group: its state records are not this test's.
        let mut share_consumer = self.create_share_consumer_with_deserializers(
            &warmup_group,
            &HashMap::new(),
            ByteArrayDeserializer::new(),
            ByteArrayDeserializer::new(),
        );
        self.groups.lock().expect("groups lock").remove(&warmup_group);

        send_record(&producer, record).await.expect("send");
        producer.flush().await.expect("flush");

        share_consumer.subscribe(subscription).await.expect("subscribe");
        let mut deadline = WaitDeadline::new(30_000, 200, "warmup record not received");
        while share_consumer.poll(Duration::from_millis(5000)).await.expect("poll").count() != 1 {
            deadline.fail_or_pause().await;
        }
        close_producer(producer).await;
        close_share_consumer(share_consumer).await;
    }

    /// Java's `waitForMetadataCache()`: waits until the broker lists an alive
    /// broker. Java reads the broker's metadata cache; this asks
    /// `describeCluster`, the client-visible view of the same cache.
    pub async fn wait_for_metadata_cache(&self) {
        let admin = self.create_admin_client();
        let admin_ref = &admin;
        wait_for_condition_with_pause(
            || async move {
                admin_ref
                    .describe_cluster()
                    .nodes()
                    .get()
                    .await
                    .is_ok_and(|nodes| !nodes.is_empty())
            },
            DEFAULT_MAX_WAIT_MS,
            100,
            "cache not up yet",
        )
        .await;
        close_admin(admin).await;
    }

    /// Java's `verifyShareGroupStateTopicRecordsProduced()`: reads
    /// `__share_group_state` from the beginning until it holds share state of
    /// one of this test's share groups (see the module docs for why not Java's
    /// "more than two records").
    pub async fn verify_share_group_state_topic_records_produced(&self) {
        let groups: Vec<Vec<u8>> = self
            .groups
            .lock()
            .expect("groups lock")
            .iter()
            .map(|group| group.as_bytes().to_vec())
            .collect();
        assert!(!groups.is_empty(), "the test used no share group");
        let mut props = HashMap::from([
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            ("group.id".to_string(), self.group(&format!("group_{}", random_string(5)))),
            ("group.protocol".to_string(), "consumer".to_string()),
        ]);
        self.ctx.configure(&mut props);
        let mut consumer = KafkaConsumer::<Vec<u8>, Vec<u8>>::new(
            ConsumerConfig::new(&props).expect("valid consumer config"),
            ByteArrayDeserializer::new(),
            ByteArrayDeserializer::new(),
        )
        .expect("KafkaConsumer::new");
        consumer.assign(self.sgs_topic_partitions.clone()).await.expect("assign");
        consumer
            .seek_to_beginning(&self.sgs_topic_partitions)
            .await
            .expect("seek to beginning");
        let mut deadline = WaitDeadline::new(30_000, 200, "no records produced");
        let mut found = false;
        loop {
            let msgs = consumer.poll(Duration::from_millis(5000)).await.expect("poll");
            for record in msgs.records_with_topic(SHARE_GROUP_STATE_TOPIC_NAME) {
                let key = record.key().map(Vec::as_slice).unwrap_or_default();
                if groups.iter().any(|group| contains(key, group)) {
                    found = true;
                }
            }
            if found {
                break;
            }
            deadline.fail_or_pause().await;
        }
        tokio::time::timeout(CLOSE_TIMEOUT, consumer.close())
            .await
            .expect("consumer close did not return in time")
            .expect("consumer close");
    }

    // -- group configs -------------------------------------------------------

    /// Java's `alterShareGroupConfig`: sets the group config `config_key` and
    /// waits until a new admin client describes the new value.
    pub async fn alter_share_group_config(&self, group_id: &str, config_key: &str, new_value: &str) {
        let config_resource = ConfigResource::new(config_resource::Type::Group, group_id.to_string());
        let alter_entries = HashMap::from([(
            config_resource.clone(),
            vec![AlterConfigOp::new(
                ConfigEntry::new(config_key.to_string(), Some(new_value.to_string())),
                OpType::Set,
            )],
        )]);
        let admin = self.create_admin_client();
        admin
            .incremental_alter_configs_with_options(&alter_entries, AlterConfigsOptions::new())
            .all()
            .get_with_timeout(Duration::from_secs(60))
            .await
            .unwrap_or_else(|e| panic!("Failed to alter configs: {e}"));
        close_admin(admin).await;

        // This config is changed dynamically in tests, and we need it to have propagated before the test proceeds.
        // Describing the config with a new admin client is not totally foolproof, but it's better than just
        // altering the config and continuing.
        let admin = self.create_admin_client();
        let admin_ref = &admin;
        let resource_ref = &config_resource;
        wait_for_condition_with_pause(
            || async move {
                let configs = admin_ref
                    .describe_configs(std::slice::from_ref(resource_ref))
                    .all()
                    .get()
                    .await
                    .unwrap_or_else(|e| panic!("Failed to describe configs: {e}"));
                configs
                    .get(resource_ref)
                    .and_then(|config| config.get(config_key))
                    .is_some_and(|entry| entry.value() == Some(new_value))
            },
            10_000,
            100,
            "New config value did not propagate",
        )
        .await;
        close_admin(admin).await;
    }

    /// Java's `alterShareAutoOffsetReset` (`share.auto.offset.reset`).
    pub async fn alter_share_auto_offset_reset(&self, group_id: &str, new_value: &str) {
        self.alter_share_group_config(group_id, "share.auto.offset.reset", new_value)
            .await;
    }

    /// Java's `alterShareDeliveryCountLimit` (`share.delivery.count.limit`).
    pub async fn alter_share_delivery_count_limit(&self, group_id: &str, new_value: &str) {
        self.alter_share_group_config(group_id, "share.delivery.count.limit", new_value)
            .await;
    }

    /// Java's `alterShareIsolationLevel` (`share.isolation.level`).
    pub async fn alter_share_isolation_level(&self, group_id: &str, new_value: &str) {
        self.alter_share_group_config(group_id, "share.isolation.level", new_value)
            .await;
    }

    /// Java's `alterShareRenewAcknowledgeEnable` (`share.renew.acknowledge.enable`).
    pub async fn alter_share_renew_acknowledge_enable(&self, group_id: &str, new_value: bool) {
        self.alter_share_group_config(group_id, "share.renew.acknowledge.enable", &new_value.to_string())
            .await;
    }

    /// Java's `alterSharePartitionMaxRecordLocks` (`share.partition.max.record.locks`).
    pub async fn alter_share_partition_max_record_locks(&self, group_id: &str, new_value: &str) {
        self.alter_share_group_config(group_id, "share.partition.max.record.locks", new_value)
            .await;
    }

    /// Java's `alterShareRecordLockDurationMs` (`share.record.lock.duration.ms`).
    pub async fn alter_share_record_lock_duration_ms(&self, group_id: &str, new_value: i32) {
        self.alter_share_group_config(group_id, "share.record.lock.duration.ms", &new_value.to_string())
            .await;
    }

    // -- admin share-group helpers -------------------------------------------

    /// Java's `topicPartitionLeader`: the leader id of `partition`, empty while
    /// it has none.
    pub async fn topic_partition_leader(
        &self,
        admin_client: &dyn Admin,
        topic_name: &str,
        partition: i32,
    ) -> Result<Vec<i32>, Error> {
        let descriptions = admin_client
            .describe_topics_with_topic_names(&[topic_name.to_string()])
            .all_topic_names()
            .expect("described by name")
            .get()
            .await?;
        Ok(descriptions
            .get(topic_name)
            .map(|description| {
                description
                    .partitions()
                    .iter()
                    .filter(|info| info.partition() == partition)
                    .filter_map(|info| info.leader().map(|leader| leader.id()))
                    .filter(|id| *id != -1)
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Java's `sharePartitionOffsetInfo`: `listShareGroupOffsets` for one
    /// partition, `None` when the group has no offset for it.
    pub async fn share_partition_offset_info(
        &self,
        admin_client: &dyn Admin,
        group_id: &str,
        tp: &TopicPartition,
    ) -> Result<Option<SharePartitionOffsetInfo>, Error> {
        let result = admin_client.list_share_group_offsets_with_options(
            &HashMap::from([(
                group_id.to_string(),
                ListShareGroupOffsetsSpec::new().set_topic_partitions(Some(vec![tp.clone()])),
            )]),
            ListShareGroupOffsetsOptions::new().set_timeout_ms(Some(30_000)),
        );
        let offsets = result.partitions_to_offset_info(group_id)?.get().await?;
        Ok(offsets.get(tp).cloned().flatten())
    }

    /// Java's `verifySharePartitionStartOffset`.
    pub async fn verify_share_partition_start_offset(
        &self,
        admin_client: &dyn Admin,
        group_id: &str,
        tp: &TopicPartition,
        expected_start_offset: i64,
    ) {
        wait_for_condition_with_pause(
            || async move {
                self.share_partition_offset_info(admin_client, group_id, tp)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|info| info.start_offset() == expected_start_offset)
            },
            DEFAULT_MAX_WAIT_MS,
            DEFAULT_POLL_INTERVAL_MS,
            "Failed to retrieve share partition lag",
        )
        .await;
    }

    /// Java's `verifySharePartitionLag`.
    pub async fn verify_share_partition_lag(
        &self,
        admin_client: &dyn Admin,
        group_id: &str,
        tp: &TopicPartition,
        expected_lag: i64,
    ) {
        wait_for_condition_with_pause(
            || async move {
                self.share_partition_offset_info(admin_client, group_id, tp)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|info| info.lag() == Some(expected_lag))
            },
            DEFAULT_MAX_WAIT_MS,
            DEFAULT_POLL_INTERVAL_MS,
            "Failed to retrieve share partition lag",
        )
        .await;
    }

    /// Java's `verifySharePartitionOffsetsDeleted`.
    pub async fn verify_share_partition_offsets_deleted(
        &self,
        admin_client: &dyn Admin,
        group_id: &str,
        tp: &TopicPartition,
    ) {
        wait_for_condition_with_pause(
            || async move { matches!(self.share_partition_offset_info(admin_client, group_id, tp).await, Ok(None)) },
            DEFAULT_MAX_WAIT_MS,
            DEFAULT_POLL_INTERVAL_MS,
            "Failed to retrieve share partition lag",
        )
        .await;
    }

    /// Java's `alterShareGroupOffsets`.
    pub async fn alter_share_group_offsets(
        &self,
        admin_client: &dyn Admin,
        group_id: &str,
        topic_partition: &TopicPartition,
        new_offset: i64,
    ) -> Result<(), Error> {
        admin_client
            .alter_share_group_offsets_with_options(
                group_id,
                &HashMap::from([(topic_partition.clone(), new_offset)]),
                AlterShareGroupOffsetsOptions::new().set_timeout_ms(Some(30_000)),
            )
            .partition_result(topic_partition)
            .get()
            .await
    }

    /// Java's `deleteShareGroupOffsets`.
    pub async fn delete_share_group_offsets(
        &self,
        admin_client: &dyn Admin,
        group_id: &str,
        topic: &str,
    ) -> Result<(), Error> {
        admin_client
            .delete_share_group_offsets_with_options(
                group_id,
                &HashSet::from([topic.to_string()]),
                DeleteShareGroupOffsetsOptions::new().set_timeout_ms(Some(30_000)),
            )
            .topic_result(topic)?
            .get()
            .await
    }

    /// Java's `waitForAssignment`: waits until the members of `group_id` are
    /// assigned every partition of `tps`.
    pub async fn wait_for_assignment(&self, group_id: &str, tps: &[TopicPartition]) {
        wait_for_condition_with_pause(
            || async move { self.is_assigned(group_id, tps).await },
            DEFAULT_MAX_WAIT_MS,
            1000,
            "tps not assigned to members",
        )
        .await;
    }

    async fn is_assigned(&self, group_id: &str, tps: &[TopicPartition]) -> bool {
        let admin = self.create_admin_client();
        let described = admin
            .describe_share_groups_with_options(
                &[group_id.to_string()],
                DescribeShareGroupsOptions::new().set_include_authorized_operations(true),
            )
            .described_groups()
            .remove(group_id)
            .expect("the group was described")
            .get()
            .await;
        close_admin(admin).await;
        let description = described.unwrap_or_else(|e| panic!("describeShareGroups failed: {e}"));
        let assigned: HashSet<&TopicPartition> = description
            .members()
            .iter()
            .flat_map(|member| member.assignment().topic_partitions())
            .collect();
        tps.iter().all(|tp| assigned.contains(tp))
    }

    // -- polling -------------------------------------------------------------

    /// Java's `waitedPoll(ShareConsumer, long, int)`.
    pub async fn waited_poll(
        &self,
        share_consumer: &mut BytesShareConsumer,
        poll_ms: u64,
        record_count: usize,
    ) -> ConsumerRecords<Vec<u8>, Vec<u8>> {
        self.waited_poll_with_assignment(share_consumer, poll_ms, record_count, false, "", &[])
            .await
    }

    /// Java's `waitedPoll(ShareConsumer, long, int, boolean, String, List)`:
    /// polls until a poll returns `record_count` records, checking the
    /// assignment of `group_id` after each poll when `check_assignment`.
    pub async fn waited_poll_with_assignment(
        &self,
        share_consumer: &mut BytesShareConsumer,
        poll_ms: u64,
        record_count: usize,
        check_assignment: bool,
        group_id: &str,
        tps: &[TopicPartition],
    ) -> ConsumerRecords<Vec<u8>, Vec<u8>> {
        let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 500, "failed to get records");
        loop {
            let recs = share_consumer.poll(Duration::from_millis(poll_ms)).await.expect("poll");
            if check_assignment {
                self.wait_for_assignment(group_id, tps).await;
            }
            if recs.count() == record_count {
                return recs;
            }
            deadline.fail_or_pause().await;
        }
    }

    /// Java's `validateExpectedRecordsInEachPollAndRelease`.
    pub async fn validate_expected_records_in_each_poll_and_release(
        &self,
        share_consumer: &mut BytesShareConsumer,
        start_offset: i64,
        last_offset: i64,
        expected_records_in_each_poll: usize,
    ) {
        self.validate_expected_records_in_each_poll_and_acknowledge(
            share_consumer,
            start_offset,
            last_offset,
            expected_records_in_each_poll,
            AcknowledgeType::Release,
        )
        .await;
    }

    /// Java's `validateExpectedRecordsInEachPollAndAcknowledge`.
    pub async fn validate_expected_records_in_each_poll_and_acknowledge(
        &self,
        share_consumer: &mut BytesShareConsumer,
        start_offset: i64,
        last_offset: i64,
        expected_records_in_each_poll: usize,
        acknowledge_type: AcknowledgeType,
    ) {
        let mut i = start_offset;
        while i < last_offset {
            let records = self.waited_poll(share_consumer, 2500, expected_records_in_each_poll).await;
            assert_eq!(expected_records_in_each_poll, records.count());
            // Verify the first offset of the fetched records.
            assert_eq!(i, (&records).into_iter().next().expect("a record").offset());
            for record in &records {
                share_consumer
                    .acknowledge_with_record_type(record, acknowledge_type)
                    .expect("acknowledge");
            }
            let result = share_consumer.commit_sync().await.expect("commitSync");
            assert_eq!(1, result.len());
            assert!(result.get(&self.tip()).expect("tp in the commit result").is_none());
            i += expected_records_in_each_poll as i64;
        }
    }
}

// ---------------------------------------------------------------------------
// ComplexShareConsumer
// ---------------------------------------------------------------------------

/// The record processing of a [`ComplexShareConsumer`].
pub type ProcessFunc = Arc<dyn Fn(&mut BytesShareConsumer, &BytesRecord) + Send + Sync>;
/// The exit criteria of a [`ComplexShareConsumer`].
pub type ExitCriteria = Arc<dyn Fn(&ConsumerRecords<Vec<u8>, Vec<u8>>) -> bool + Send + Sync>;

/// Java's `ComplexShareConsumer`: a share consumer whose record processing is
/// supplied as a function, run on its own task by [`Self::run`].
///
/// This can be used to create different consume patterns on the broker and
/// study the status of broker side share group abstractions.
pub struct ComplexShareConsumer {
    topic_name: String,
    configs: HashMap<String, String>,
    state: ClientState,
    exit_criteria: ExitCriteria,
    process_func: ProcessFunc,
}

impl ComplexShareConsumer {
    /// Java's `POLL_TIMEOUT_MS`.
    pub const POLL_TIMEOUT_MS: u64 = 15_000;
    /// Java's `MAX_DELIVERY_COUNT`.
    pub const MAX_DELIVERY_COUNT: i64 = SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT;

    /// Java's four-argument constructor: exits on an empty poll; rejects,
    /// accepts or releases each record by its offset and delivery count.
    pub fn new(
        bootstrap_servers: &str,
        topic_name: &str,
        group_id: &str,
        additional_properties: &HashMap<String, String>,
    ) -> Self {
        Self::with_exit_criteria_process_func(
            bootstrap_servers,
            topic_name,
            group_id,
            additional_properties,
            Arc::new(|records| records.count() == 0),
            Arc::new(|consumer, record| {
                let max = Self::MAX_DELIVERY_COUNT;
                let delivery_count_before_accept = ((record.offset() + record.offset() / (max + 2)) % (max + 2)) as i16;
                if delivery_count_before_accept == 0 {
                    consumer
                        .acknowledge_with_record_type(record, AcknowledgeType::Reject)
                        .expect("acknowledge");
                } else if record.delivery_count() == Some(delivery_count_before_accept) {
                    consumer
                        .acknowledge_with_record_type(record, AcknowledgeType::Accept)
                        .expect("acknowledge");
                } else {
                    consumer
                        .acknowledge_with_record_type(record, AcknowledgeType::Release)
                        .expect("acknowledge");
                }
            }),
        )
    }

    /// Java's six-argument constructor.
    pub fn with_exit_criteria_process_func(
        bootstrap_servers: &str,
        topic_name: &str,
        group_id: &str,
        additional_properties: &HashMap<String, String>,
        exit_criteria: ExitCriteria,
        process_func: ProcessFunc,
    ) -> Self {
        let mut configs = HashMap::from([
            ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
            ("group.id".to_string(), group_id.to_string()),
        ]);
        configs.extend(additional_properties.iter().map(|(k, v)| (k.clone(), v.clone())));
        Self {
            topic_name: topic_name.to_string(),
            configs,
            state: ClientState::default(),
            exit_criteria,
            process_func,
        }
    }

    /// Java's `run()`: subscribes and polls until the exit criteria hold,
    /// processing every record. Run it on its own task.
    pub async fn run(self: Arc<Self>) {
        let mut consumer =
            KafkaShareConsumer::new(&self.configs, ByteArrayDeserializer::new(), ByteArrayDeserializer::new())
                .expect("KafkaShareConsumer::new");
        consumer.subscribe(vec![self.topic_name.clone()]).await.expect("subscribe");
        while !self.state.done().load(Ordering::SeqCst) {
            let records = consumer.poll(Duration::from_millis(Self::POLL_TIMEOUT_MS)).await.expect("poll");
            self.state.count().fetch_add(records.count(), Ordering::SeqCst);
            if (self.exit_criteria)(&records) {
                self.state.done().store(true, Ordering::SeqCst);
            }
            for record in &records {
                (self.process_func)(&mut consumer, record);
            }
        }
        close_share_consumer(consumer).await;
    }

    /// Java's `isDone()`.
    pub fn is_done(&self) -> bool {
        self.state.done().load(Ordering::SeqCst)
    }

    /// Java's `recordsRead()`.
    pub fn records_read(&self) -> usize {
        self.state.count().load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Java's `consumeMessages(ShareConsumer, AtomicInteger, int, int, int,
/// boolean)`: polls (2 s each) until `total_messages_consumed` reaches
/// `total_messages` or `max_polls` polls ran (all `max_polls` when
/// `total_messages` is 0), then commits when `commit`. Returns the records this
/// consumer read.
pub async fn consume_messages_with_consumer(
    consumer: &mut BytesShareConsumer,
    total_messages_consumed: &AtomicUsize,
    total_messages: usize,
    consumer_number: usize,
    max_polls: usize,
    commit: bool,
) -> usize {
    let mut messages_consumed = 0;
    let mut retries = 0;
    let fail = |e: Error| -> ! { panic!("Consumer {consumer_number} failed with exception: {e}") };
    if total_messages > 0 {
        while total_messages_consumed.load(Ordering::SeqCst) < total_messages && retries < max_polls {
            let records = consumer.poll(Duration::from_millis(2000)).await.unwrap_or_else(|e| fail(e));
            messages_consumed += records.count();
            total_messages_consumed.fetch_add(records.count(), Ordering::SeqCst);
            retries += 1;
        }
    } else {
        while retries < max_polls {
            let records = consumer.poll(Duration::from_millis(2000)).await.unwrap_or_else(|e| fail(e));
            messages_consumed += records.count();
            total_messages_consumed.fetch_add(records.count(), Ordering::SeqCst);
            retries += 1;
        }
    }

    if commit {
        // Complete acknowledgement of the records
        consumer
            .commit_sync_with_timeout(Duration::from_millis(10_000))
            .await
            .unwrap_or_else(|e| fail(e));
    }
    messages_consumed
}

/// Java's `consumeRecords`: polls (100 ms each) until `num_records` records
/// were read, failing after 60 s.
pub async fn consume_records<K, V>(
    consumer: &mut KafkaShareConsumer<K, V>,
    num_records: usize,
) -> Vec<ConsumerRecord<K, V>>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let mut accumulated_records = Vec::new();
    let start = Instant::now();
    while accumulated_records.len() < num_records {
        let records = consumer.poll(Duration::from_millis(100)).await.expect("poll");
        accumulated_records.extend(records);
        assert!(
            start.elapsed() <= Duration::from_secs(60),
            "Timed out before consuming expected records."
        );
    }
    accumulated_records
}

/// Java's `TestUtils.waitForCondition(condition, maxWaitMs, pollIntervalMs,
/// details)`: polls `condition` every `poll_interval_ms` and fails with
/// `Condition not met within timeout <max_wait_ms>. <details>`.
pub async fn wait_for_condition_with_pause<F, Fut>(condition: F, max_wait_ms: u64, poll_interval_ms: u64, details: &str)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let mut condition = condition;
    test_utils::retry_on_error_with_timeout_poll(
        Duration::from_millis(max_wait_ms),
        Duration::from_millis(poll_interval_ms),
        || {
            let met = condition();
            async move {
                if met.await {
                    Ok(())
                } else {
                    Err(format!("Condition not met within timeout {max_wait_ms}. {details}"))
                }
            }
        },
    )
    .await;
}

/// The deadline of a Java `TestUtils.waitForCondition` written as a loop, for
/// a condition that borrows the consumer mutably. Java's helper checks the
/// condition, fails once the deadline passed, and otherwise sleeps
/// `poll_interval_ms`.
pub struct WaitDeadline {
    end: Instant,
    max_wait_ms: u64,
    poll_interval: Duration,
    details: String,
}

impl WaitDeadline {
    /// Starts the clock of a `waitForCondition(..., max_wait_ms,
    /// poll_interval_ms, () -> details)`.
    pub fn new(max_wait_ms: u64, poll_interval_ms: u64, details: &str) -> Self {
        Self {
            end: Instant::now() + Duration::from_millis(max_wait_ms),
            max_wait_ms,
            poll_interval: Duration::from_millis(poll_interval_ms.min(max_wait_ms)),
            details: details.to_string(),
        }
    }

    /// Called after the condition was not met: fails the test once the
    /// deadline passed, otherwise sleeps the poll interval.
    pub async fn fail_or_pause(&mut self) {
        if Instant::now() >= self.end {
            panic!("Condition not met within timeout {}. {}", self.max_wait_ms, self.details);
        }
        tokio::time::sleep(self.poll_interval).await;
    }
}

/// Java's `new ProducerRecord<>(topic, partition, null, key, value)`.
pub fn producer_record(
    topic: String,
    partition: Option<i32>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    ProducerRecord::with_partition_key(topic, partition, key, value).expect("a valid record")
}

/// Java's `new ProducerRecord<>(topic, partition, timestamp, key, value)`.
pub fn producer_record_with_timestamp(
    topic: String,
    partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    ProducerRecord::with_partition_timestamp_key(topic, partition, timestamp, key, value).expect("a valid record")
}

/// `KafkaProducer::send` through the `Producer` trait (the inherent method of
/// the same name takes borrowed bytes and a callback).
pub async fn send_record(
    producer: &BytesProducer,
    record: ProducerRecord<Vec<u8>, Vec<u8>>,
) -> Result<confluent_kafka::common::KafkaFuture<confluent_kafka::producer::RecordMetadata>, Error> {
    <BytesProducer as Producer<Vec<u8>, Vec<u8>>>::send(producer, record).await
}

/// The JMX port the brokers of a [`with_broker_jmx`] cluster listen on, inside
/// their containers only (it is not mapped to the host).
pub const BROKER_JMX_PORT: u16 = 9999;

/// `config` with the brokers' JMX enabled on [`BROKER_JMX_PORT`] (the
/// `apache/kafka` image's `KAFKA_JMX_PORT`), so [`broker_meter_count`] can read
/// their meters.
pub fn with_broker_jmx(mut config: ClusterConfig) -> ClusterConfig {
    config
        .server_properties
        .insert("KAFKA_JMX_PORT".to_string(), BROKER_JMX_PORT.to_string());
    config
}

/// The `Count` of the broker meter `object_name` (a JMX object name such as
/// `kafka.server:type=ShareGroupMetrics,name=DeadLetterQueueRecordCount,group=g`)
/// on the first broker of a [`with_broker_jmx`] cluster, or `-1` when the
/// broker has not registered it.
///
/// Java's suites read the meter from the broker's Yammer registry, which runs
/// in the test JVM (`KafkaYammerMetrics.defaultRegistry()`). The broker here
/// runs in a container, so this reads the same meter through the broker's JMX
/// with the image's own `org.apache.kafka.tools.JmxTool`, run by `docker exec`.
pub async fn broker_meter_count(ctx: &TestContext, object_name: &str) -> i64 {
    let container = ctx.cluster().container_ids().first().expect("a broker container").clone();
    let query = object_name.to_string();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args([
                "exec",
                &container,
                "/opt/kafka/bin/kafka-run-class.sh",
                "org.apache.kafka.tools.JmxTool",
                "--jmx-url",
                &format!("service:jmx:rmi:///jndi/rmi://localhost:{BROKER_JMX_PORT}/jmxrmi"),
                "--object-name",
                &query,
                "--attributes",
                "Count",
                "--one-time",
                "true",
            ])
            .output()
            .expect("docker exec JmxTool")
    })
    .await
    .expect("JmxTool task");
    // JmxTool exits with an error when no registered object matches the name:
    // that is the unregistered meter. Any other failure (no connection) fails
    // the caller rather than reading as an unregistered meter.
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        assert!(
            stderr.contains("No matched attributes for the queried objects"),
            "JmxTool failed for {object_name}: {stderr}"
        );
        return -1;
    }
    // The output is a CSV header (`"time","<object name>:Count"`) and one row of
    // values. For an object name the broker has not registered the header is
    // `"time"` alone, and the row only the time.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines().filter(|line| !line.trim().is_empty());
    let header = lines.by_ref().find(|line| line.starts_with("\"time\""));
    if !header.is_some_and(|header| header.contains(":Count\"")) {
        return -1;
    }
    lines
        .next()
        .and_then(|row| row.rsplit(',').next())
        .and_then(|count| count.trim().parse::<i64>().ok())
        .unwrap_or_else(|| panic!("JmxTool printed no count for {object_name}: {stdout}"))
}

/// Closes `consumer`, failing the test when the close fails or hangs.
pub async fn close_share_consumer<K, V>(mut consumer: KafkaShareConsumer<K, V>)
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    tokio::time::timeout(CLOSE_TIMEOUT, consumer.close())
        .await
        .expect("share consumer close did not return in time")
        .expect("share consumer close");
}

/// Closes `producer`, failing the test when the close fails or hangs.
pub async fn close_producer(producer: BytesProducer) {
    tokio::time::timeout(CLOSE_TIMEOUT, producer.close())
        .await
        .expect("producer close did not return in time")
        .expect("producer close");
}

/// Closes `admin`, failing the test when the close hangs.
pub async fn close_admin(admin: Box<dyn Admin>) {
    tokio::time::timeout(CLOSE_TIMEOUT, admin.close())
        .await
        .expect("admin close did not return in time");
}

/// `TestUtils.randomString(length)`: random lowercase letters.
pub fn random_string(length: usize) -> String {
    (0..length).map(|_| char::from(b'a' + rand::random_range(0..26u8))).collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|window| window == needle)
}
