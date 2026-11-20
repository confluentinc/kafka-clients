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

//! Integration tests translated from
//! `org.apache.kafka.clients.consumer.ShareConsumerDLQTest` (Apache Kafka 4.4.0,
//! `clients-integration-tests`): the share group dead letter queue (KIP-1191).
//!
//! The fixture is [`super::share_consumer_test_base`]; its module docs list the
//! deviations every share suite shares (pooled clusters, per-test names).
//!
//! # Broker version
//!
//! Every test needs a 4.4 broker and skips itself on an older one
//! ([`SHARE_DLQ_RELEASE`]): the DLQ needs `share.version=2`
//! (`ShareVersion#supportsShareGroupDLQ`), and the `errors.deadletterqueue.*`
//! group, topic and broker configs the tests set exist only from 4.4.
//!
//! # Deviations from Java
//!
//! - **Broker meters.** Java reads the per-group `ShareGroupMetrics` meters
//!   (`DeadLetterQueueRecordCount`, `DeadLetterQueueTotalProduceRequestsPerSec`)
//!   from the broker's Yammer registry in the test JVM. The broker runs in a
//!   container here, so the suite enables the broker's JMX
//!   ([`with_broker_jmx`]) and reads the same meters with
//!   [`broker_meter_count`]. The meters are per group and every group name is
//!   per test, so the pooled cluster gives each test the counts Java's fresh
//!   cluster gives it.
//! - **DLQ topic names.** The broker's DLQ topic prefix is `dlq.`, so Java's
//!   `dlq.topic`, `dlq.release`, … become `dlq.<per-test name>`, and Java's
//!   `wrong.prefix` becomes `wrong.<per-test name>`.
//!
//! # Skipped
//!
//! - `testDlqCopiesRecordsReadFromRemoteStorage` and
//!   `testDlqCopiesRecordsReadFromRemoteAndLocalStorage` configure the broker
//!   with `org.apache.kafka.server.log.remote.storage.LocalTieredStorage`, a
//!   class of the `storage` module's test fixtures: no jar in the
//!   `apache/kafka:4.4.0` image's `/opt/kafka/libs` contains it (checked by
//!   listing every jar), so the broker cannot start with that remote storage
//!   manager. They also name `remote.log.metadata.manager.listener.name=EXTERNAL`,
//!   the Java test kit's listener, which the harness's brokers do not have.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::AlterConfigOp;
use confluent_kafka::admin::AlterConfigsOptions;
use confluent_kafka::admin::ConfigEntry;
use confluent_kafka::admin::NewTopic;
use confluent_kafka::admin::OpType;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::config::ConfigResource;
use confluent_kafka::common::config::config_resource;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::consumer::AcknowledgeType;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::producer::Producer;

use super::share_consumer_test_base::*;
use crate::common::cluster_config::ClusterConfig;

// DLQ context headers written onto each DLQ record. These mirror the (package-private) constants in
// org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager and form the wire contract for DLQ records.
const HEADER_DLQ_ERRORS_TOPIC: &str = "__dlq.errors.topic";
const HEADER_DLQ_ERRORS_PARTITION: &str = "__dlq.errors.partition";
const HEADER_DLQ_ERRORS_OFFSET: &str = "__dlq.errors.offset";
const HEADER_DLQ_ERRORS_GROUP: &str = "__dlq.errors.group";

// Yammer metric names registered by org.apache.kafka.server.share.metrics.ShareGroupMetrics.
const METRIC_DLQ_RECORD_COUNT: &str = "DeadLetterQueueRecordCount";
const METRIC_DLQ_PRODUCE_TOTAL: &str = "DeadLetterQueueTotalProduceRequestsPerSec";

/// `GroupConfig#ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG`.
const ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG: &str = "errors.deadletterqueue.topic.name";
/// `GroupConfig#ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG`.
const ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG: &str = "errors.deadletterqueue.copy.record.enable";
/// `TopicConfig#ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG`.
const ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG: &str = "errors.deadletterqueue.group.enable";
/// `TopicConfig#MAX_MESSAGE_BYTES_CONFIG`.
const MAX_MESSAGE_BYTES_CONFIG: &str = "max.message.bytes";

/// The class-level `@ClusterTestDefaults` of `ShareConsumerDLQTest`: no
/// `transaction.state.log.*`, and a 1.5 s share heartbeat.
const DLQ_CLUSTER_DEFAULTS: &[(&str, &str)] = &[
    ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
    ("KAFKA_GROUP_SHARE_MAX_PARTITION_MAX_RECORD_LOCKS", "10000"),
    ("KAFKA_GROUP_SHARE_PARTITION_MAX_RECORD_LOCKS", "10000"),
    ("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS", "15000"),
    ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
    ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1"),
    ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS", "3"),
    ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1"),
    ("KAFKA_GROUP_SHARE_MIN_HEARTBEAT_INTERVAL_MS", "1500"),
    ("KAFKA_GROUP_SHARE_HEARTBEAT_INTERVAL_MS", "1500"),
];

/// The suite's cluster, with the brokers' JMX on for [`dlq_meter_count`].
fn dlq_cluster_config() -> ClusterConfig {
    dlq_cluster_config_with(&[])
}

/// [`dlq_cluster_config`] plus a per-test `@ClusterTest(serverProperties = ...)`.
fn dlq_cluster_config_with(overrides: &[(&str, &str)]) -> ClusterConfig {
    let mut config = share_cluster_config(DLQ_CLUSTER_DEFAULTS);
    config
        .server_properties
        .extend(overrides.iter().map(|(key, value)| ((*key).to_string(), (*value).to_string())));
    with_broker_jmx(config)
}

/// The per-test name of a DLQ topic: the broker's default share-group DLQ
/// topic prefix is "dlq.", so the topic name must start with it.
fn dlq_topic_name(base: &mut ShareConsumerTestBase, name: &str) -> String {
    format!("dlq.{}", base.topic(name))
}

/// `share.acknowledgement.mode=explicit`.
fn explicit_mode() -> HashMap<String, String> {
    HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())])
}

/// Produces 5 records, rejects every one of them with a share consumer in EXPLICIT acknowledgement mode,
/// and verifies they are written to the configured DLQ topic. Record copy is disabled, so the DLQ records
/// carry only the context headers (no key/value). Finally asserts the DLQ metrics for records written and
/// produce requests enqueued.
#[tokio::test(flavor = "multi_thread")]
async fn test_rejected_records_written_to_dlq_with_copy_record_disabled() {
    if skip_below(SHARE_DLQ_RELEASE, "testRejectedRecordsWrittenToDlqWithCopyRecordDisabled") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-group");
    let dlq_topic = dlq_topic_name(&mut base, "topic");
    let record_count = 5;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    // Create the DLQ topic with DLQ enabled, and point the share group at it. Record copy is left
    // disabled (the default), so produced DLQ records contain headers only.
    create_dlq_topic(&base, &dlq_topic).await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;

    // Produce the source records onto "topic" (partition 0), created by the base setup.
    base.produce_messages(record_count).await;

    // Reject every record using an EXPLICIT-mode share consumer.
    reject_records(&base, &group_id, record_count).await;

    // Verify by reading from the DLQ topic, then assert the DLQ metrics.
    verify_dlq_topic_records(&base, &dlq_topic, &group_id, &expected_source_offsets(record_count), false).await;
    verify_dlq_metrics(&base, &group_id, record_count).await;
}

/// As `test_rejected_records_written_to_dlq_with_copy_record_disabled`, but the DLQ topic is not created up
/// front: with DLQ auto topic creation enabled on the broker, the broker should create the configured DLQ
/// topic on the first write. Verifies the topic was created (with DLQ enabled), received the records, and
/// that the DLQ metrics fired.
#[tokio::test(flavor = "multi_thread")]
async fn test_rejected_records_written_to_auto_created_dlq() {
    if skip_below(SHARE_DLQ_RELEASE, "testRejectedRecordsWrittenToAutoCreatedDlq") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config_with(&[(
        "KAFKA_ERRORS_DEADLETTERQUEUE_AUTO_CREATE_TOPICS_ENABLE",
        "true",
    )]))
    .await;
    let group_id = base.group("dlq-autocreate-group");
    let dlq_topic = dlq_topic_name(&mut base, "autocreate");
    let record_count = 5;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    // Point the share group at a DLQ topic that does NOT exist yet; the broker should auto-create it.
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;

    base.produce_messages(record_count).await;
    reject_records(&base, &group_id, record_count).await;

    // Verify the DLQ topic was auto-created (with DLQ enabled), received the records, and metrics fired.
    verify_dlq_topic_created(&base, &dlq_topic).await;
    verify_dlq_topic_records(&base, &dlq_topic, &group_id, &expected_source_offsets(record_count), false).await;
    verify_dlq_metrics(&base, &group_id, record_count).await;
}

/// Produces 5 records and repeatedly releases them with a share consumer. Each release makes the records
/// available again and, on re-acquisition, increments their delivery count; once the delivery count limit
/// is exceeded the broker archives them and writes them to the DLQ (cause: delivery count exceeded). The
/// DLQ topic is created manually up front. Verifies the records reached the DLQ and the DLQ metrics fired.
#[tokio::test(flavor = "multi_thread")]
async fn test_released_records_exceeding_delivery_count_written_to_dlq() {
    if skip_below(SHARE_DLQ_RELEASE, "testReleasedRecordsExceedingDeliveryCountWrittenToDlq") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-release-group");
    let dlq_topic = dlq_topic_name(&mut base, "release");
    let record_count = 5;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    // Keep the delivery count limit low so a couple of releases exhaust it and trigger the DLQ.
    base.alter_share_delivery_count_limit(&group_id, "2").await;
    // Create the DLQ topic with DLQ enabled, and point the share group at it.
    create_dlq_topic(&base, &dlq_topic).await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;

    base.produce_messages(record_count).await;

    // Repeatedly release the records until their delivery count is exceeded and they are written to the DLQ.
    release_records_until_dlq(&base, &group_id, record_count).await;

    verify_dlq_topic_records(&base, &dlq_topic, &group_id, &expected_source_offsets(record_count), false).await;
    verify_dlq_metrics(&base, &group_id, record_count).await;
}

/// Produces records and applies a mix of acknowledgement types so the DLQ is reached via more than one path:
/// some records are rejected (DLQ via client reject), some are released on every delivery so they reach the
/// DLQ once their delivery count is exceeded (release-based DLQ), and the rest are accepted (never DLQ'd).
/// The actions are interleaved by offset so the DLQ'd offsets are non-contiguous and produce multiple
/// separate DLQ writes to the same DLQ partition. The DLQ topic is created manually up front.
#[tokio::test(flavor = "multi_thread")]
async fn test_mixed_acknowledgement_types_written_to_dlq() {
    if skip_below(SHARE_DLQ_RELEASE, "testMixedAcknowledgementTypesWrittenToDlq") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-mixed-group");
    let dlq_topic = dlq_topic_name(&mut base, "mixed");
    let record_count = 6;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    // Low delivery count limit so the released records reach the DLQ (delivery count exceeded) quickly.
    base.alter_share_delivery_count_limit(&group_id, "2").await;
    create_dlq_topic(&base, &dlq_topic).await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;

    base.produce_messages(record_count).await;

    // Assign each source offset an action, interleaved so the DLQ'd offsets are non-contiguous:
    //   offset % 3 == 0 -> reject  (DLQ via client reject)
    //   offset % 3 == 1 -> release (DLQ via delivery count exceeded - released on every delivery)
    //   offset % 3 == 2 -> accept  (never DLQ'd)
    let mut reject_offsets = HashSet::new();
    let mut release_offsets = HashSet::new();
    for offset in 0..record_count as i64 {
        if offset % 3 == 0 {
            reject_offsets.insert(offset);
        } else if offset % 3 == 1 {
            release_offsets.insert(offset);
        }
    }
    let expected_dlq_offsets: HashSet<i64> = reject_offsets.union(&release_offsets).copied().collect();

    let mut share_consumer = base.create_share_consumer_with_properties(&group_id, &explicit_mode());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Keep polling/acknowledging until every rejected and released record has reached the DLQ. Released
    // records are released on every delivery, so they are redelivered until their delivery count is
    // exceeded and they are archived to the DLQ.
    let deadline = Instant::now() + Duration::from_millis(DEFAULT_MAX_WAIT_MS);
    while dlq_meter_count(&base, METRIC_DLQ_RECORD_COUNT, &group_id).await < expected_dlq_offsets.len() as i64
        && Instant::now() < deadline
    {
        let records = share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
        for record in &records {
            let offset = record.offset();
            let acknowledge_type = if reject_offsets.contains(&offset) {
                AcknowledgeType::Reject
            } else if release_offsets.contains(&offset) {
                AcknowledgeType::Release
            } else {
                AcknowledgeType::Accept
            };
            share_consumer
                .acknowledge_with_record_type(record, acknowledge_type)
                .expect("acknowledge");
        }
        if records.count() > 0 {
            share_consumer
                .commit_sync_with_timeout(Duration::from_millis(10_000))
                .await
                .expect("commitSync");
        }
    }
    close_share_consumer(share_consumer).await;

    // Exactly the rejected and released records should be on the DLQ; accepted records must not be.
    verify_dlq_topic_records(&base, &dlq_topic, &group_id, &expected_dlq_offsets, false).await;
    verify_dlq_metrics(&base, &group_id, expected_dlq_offsets.len()).await;
}

/// As `test_rejected_records_written_to_dlq_with_copy_record_disabled`, but with record copy enabled, so
/// each DLQ record must carry the original key and value (in addition to the context headers).
#[tokio::test(flavor = "multi_thread")]
async fn test_rejected_records_written_to_dlq_with_copy_record_enabled() {
    if skip_below(SHARE_DLQ_RELEASE, "testRejectedRecordsWrittenToDlqWithCopyRecordEnabled") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-copy-group");
    let dlq_topic = dlq_topic_name(&mut base, "copy");
    let record_count = 5;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    create_dlq_topic(&base, &dlq_topic).await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;
    // Enable record copy so the original key/value are written onto the DLQ record.
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true")
        .await;

    base.produce_messages(record_count).await;
    reject_records(&base, &group_id, record_count).await;

    verify_dlq_topic_records(&base, &dlq_topic, &group_id, &expected_source_offsets(record_count), true).await;
    verify_dlq_metrics(&base, &group_id, record_count).await;
}

/// Rejects records from a multi-partition source topic and verifies they are routed to the correct DLQ
/// partition. The destination partition is `sourcePartition % numDlqPartitions`; with a DLQ topic that
/// has as many partitions as the source, each source partition maps to the DLQ partition of the same index.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_records_routed_to_correct_dlq_partition() {
    if skip_below(SHARE_DLQ_RELEASE, "testDlqRecordsRoutedToCorrectDlqPartition") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-routing-group");
    let source_topic = base.topic("dlq-routing-source");
    let dlq_topic = dlq_topic_name(&mut base, "routing");
    let partitions = 2;
    let records_per_partition = 3;

    base.create_topic_with_partitions_replication_factor(&source_topic, partitions, 1)
        .await;
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    create_dlq_topic_with_partitions(&base, &dlq_topic, partitions).await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;

    // Produce the same number of records to each source partition.
    for source_partition in 0..partitions {
        produce_to(&base, &source_topic, source_partition, records_per_partition).await;
    }

    let total = partitions as usize * records_per_partition;
    reject_records_with_topic(&base, &group_id, &source_topic, total).await;
    verify_dlq_metrics(&base, &group_id, total).await;

    // Source partition p maps to DLQ partition (p % partitions) == p. Each DLQ partition should hold the
    // records from the matching source partition (offsets 0..recordsPerPartition-1).
    for source_partition in 0..partitions {
        let dlq_records = read_dlq_partition(&base, &dlq_topic, source_partition, records_per_partition).await;
        assert_eq!(
            records_per_partition,
            dlq_records.len(),
            "DLQ partition {source_partition} has an unexpected number of records"
        );
        let mut offsets = HashSet::new();
        for record in &dlq_records {
            assert_eq!(
                Some(group_id.as_str()),
                header_value(record, HEADER_DLQ_ERRORS_GROUP).as_deref()
            );
            assert_eq!(
                Some(source_topic.as_str()),
                header_value(record, HEADER_DLQ_ERRORS_TOPIC).as_deref()
            );
            assert_eq!(
                Some(source_partition.to_string()),
                header_value(record, HEADER_DLQ_ERRORS_PARTITION),
                "Records on DLQ partition {source_partition} must originate from source partition {source_partition}"
            );
            offsets.insert(source_offset(record));
        }
        assert_eq!(expected_source_offsets(records_per_partition), offsets);
    }
}

/// Rejects a larger batch of records and verifies they all reach the DLQ. Exercises the produce coalescing /
/// record-merge path at volume (many records DLQ'd to the same DLQ partition).
#[tokio::test(flavor = "multi_thread")]
async fn test_many_rejected_records_all_written_to_dlq() {
    if skip_below(SHARE_DLQ_RELEASE, "testManyRejectedRecordsAllWrittenToDlq") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-scale-group");
    let dlq_topic = dlq_topic_name(&mut base, "scale");
    let record_count = 500;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    create_dlq_topic(&base, &dlq_topic).await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;

    base.produce_messages(record_count).await;
    reject_records(&base, &group_id, record_count).await;

    verify_dlq_topic_records(&base, &dlq_topic, &group_id, &expected_source_offsets(record_count), false).await;
    verify_dlq_metrics(&base, &group_id, record_count).await;
}

/// Verifies that the DLQ topic's own `max.message.bytes` - not the source topic's - bounds each DLQ
/// produce request, and that this is honored dynamically when the config changes.
///
/// Phase 1: the source topic allows records up to `sourceMaxMessageBytes`, but the DLQ topic is
/// configured with a third of that (`dlqMaxMessageBytes`). Three records, each sized so that any two
/// of their (record-copy-enabled) DLQ copies together would exceed dlqMaxMessageBytes, are rejected together
/// in a single commit - so SharePartition issues one DLQ call spanning the whole offset range. The DLQ
/// produce path must then split that single call into multiple sequential produce requests (one record each)
/// rather than failing or dropping any record; this confirms all 3 records still land on the DLQ topic, and
/// that the DLQ produce-request count increased by at least 3 (one per chunk).
///
/// Phase 2: the DLQ topic's `max.message.bytes` is then raised via `IncrementalAlterConfigs` to comfortably
/// exceed what a fresh batch of 3 more (same-sized) records needs combined, and the reject scenario is
/// repeated with that new batch. The broker must pick up the raised limit dynamically, and the chunking logic
/// must not split unnecessarily: the produce-request count must increase by exactly 1 for the second batch.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_respects_dlq_topic_max_message_bytes_not_eq_to_source_topic() {
    if skip_below(SHARE_DLQ_RELEASE, "testDlqRespectsDlqTopicMaxMessageBytesNotEqToSourceTopic") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-maxbytes-group");
    let source_topic = base.topic("dlq-maxbytes-source");
    let dlq_topic = dlq_topic_name(&mut base, "maxbytes");
    let record_count = 3;
    let source_max_message_bytes = 300_000;
    let dlq_max_message_bytes = source_max_message_bytes / 3;
    // Leave headroom below dlqMaxMessageBytes for the DLQ context headers/record-batch framing overhead,
    // so a single record's DLQ copy cleanly fits under the limit but two together clearly don't.
    let payload_size = dlq_max_message_bytes - 2_000;

    create_topics(
        &base,
        vec![
            new_topic(
                &source_topic,
                1,
                &[(MAX_MESSAGE_BYTES_CONFIG, &source_max_message_bytes.to_string())],
            ),
            new_topic(
                &dlq_topic,
                1,
                &[
                    (ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true"),
                    (MAX_MESSAGE_BYTES_CONFIG, &dlq_max_message_bytes.to_string()),
                ],
            ),
        ],
    )
    .await;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;
    // Record copy must be enabled - otherwise DLQ records carry headers only (tiny) and would never
    // approach dlqMaxMessageBytes regardless of the source record size.
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true")
        .await;

    let payload = vec![0u8; payload_size];
    produce_payload(&base, &HashMap::new(), &source_topic, record_count, &payload).await;

    // Reject all 3 records together in one commit, so SharePartition issues a single DLQ call spanning
    // the whole offset range (a fresh, contiguous, single-fetch acquisition with no prior redeliveries
    // maps to one cached in-flight batch, so one client-side reject commit produces one DLQ call).
    reject_records_with_topic(&base, &group_id, &source_topic, record_count).await;

    // All 3 records must still reach the DLQ, split across multiple produce requests since no pairing
    // of their DLQ copies fits within dlqMaxMessageBytes. Verified inline (rather than via
    // verify_dlq_topic_records()) since that helper hardcodes checking the copied value against the fixed
    // "value" content produced by produceMessages()/produceTo(), not this test's large payload.
    let dlq_records = read_dlq_partition(&base, &dlq_topic, 0, record_count).await;
    assert_eq!(record_count, dlq_records.len(), "Unexpected number of records on the DLQ topic");
    let mut actual_source_offsets = HashSet::new();
    for record in &dlq_records {
        assert_eq!(Some(&payload), record.value(), "DLQ record value should be the copied payload");
        assert_eq!(
            Some(group_id.as_str()),
            header_value(record, HEADER_DLQ_ERRORS_GROUP).as_deref()
        );
        assert_eq!(
            Some(source_topic.as_str()),
            header_value(record, HEADER_DLQ_ERRORS_TOPIC).as_deref()
        );
        assert_eq!(Some("0"), header_value(record, HEADER_DLQ_ERRORS_PARTITION).as_deref());
        actual_source_offsets.insert(source_offset(record));
    }
    assert_eq!(
        expected_source_offsets(record_count),
        actual_source_offsets,
        "DLQ records should cover every expected source offset"
    );
    verify_dlq_metrics(&base, &group_id, record_count).await;

    // Concrete proof that splitting - not some other mechanism - is why all 3 records arrived: there is
    // exactly one logical DLQ call here (one contiguous reject batch, offsets 0-2), so any produce-request
    // count above 1 for this group can only come from the resumable-cursor chunking logic splitting that
    // one call into multiple sequential produce requests to stay within dlqMaxMessageBytes. >= rather
    // than == tolerates an occasional extra retry (e.g. a transient network blip) without being flaky.
    let produce_total = dlq_meter_count(&base, METRIC_DLQ_PRODUCE_TOTAL, &group_id).await;
    assert!(
        produce_total >= record_count as i64,
        "Expected at least {record_count} separate DLQ produce requests (one per chunk), was {produce_total}"
    );
    let produce_count_before_raise = dlq_meter_count(&base, METRIC_DLQ_PRODUCE_TOTAL, &group_id).await;

    // Now raise the DLQ topic's max.message.bytes well above what all 3 (record-copy-enabled) DLQ copies
    // need together, and repeat the same reject scenario with a fresh batch of 3 records. This confirms
    // two things at once: the limit is picked up dynamically, and the chunking logic doesn't split
    // unnecessarily when the budget is actually sufficient - the produce-request count must increase by
    // exactly 1 (one request for the whole new batch), not 3.
    let raised_dlq_max_message_bytes = (source_max_message_bytes * 2).to_string();
    let dlq_topic_resource = ConfigResource::new(config_resource::Type::Topic, dlq_topic.clone());
    let admin = base.create_admin_client();
    admin
        .incremental_alter_configs_with_options(
            &HashMap::from([(
                dlq_topic_resource.clone(),
                vec![AlterConfigOp::new(
                    ConfigEntry::new(MAX_MESSAGE_BYTES_CONFIG.to_string(), Some(raised_dlq_max_message_bytes.clone())),
                    OpType::Set,
                )],
            )]),
            AlterConfigsOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("alter max.message.bytes");
    let admin_ref = &admin;
    let resource_ref = &dlq_topic_resource;
    let raised_ref = raised_dlq_max_message_bytes.as_str();
    wait_for_condition_with_pause(
        || async move {
            let configs = admin_ref
                .describe_configs(std::slice::from_ref(resource_ref))
                .all()
                .get()
                .await
                .expect("describe");
            configs
                .get(resource_ref)
                .and_then(|config| config.get(MAX_MESSAGE_BYTES_CONFIG))
                .is_some_and(|entry| entry.value() == Some(raised_ref))
        },
        DEFAULT_MAX_WAIT_MS,
        100,
        "Raised max.message.bytes did not propagate on the DLQ topic",
    )
    .await;
    close_admin(admin).await;

    produce_payload(&base, &HashMap::new(), &source_topic, record_count, &payload).await;
    reject_records_with_topic(&base, &group_id, &source_topic, record_count).await;

    let second_batch_dlq_records = read_dlq_partition(&base, &dlq_topic, 0, record_count * 2).await;
    assert_eq!(
        record_count * 2,
        second_batch_dlq_records.len(),
        "Unexpected number of records on the DLQ topic after the second batch"
    );

    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        200,
        &format!(
            "Expected exactly 1 additional DLQ produce request for the second batch (budget no longer forces \
             chunking), count went from {produce_count_before_raise}"
        ),
    );
    loop {
        let count = dlq_meter_count(&base, METRIC_DLQ_PRODUCE_TOTAL, &group_id).await;
        if count == produce_count_before_raise + 1 {
            break;
        }
        eprintln!("DLQ produce requests: {count}, expected {}", produce_count_before_raise + 1);
        deadline.fail_or_pause().await;
    }
}

/// Guards against a decompression-bomb-shaped source record: a highly compressible record that is tiny
/// on the wire but expands to a much larger size once decompressed. The DLQ record fetcher bounds the
/// decompression budget it will spend copying a record by the DLQ topic's own `max.message.bytes`.
///
/// Phase 1: the DLQ topic is configured with a deliberately low `max.message.bytes`. Two records with a
/// highly compressible ~200KB payload (tiny once gzip-compressed) are rejected with record copy enabled. The
/// decompression budget is exhausted well before the payload can be fully decompressed, so the copy is skipped
/// - but the DLQ write itself still succeeds, headers-only: the DLQ metrics still fire and the records still
/// land on the DLQ topic, just without a key/value.
///
/// Phase 2: the group is pointed at a second DLQ topic whose `max.message.bytes` is comfortably above the
/// decompressed payload size, and a fresh batch of the same records is rejected. This time the copy succeeds
/// and the new DLQ records carry the original key/value.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_copy_record_skipped_when_decompressed_size_exceeds_dlq_max_message_bytes() {
    if skip_below(
        SHARE_DLQ_RELEASE,
        "testDlqCopyRecordSkippedWhenDecompressedSizeExceedsDlqMaxMessageBytes",
    ) {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-decompress-cap-group");
    let source_topic = base.topic("dlq-decompress-cap-source");
    let dlq_topic = dlq_topic_name(&mut base, "decompress-cap");
    let record_count = 2;
    let payload_size = 200_000;
    let low_dlq_max_message_bytes = 2_000;
    // Comfortably above the two records' combined decompressed payload (2 * payloadSize = 400,000 bytes)
    // plus batch/record framing overhead.
    let high_dlq_max_message_bytes = 500_000;

    create_topics(
        &base,
        vec![
            new_topic(&source_topic, 1, &[]),
            new_topic(
                &dlq_topic,
                1,
                &[
                    (ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true"),
                    (MAX_MESSAGE_BYTES_CONFIG, &low_dlq_max_message_bytes.to_string()),
                ],
            ),
        ],
    )
    .await;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true")
        .await;

    // Highly compressible payload: tiny on the wire (comfortably under any max.message.bytes involved),
    // but decompresses to `payloadSize` bytes - large enough to blow past lowDlqMaxMessageBytes once the
    // DLQ record fetcher tries to decompress it for copying.
    let payload = vec![b'a'; payload_size];
    let gzip = HashMap::from([("compression.type".to_string(), "gzip".to_string())]);
    produce_payload(&base, &gzip, &source_topic, record_count, &payload).await;
    reject_records_with_topic(&base, &group_id, &source_topic, record_count).await;

    // Phase 1: DLQ write succeeds (headers-only) despite the low limit - copy is skipped, not the write.
    wait_for_dlq_record_count(&base, &group_id, record_count as i64).await;
    let dlq_records = read_dlq_partition(&base, &dlq_topic, 0, record_count).await;
    assert_eq!(record_count, dlq_records.len(), "Unexpected number of records on the DLQ topic");
    for record in &dlq_records {
        assert_eq!(
            None,
            record.key(),
            "DLQ record key should be absent - record copy must have been skipped"
        );
        assert_eq!(
            None,
            record.value(),
            "DLQ record value should be absent - record copy must have been skipped"
        );
        assert_eq!(
            Some(group_id.as_str()),
            header_value(record, HEADER_DLQ_ERRORS_GROUP).as_deref()
        );
        assert_eq!(
            Some(source_topic.as_str()),
            header_value(record, HEADER_DLQ_ERRORS_TOPIC).as_deref()
        );
    }

    // Phase 2: point the group at a second, freshly-created DLQ topic whose max.message.bytes is set
    // comfortably above the decompressed payload size up front - simpler than altering the first
    // topic's config and waiting for it to propagate.
    let dlq_topic2 = dlq_topic_name(&mut base, "decompress-cap-2");
    create_topics(
        &base,
        vec![new_topic(
            &dlq_topic2,
            1,
            &[
                (ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true"),
                (MAX_MESSAGE_BYTES_CONFIG, &high_dlq_max_message_bytes.to_string()),
            ],
        )],
    )
    .await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic2)
        .await;

    produce_payload(&base, &gzip, &source_topic, record_count, &payload).await;
    reject_records_with_topic(&base, &group_id, &source_topic, record_count).await;

    // The budget is now sufficient, so the second batch's DLQ records (on the new topic) must carry the
    // copied payload. The per-group metric accumulates across both DLQ topics used by this group.
    wait_for_dlq_record_count(&base, &group_id, record_count as i64 * 2).await;
    let second_batch_dlq_records = read_dlq_partition(&base, &dlq_topic2, 0, record_count).await;
    assert_eq!(
        record_count,
        second_batch_dlq_records.len(),
        "Unexpected number of records on the second DLQ topic"
    );
    for record in &second_batch_dlq_records {
        assert_eq!(Some(&payload), record.value(), "DLQ record value should be the copied payload");
        assert_eq!(
            Some(group_id.as_str()),
            header_value(record, HEADER_DLQ_ERRORS_GROUP).as_deref()
        );
        assert_eq!(
            Some(source_topic.as_str()),
            header_value(record, HEADER_DLQ_ERRORS_TOPIC).as_deref()
        );
    }
}

/// Verifies the DLQ is NOT written when it is gated off, across the gating conditions:
///   (a) no DLQ topic configured for the group;
///   (b) the configured DLQ topic exists but is not DLQ-enabled;
///   (c) the configured DLQ topic does not exist and auto creation is disabled (the cluster default);
///   (d) the configured DLQ topic name does not match the broker's DLQ topic name prefix ("dlq.").
/// In every case the rejected records are archived (the start offset advances) but no DLQ record is written.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_not_triggered_when_gated_off() {
    if skip_below(SHARE_DLQ_RELEASE, "testDlqNotTriggeredWhenGatedOff") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let record_count = 3;

    // (a) No DLQ topic configured for the group.
    let source_a = base.topic("dlq-gate-source-a");
    assert_no_dlq_written(
        &base,
        &base.group("dlq-gate-noname"),
        &source_a,
        record_count,
        async |_group: &str| {},
    )
    .await;

    // (b) DLQ topic exists but is not DLQ-enabled.
    let source_b = base.topic("dlq-gate-source-b");
    let disabled = dlq_topic_name(&mut base, "disabled");
    assert_no_dlq_written(
        &base,
        &base.group("dlq-gate-disabled"),
        &source_b,
        record_count,
        async |group: &str| {
            base.create_topic(&disabled).await;
            base.alter_share_group_config(group, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &disabled)
                .await;
        },
    )
    .await;

    // (c) DLQ topic missing and auto creation disabled (broker default).
    let source_c = base.topic("dlq-gate-source-c");
    let missing = dlq_topic_name(&mut base, "missing");
    assert_no_dlq_written(
        &base,
        &base.group("dlq-gate-missing"),
        &source_c,
        record_count,
        async |group: &str| {
            base.alter_share_group_config(group, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &missing)
                .await;
        },
    )
    .await;

    // (d) DLQ topic is DLQ-enabled but its name violates the "dlq." prefix.
    let source_d = base.topic("dlq-gate-source-d");
    let wrong_prefix = format!("wrong.{}", base.topic("prefix"));
    assert_no_dlq_written(
        &base,
        &base.group("dlq-gate-prefix"),
        &source_d,
        record_count,
        async |group: &str| {
            create_dlq_topic(&base, &wrong_prefix).await;
            base.alter_share_group_config(group, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &wrong_prefix)
                .await;
        },
    )
    .await;
}

/// A single consumer rejects records while the DLQ is enabled (they are written to the DLQ), then the DLQ is
/// turned off for the group by clearing its DLQ topic name, and the same consumer keeps rejecting more
/// records. Verifies via the DLQ record-count metric that no further DLQ records are written after the DLQ
/// is disabled.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_stops_after_disabling_for_group() {
    if skip_below(SHARE_DLQ_RELEASE, "testDlqStopsAfterDisablingForGroup") {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(dlq_cluster_config()).await;
    let group_id = base.group("dlq-toggle-group");
    let dlq_topic = dlq_topic_name(&mut base, "toggle");
    let first_batch = 5;
    let second_batch = 5;

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    create_dlq_topic(&base, &dlq_topic).await;
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, &dlq_topic)
        .await;

    let mut share_consumer = base.create_share_consumer_with_properties(&group_id, &explicit_mode());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // Phase 1: DLQ enabled - reject the first batch; they are written to the DLQ.
    base.produce_messages(first_batch).await;
    reject_records_with_consumer(&mut share_consumer, first_batch).await;
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 200, "First batch not written to DLQ");
    loop {
        let count = dlq_meter_count(&base, METRIC_DLQ_RECORD_COUNT, &group_id).await;
        if count == first_batch as i64 {
            break;
        }
        eprintln!("First batch not written to DLQ, count was {count}");
        deadline.fail_or_pause().await;
    }

    // Turn DLQ off for the group by clearing its DLQ topic name.
    base.alter_share_group_config(&group_id, ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "")
        .await;

    // Phase 2: DLQ disabled - the same consumer rejects more records; none should be DLQ'd.
    base.produce_messages(second_batch).await;
    reject_records_with_consumer(&mut share_consumer, second_batch).await;

    // The second-batch records are archived (terminal) and not redelivered; confirming this also gives
    // any (erroneous) DLQ write a chance to land before we assert.
    for _ in 0..3 {
        assert_eq!(
            0,
            share_consumer.poll(Duration::from_millis(2000)).await.expect("poll").count(),
            "Archived records must not be redelivered"
        );
    }
    close_share_consumer(share_consumer).await;

    // Only the first batch (written while DLQ was enabled) should be on the DLQ.
    assert_eq!(
        first_batch as i64,
        dlq_meter_count(&base, METRIC_DLQ_RECORD_COUNT, &group_id).await,
        "No DLQ records should be written after the DLQ is disabled for the group"
    );
}

// ---------------------------------------------------------------------------
// Helpers (the private methods of ShareConsumerDLQTest)
// ---------------------------------------------------------------------------

/// Java's `rejectRecords(String, int)`: consumes from the base source topic in EXPLICIT acknowledgement mode
/// and rejects `record_count` records.
async fn reject_records(base: &ShareConsumerTestBase, group_id: &str, record_count: usize) {
    reject_records_with_topic(base, group_id, base.tp.topic(), record_count).await;
}

/// Java's `rejectRecords(String, String, int)`: consumes from the given source topic in EXPLICIT
/// acknowledgement mode and rejects `record_count` records.
async fn reject_records_with_topic(base: &ShareConsumerTestBase, group_id: &str, topic: &str, record_count: usize) {
    let mut share_consumer = base.create_share_consumer_with_properties(group_id, &explicit_mode());
    share_consumer.subscribe(vec![topic.to_string()]).await.expect("subscribe");
    reject_records_with_consumer(&mut share_consumer, record_count).await;
    close_share_consumer(share_consumer).await;
}

/// Java's `rejectRecords(ShareConsumer, int)`: polls the given (already subscribed) consumer and rejects
/// records until `count` have been rejected.
async fn reject_records_with_consumer(share_consumer: &mut BytesShareConsumer, count: usize) {
    let mut rejected = 0;
    let deadline = Instant::now() + Duration::from_millis(DEFAULT_MAX_WAIT_MS);
    while rejected < count && Instant::now() < deadline {
        let records = share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
        for record in &records {
            share_consumer
                .acknowledge_with_record_type(record, AcknowledgeType::Reject)
                .expect("acknowledge");
            rejected += 1;
        }
        if records.count() > 0 {
            share_consumer
                .commit_sync_with_timeout(Duration::from_millis(10_000))
                .await
                .expect("commitSync");
        }
    }
    assert_eq!(count, rejected, "Expected to reject the requested number of records");
}

/// Java's `releaseRecordsUntilDlq`: repeatedly polls and releases every record until the delivery count
/// limit is exceeded for all of them and they have been written to the DLQ (tracked via the DLQ
/// record-count metric).
async fn release_records_until_dlq(base: &ShareConsumerTestBase, group_id: &str, record_count: usize) {
    let mut share_consumer = base.create_share_consumer_with_properties(group_id, &explicit_mode());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let deadline = Instant::now() + Duration::from_millis(DEFAULT_MAX_WAIT_MS);
    while dlq_meter_count(base, METRIC_DLQ_RECORD_COUNT, group_id).await < record_count as i64
        && Instant::now() < deadline
    {
        let records = share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
        for record in &records {
            share_consumer
                .acknowledge_with_record_type(record, AcknowledgeType::Release)
                .expect("acknowledge");
        }
        if records.count() > 0 {
            share_consumer
                .commit_sync_with_timeout(Duration::from_millis(10_000))
                .await
                .expect("commitSync");
        }
    }
    close_share_consumer(share_consumer).await;
}

/// Java's `assertNoDlqWritten`: sets up a gating condition for a fresh group/source topic, rejects records,
/// and asserts that although the records are archived no DLQ record is ever written for the group.
async fn assert_no_dlq_written(
    base: &ShareConsumerTestBase,
    group_id: &str,
    source_topic: &str,
    record_count: usize,
    gate_setup: impl AsyncFnOnce(&str),
) {
    base.create_topic(source_topic).await;
    base.alter_share_auto_offset_reset(group_id, "earliest").await;
    gate_setup(group_id).await;

    produce_to(base, source_topic, 0, record_count).await;
    reject_records_with_topic(base, group_id, source_topic, record_count).await;

    // The rejected records are archived (terminal) so they are never redelivered. Confirming this also
    // gives any DLQ attempt time to run (and, for the gated cases, to fail/short-circuit).
    let mut share_consumer = base.create_share_consumer_with_properties(group_id, &explicit_mode());
    share_consumer
        .subscribe(vec![source_topic.to_string()])
        .await
        .expect("subscribe");
    for _ in 0..3 {
        assert_eq!(
            0,
            share_consumer.poll(Duration::from_millis(2000)).await.expect("poll").count(),
            "Archived records must not be redelivered for group: {group_id}"
        );
    }
    close_share_consumer(share_consumer).await;
    // No DLQ record was written for this group (the per-group meter is never even registered).
    assert_eq!(
        -1,
        dlq_meter_count(base, METRIC_DLQ_RECORD_COUNT, group_id).await,
        "DLQ should not have been written for a gated-off group: {group_id}"
    );
}

/// Java's `expectedSourceOffsets`.
fn expected_source_offsets(record_count: usize) -> HashSet<i64> {
    (0..record_count as i64).collect()
}

/// Java's `verifyDlqMetrics`: asserts that every record was written to the DLQ and that at least one DLQ
/// produce request was enqueued.
async fn verify_dlq_metrics(base: &ShareConsumerTestBase, group_id: &str, expected_record_count: usize) {
    let expected = expected_record_count as i64;
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        200,
        &format!("DeadLetterQueueRecordCount did not reach {expected}"),
    );
    loop {
        let count = dlq_meter_count(base, METRIC_DLQ_RECORD_COUNT, group_id).await;
        if count == expected {
            break;
        }
        eprintln!("DeadLetterQueueRecordCount did not reach {expected}, was {count}");
        deadline.fail_or_pause().await;
    }
    assert_eq!(expected, dlq_meter_count(base, METRIC_DLQ_RECORD_COUNT, group_id).await);
    assert!(
        dlq_meter_count(base, METRIC_DLQ_PRODUCE_TOTAL, group_id).await >= 1,
        "Expected at least one DLQ produce request to have been enqueued"
    );
}

/// The `waitForCondition(() -> dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId) == expected,
/// DEFAULT_MAX_WAIT_MS, 200L, ...)` of `testDlqCopyRecordSkippedWhenDecompressedSizeExceedsDlqMaxMessageBytes`.
async fn wait_for_dlq_record_count(base: &ShareConsumerTestBase, group_id: &str, expected: i64) {
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 200, &format!("Expected {expected} DLQ records"));
    loop {
        let count = dlq_meter_count(base, METRIC_DLQ_RECORD_COUNT, group_id).await;
        if count == expected {
            break;
        }
        eprintln!("Expected {expected} DLQ records, was {count}");
        deadline.fail_or_pause().await;
    }
}

/// Java's `verifyDlqTopicCreated`: verifies the DLQ topic was auto-created by the broker and has the
/// DLQ-enable topic config set.
async fn verify_dlq_topic_created(base: &ShareConsumerTestBase, dlq_topic: &str) {
    let admin = base.create_admin_client();
    let admin_ref = &admin;
    wait_for_condition_with_pause(
        || async move {
            admin_ref
                .list_topics()
                .names()
                .get()
                .await
                .expect("list topics")
                .contains(dlq_topic)
        },
        DEFAULT_MAX_WAIT_MS,
        500,
        &format!("DLQ topic {dlq_topic} was not auto-created"),
    )
    .await;

    let resource = ConfigResource::new(config_resource::Type::Topic, dlq_topic.to_string());
    let configs = admin
        .describe_configs(std::slice::from_ref(&resource))
        .all()
        .get()
        .await
        .expect("describe configs");
    let entry = configs
        .get(&resource)
        .and_then(|config| config.get(ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG));
    let entry = entry.expect("Auto-created DLQ topic is missing the DLQ-enable config");
    assert_eq!(Some("true"), entry.value(), "Auto-created DLQ topic should have DLQ enabled");
    close_admin(admin).await;
}

/// Java's `verifyDlqTopicRecords(String, String, Set, boolean)`: for the base `tp`.
async fn verify_dlq_topic_records(
    base: &ShareConsumerTestBase,
    dlq_topic: &str,
    group_id: &str,
    expected_source_offsets: &HashSet<i64>,
    copy_enabled: bool,
) {
    let source_topic = base.tp.topic().to_string();
    let source_partition = base.tp.partition();
    verify_dlq_topic_records_with_source(
        base,
        dlq_topic,
        group_id,
        &source_topic,
        source_partition,
        expected_source_offsets,
        copy_enabled,
    )
    .await;
}

/// Java's `verifyDlqTopicRecords(String, String, String, int, Set, boolean)`: reads the DLQ topic (single
/// partition) and asserts it received exactly one record per expected source offset, each carrying the
/// context headers for `source_topic` / `source_partition`. When `copy_enabled` the DLQ records must also
/// carry the original key/value; otherwise they carry headers only.
async fn verify_dlq_topic_records_with_source(
    base: &ShareConsumerTestBase,
    dlq_topic: &str,
    group_id: &str,
    source_topic: &str,
    source_partition: i32,
    expected_source_offsets: &HashSet<i64>,
    copy_enabled: bool,
) {
    let dlq_records = read_dlq_partition(base, dlq_topic, 0, expected_source_offsets.len()).await;

    assert_eq!(
        expected_source_offsets.len(),
        dlq_records.len(),
        "Unexpected number of records on the DLQ topic"
    );
    let mut actual_source_offsets = HashSet::new();
    for record in &dlq_records {
        if copy_enabled {
            // produceMessages()/produceTo() produce records with key "key" and value "value".
            assert_eq!(
                Some(b"key".as_slice()),
                record.key().map(Vec::as_slice),
                "DLQ record key should be copied when record copy is enabled"
            );
            assert_eq!(
                Some(b"value".as_slice()),
                record.value().map(Vec::as_slice),
                "DLQ record value should be copied when record copy is enabled"
            );
        } else {
            // Record copy is disabled, so only headers are written - the value (and key) are null.
            assert_eq!(
                None,
                record.value(),
                "DLQ record value should be null when record copy is disabled"
            );
        }
        assert_eq!(Some(group_id), header_value(record, HEADER_DLQ_ERRORS_GROUP).as_deref());
        assert_eq!(Some(source_topic), header_value(record, HEADER_DLQ_ERRORS_TOPIC).as_deref());
        assert_eq!(
            Some(source_partition.to_string()),
            header_value(record, HEADER_DLQ_ERRORS_PARTITION)
        );
        actual_source_offsets.insert(source_offset(record));
    }
    assert_eq!(
        expected_source_offsets, &actual_source_offsets,
        "DLQ records should cover every expected source offset"
    );
}

/// Java's `readDlqPartition`: reads at least `expected_count` records from a single DLQ topic-partition,
/// with `cluster.consumer()` (earliest, a random group).
async fn read_dlq_partition(
    base: &ShareConsumerTestBase,
    dlq_topic: &str,
    partition: i32,
    expected_count: usize,
) -> Vec<BytesRecord> {
    let dlq_tp = TopicPartition::new(dlq_topic.to_string(), partition);
    let mut props = HashMap::from([
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("group.id".to_string(), base.group(&format!("group_{}", random_string(5)))),
        ("group.protocol".to_string(), "consumer".to_string()),
    ]);
    base.ctx().configure(&mut props);
    let mut consumer = KafkaConsumer::<Vec<u8>, Vec<u8>>::new(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        ByteArrayDeserializer::new(),
        ByteArrayDeserializer::new(),
    )
    .expect("KafkaConsumer::new");
    consumer.assign(vec![dlq_tp.clone()]).await.expect("assign");
    consumer
        .seek_to_beginning(std::slice::from_ref(&dlq_tp))
        .await
        .expect("seek to beginning");
    let mut dlq_records = Vec::new();
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        500,
        &format!("{dlq_tp} did not receive {expected_count} records"),
    );
    loop {
        let records = consumer.poll(Duration::from_millis(1000)).await.expect("poll");
        dlq_records.extend(records.into_iter().filter(|record| record.partition() == partition));
        if dlq_records.len() >= expected_count {
            break;
        }
        eprintln!("{dlq_tp} did not receive {expected_count} records, got {}", dlq_records.len());
        deadline.fail_or_pause().await;
    }
    tokio::time::timeout(Duration::from_secs(60), consumer.close())
        .await
        .expect("consumer close did not return in time")
        .expect("consumer close");
    dlq_records
}

/// Java's `createDlqTopic(String)`.
async fn create_dlq_topic(base: &ShareConsumerTestBase, topic_name: &str) {
    create_dlq_topic_with_partitions(base, topic_name, 1).await;
}

/// Java's `createDlqTopic(String, int)`: a topic with `errors.deadletterqueue.group.enable=true`.
async fn create_dlq_topic_with_partitions(base: &ShareConsumerTestBase, topic_name: &str, num_partitions: i32) {
    create_topics(
        base,
        vec![new_topic(
            topic_name,
            num_partitions,
            &[(ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true")],
        )],
    )
    .await;
}

/// Java's `new NewTopic(name, numPartitions, (short) 1).configs(configs)`.
fn new_topic(name: &str, num_partitions: i32, configs: &[(&str, &str)]) -> NewTopic {
    let topic = NewTopic::with_num_partitions_replication_factor(name.to_string(), Some(num_partitions), Some(1));
    if configs.is_empty() {
        return topic;
    }
    topic.set_configs(Some(
        configs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<BTreeMap<_, _>>(),
    ))
}

/// Java's `admin.createTopics(topics).all().get()`.
async fn create_topics(base: &ShareConsumerTestBase, topics: Vec<NewTopic>) {
    let admin = base.create_admin_client();
    admin
        .create_topics(&topics)
        .all()
        .get()
        .await
        .unwrap_or_else(|e| panic!("Failed to create topics: {e}"));
    close_admin(admin).await;
}

/// Java's `produceTo`: produces `count` records (key "key", value "value") to a specific topic-partition.
async fn produce_to(base: &ShareConsumerTestBase, topic: &str, partition: i32, count: usize) {
    let producer = base.create_producer().await;
    for _ in 0..count {
        let record = producer_record(
            topic.to_string(),
            Some(partition),
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
        );
        send_record(&producer, record).await.expect("send");
    }
    producer.flush().await.expect("flush");
    close_producer(producer).await;
}

/// The inline producer blocks of the two max-message-bytes tests: `count` records with key "key" and
/// `payload` to partition 0 of `topic`, with the extra producer `config`.
async fn produce_payload(
    base: &ShareConsumerTestBase,
    config: &HashMap<String, String>,
    topic: &str,
    count: usize,
    payload: &[u8],
) {
    let producer = base.create_producer_with_config(config);
    for _ in 0..count {
        let record = producer_record(topic.to_string(), Some(0), Some(b"key".to_vec()), Some(payload.to_vec()));
        send_record(&producer, record).await.expect("send");
    }
    producer.flush().await.expect("flush");
    close_producer(producer).await;
}

/// Java's `headerValue`: the last header `key` as UTF-8, `None` when absent.
fn header_value(record: &BytesRecord, key: &str) -> Option<String> {
    record
        .headers()
        .last_header(key)
        .map(|header| String::from_utf8_lossy(header.value().unwrap_or_default()).into_owned())
}

/// `Long.parseLong(Objects.requireNonNull(headerValue(record, HEADER_DLQ_ERRORS_OFFSET)))`.
fn source_offset(record: &BytesRecord) -> i64 {
    header_value(record, HEADER_DLQ_ERRORS_OFFSET)
        .expect("the DLQ offset header")
        .parse()
        .expect("a numeric offset")
}

/// Java's `dlqMeterCount`: the count of the (per-group) `ShareGroupMetrics` meter `metric_name`, or `-1`
/// if it has not been registered yet (the meters are created lazily on the first DLQ write for a group).
async fn dlq_meter_count(base: &ShareConsumerTestBase, metric_name: &str, group_id: &str) -> i64 {
    broker_meter_count(
        base.ctx(),
        &format!("kafka.server:type=ShareGroupMetrics,name={metric_name},group={group_id}"),
    )
    .await
}
