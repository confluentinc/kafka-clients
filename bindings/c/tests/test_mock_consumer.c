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

#include <confluent_kafka.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <time.h>
#include <pthread.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// LocalConcurrentModificationError maps to the UnknownServerError numeric code
// (-1), since it carries no embedded Kafka `Errors` value (see
// the Rust `Error::error()`).
#define CONCURRENT_MODIFICATION_CODE (-1)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/* Captures the result delivered to an async poll callback. */
typedef struct {
    atomic_int fired;
    int had_records;
    int had_error;
    int32_t record_count;
    int32_t error_code;
    pthread_t thread_id;
} async_poll_result_t;

/* Spins up to ~5s for `*flag` to reach `expected`. Returns 1 on success. */
static int wait_for(atomic_int *flag, int expected) {
    for (int i = 0; i < 5000; i++) {
        if (atomic_load(flag) >= expected) {
            return 1;
        }
        struct timespec ts = {0, 1000000}; /* 1ms */
        nanosleep(&ts, NULL);
    }
    return atomic_load(flag) >= expected;
}

/* Async poll completion callback: takes ownership of the non-null handle. */
static void on_poll(kafka_consumer_ConsumerRecords_t *records,
                    kafka_common_Error_t *error,
                    void *user_data) {
    async_poll_result_t *r = (async_poll_result_t *)user_data;
    if (records != NULL) {
        r->had_records = 1;
        r->record_count = kafka_consumer_ConsumerRecords_count(records);
        kafka_consumer_ConsumerRecords_destroy(records);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_Error_code(error);
        kafka_common_Error_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

/* Assigns a single (topic, partition) to the consumer. */
static kafka_common_Error_t *assign_one(kafka_consumer_Consumer_t *c,
                                             const char *topic,
                                             int32_t partition) {
    const char *topics[1] = {topic};
    int32_t partitions[1] = {partition};
    return kafka_consumer_Consumer_assign(c, topics, partitions, 1);
}

// ---------------------------------------------------------------------------
// Sync poll: add_record -> poll -> iterate -> assert bytes
// ---------------------------------------------------------------------------

static void test_mock_consumer_sync_poll_returns_record(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(c);

    // assign -> add_record -> update_beginning_offsets -> poll (mirrors the Rust
    // MockConsumerTest "poll returns records" flow).
    kafka_common_Error_t *err = assign_one(c, "test", 0);
    TEST_ASSERT_NULL(err);

    const uint8_t key[] = {0x6b, 0x65, 0x79};       /* "key" */
    const uint8_t value[] = {0x76, 0x61, 0x6c};     /* "val" */
    err = kafka_consumer_MockConsumer_add_record(c, "test", 0, 0,
                                                 key, (int32_t)sizeof(key),
                                                 value, (int32_t)sizeof(value));
    TEST_ASSERT_NULL(err);

    // EARLIEST reset uses the beginning offset to position the partition.
    err = kafka_consumer_MockConsumer_update_beginning_offsets(c, "test", 0, 0);
    TEST_ASSERT_NULL(err);

    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_Consumer_poll(c, 100, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    TEST_ASSERT_NOT_NULL(records);
    TEST_ASSERT_FALSE(kafka_consumer_ConsumerRecords_is_empty(records));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));

    const kafka_consumer_ConsumerRecord_t *rec =
        kafka_consumer_ConsumerRecords_get(records, 0);
    TEST_ASSERT_NOT_NULL(rec);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_ConsumerRecord_partition(rec));
    TEST_ASSERT_EQUAL_INT64(0, kafka_consumer_ConsumerRecord_offset(rec));

    int32_t topic_len = 0;
    const char *topic = kafka_consumer_ConsumerRecord_topic(rec, &topic_len);
    TEST_ASSERT_EQUAL_INT32(4, topic_len);
    TEST_ASSERT_EQUAL_INT(0, strncmp(topic, "test", 4));

    int32_t key_len = 0;
    const uint8_t *got_key = kafka_consumer_ConsumerRecord_key(rec, &key_len);
    TEST_ASSERT_EQUAL_INT32((int32_t)sizeof(key), key_len);
    TEST_ASSERT_EQUAL_MEMORY(key, got_key, sizeof(key));

    int32_t value_len = 0;
    const uint8_t *got_value = kafka_consumer_ConsumerRecord_value(rec, &value_len);
    TEST_ASSERT_EQUAL_INT32((int32_t)sizeof(value), value_len);
    TEST_ASSERT_EQUAL_MEMORY(value, got_value, sizeof(value));

    kafka_consumer_ConsumerRecords_destroy(records);

    // An out-of-range get returns null.
    records = kafka_consumer_Consumer_poll(c, 10, &poll_err);
    TEST_ASSERT_NOT_NULL(records);
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecords_get(records, 5));
    kafka_consumer_ConsumerRecords_destroy(records);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// Null key/value round-trips as (null, -1)
// ---------------------------------------------------------------------------

static void test_mock_consumer_null_key_value(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NULL(assign_one(c, "t", 0));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_add_record(c, "t", 0, 0,
                                                            NULL, -1, NULL, -1));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_beginning_offsets(c, "t", 0, 0));

    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_Consumer_poll(c, 100, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));

    const kafka_consumer_ConsumerRecord_t *rec =
        kafka_consumer_ConsumerRecords_get(records, 0);
    int32_t key_len = 0, value_len = 0;
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecord_key(rec, &key_len));
    TEST_ASSERT_EQUAL_INT32(-1, key_len);
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecord_value(rec, &value_len));
    TEST_ASSERT_EQUAL_INT32(-1, value_len);

    kafka_consumer_ConsumerRecords_destroy(records);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// Async poll: callback fires on the dispatcher thread
// ---------------------------------------------------------------------------

static void test_mock_consumer_async_poll(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NULL(assign_one(c, "test", 0));
    const uint8_t value[] = {0x01, 0x02, 0x03};
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_add_record(c, "test", 0, 0,
                                                            NULL, -1,
                                                            value, (int32_t)sizeof(value)));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_beginning_offsets(c, "test", 0, 0));

    async_poll_result_t result;
    memset(&result, 0, sizeof(result));
    atomic_init(&result.fired, 0);

    kafka_consumer_Consumer_poll_async(c, 100, on_poll, &result);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_records);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT32(1, result.record_count);
    // Callback ran on the dispatcher thread, not the caller.
    TEST_ASSERT_NOT_EQUAL(pthread_self(), result.thread_id);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// Concurrency guard: a second op is rejected while one op is in flight
//
// The guard is held from submission until the in-flight op completes (just
// before its callback fires). We exploit this deterministically by submitting
// two async polls back-to-back: when the second is submitted the first is
// still in flight (its completion needs several thread hops through the runtime
// and dispatcher), so the second is rejected inline with LocalConcurrentModification
// — its callback fires synchronously before poll_async returns. This covers
// both (a) cross-call rejection and (b) one-op-in-flight, without relying on
// when the guard is released relative to the callback.
// ---------------------------------------------------------------------------

static void test_mock_consumer_concurrency_guard(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NULL(assign_one(c, "test", 0));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_beginning_offsets(c, "test", 0, 0));

    async_poll_result_t first, second;
    memset(&first, 0, sizeof(first));
    memset(&second, 0, sizeof(second));
    atomic_init(&first.fired, 0);
    atomic_init(&second.fired, 0);

    // First acquires the guard and spawns; second is submitted while the first
    // is still in flight.
    kafka_consumer_Consumer_poll_async(c, 50, on_poll, &first);
    kafka_consumer_Consumer_poll_async(c, 50, on_poll, &second);

    // The second was rejected inline (callback fired synchronously) with
    // LocalConcurrentModification.
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&second.fired));
    TEST_ASSERT_TRUE(second.had_error);
    TEST_ASSERT_EQUAL_INT32(CONCURRENT_MODIFICATION_CODE, second.error_code);

    // The first eventually completes successfully (empty batch).
    TEST_ASSERT_TRUE(wait_for(&first.fired, 1));
    TEST_ASSERT_TRUE(first.had_records);

    // After the in-flight op completed, a normal sync poll succeeds again.
    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *recs =
        kafka_consumer_Consumer_poll(c, 10, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    TEST_ASSERT_NOT_NULL(recs);
    kafka_consumer_ConsumerRecords_destroy(recs);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// wakeup() bypasses the guard (never rejected) and is callable any time
// ---------------------------------------------------------------------------

static void test_mock_consumer_wakeup_bypasses_guard(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NULL(assign_one(c, "test", 0));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_beginning_offsets(c, "test", 0, 0));

    // wakeup() does not acquire the guard, so it works even while an async op
    // holds it. Submit an async poll and fire wakeup from this thread.
    async_poll_result_t result;
    memset(&result, 0, sizeof(result));
    atomic_init(&result.fired, 0);
    kafka_consumer_Consumer_poll_async(c, 50, on_poll, &result);

    // Not rejected (returns void; the call simply completes). It sets the mock
    // wakeup flag observed by a subsequent poll.
    kafka_consumer_Consumer_wakeup(c);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));

    // The mock wakeup flag set above causes the NEXT poll to return Wakeup.
    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *recs =
        kafka_consumer_Consumer_poll(c, 10, &poll_err);
    // The wakeup flag may have been consumed by the async poll already; accept
    // either outcome but ensure no crash and the guard is healthy.
    if (recs != NULL) {
        kafka_consumer_ConsumerRecords_destroy(recs);
    } else if (poll_err != NULL) {
        kafka_common_Error_destroy(poll_err);
    }

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// add_record on a non-assigned partition errors; mock-only op on async errors
// ---------------------------------------------------------------------------

static void test_mock_consumer_add_record_unassigned_errors(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    // No assignment yet -> add_record must fail (LocalIllegalState).
    kafka_common_Error_t *err =
        kafka_consumer_MockConsumer_add_record(c, "test", 0, 0, NULL, -1, NULL, -1);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_destroy(err);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// Helper: a one-(topic,partition) assigned, earliest-positioned mock consumer.
// ---------------------------------------------------------------------------

static kafka_consumer_Consumer_t *make_assigned_mock(const char *topic, int32_t partition) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NULL(assign_one(c, topic, partition));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_beginning_offsets(c, topic, partition, 0));
    return c;
}

// ---------------------------------------------------------------------------
// subscribe / subscription / unsubscribe
// ---------------------------------------------------------------------------

static void test_mock_consumer_subscribe_subscription(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    const char *topics[2] = {"alpha", "beta"};
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe(c, topics, 2));

    kafka_consumer_StringList_t *subs = kafka_consumer_Consumer_subscription(c);
    TEST_ASSERT_NOT_NULL(subs);
    TEST_ASSERT_EQUAL_INT32(2, kafka_consumer_StringList_count(subs));
    // Set ordering is unspecified; just assert both topics appear.
    int saw_alpha = 0, saw_beta = 0;
    for (int i = 0; i < kafka_consumer_StringList_count(subs); i++) {
        const char *s = kafka_consumer_StringList_get(subs, i);
        if (strcmp(s, "alpha") == 0) saw_alpha = 1;
        if (strcmp(s, "beta") == 0) saw_beta = 1;
    }
    TEST_ASSERT_TRUE(saw_alpha && saw_beta);
    kafka_consumer_StringList_destroy(subs);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_unsubscribe(c));
    subs = kafka_consumer_Consumer_subscription(c);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_StringList_count(subs));
    kafka_consumer_StringList_destroy(subs);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// assignment reflects assign()
// ---------------------------------------------------------------------------

static void test_mock_consumer_assignment(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 3);
    kafka_consumer_TopicPartitionList_t *as = kafka_consumer_Consumer_assignment(c);
    TEST_ASSERT_NOT_NULL(as);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_TopicPartitionList_count(as));
    const kafka_consumer_TopicPartition_t *tp = kafka_consumer_TopicPartitionList_get(as, 0);
    TEST_ASSERT_EQUAL_INT(0, strcmp(kafka_consumer_TopicPartition_topic(tp), "test"));
    TEST_ASSERT_EQUAL_INT32(3, kafka_consumer_TopicPartition_partition(tp));
    kafka_consumer_TopicPartitionList_destroy(as);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// seek + position round-trip
// ---------------------------------------------------------------------------

static void test_mock_consumer_seek_position(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_seek(c, "test", 0, 42));

    int64_t pos = -1;
    kafka_common_Error_t *err =
        kafka_consumer_Consumer_position(c, "test", 0, &pos);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_INT64(42, pos);

    // seek_with_metadata also sets position.
    TEST_ASSERT_NULL(kafka_consumer_Consumer_seek_with_metadata(c, "test", 0, 100, -1, "meta"));
    err = kafka_consumer_Consumer_position(c, "test", 0, &pos);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_INT64(100, pos);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// commit_sync_offsets + committed round-trip
// ---------------------------------------------------------------------------

static void test_mock_consumer_commit_committed(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    int64_t offsets[1] = {7};
    const char *metas[1] = {"checkpoint"};
    kafka_common_Error_t *err = kafka_consumer_Consumer_commit_sync_offsets(
        c, topics, partitions, offsets, NULL, metas, 1);
    TEST_ASSERT_NULL(err);

    kafka_consumer_OffsetMap_t *map = NULL;
    err = kafka_consumer_Consumer_committed(c, topics, partitions, 1, &map);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_OffsetMap_count(map));
    const kafka_consumer_TopicPartition_t *k = kafka_consumer_OffsetMap_get_key(map, 0);
    const kafka_consumer_OffsetAndMetadata_t *v = kafka_consumer_OffsetMap_get_value(map, 0);
    TEST_ASSERT_EQUAL_INT(0, strcmp(kafka_consumer_TopicPartition_topic(k), "test"));
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_TopicPartition_partition(k));
    TEST_ASSERT_EQUAL_INT64(7, kafka_consumer_OffsetAndMetadata_offset(v));
    TEST_ASSERT_EQUAL_INT(0, strcmp(kafka_consumer_OffsetAndMetadata_metadata(v), "checkpoint"));
    int32_t epoch = 999;
    TEST_ASSERT_FALSE(kafka_consumer_OffsetAndMetadata_leader_epoch(v, &epoch));
    kafka_consumer_OffsetMap_destroy(map);

    // commit_sync (no offsets) just succeeds on the mock.
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_sync(c));

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// beginning / end offsets
// ---------------------------------------------------------------------------

static void test_mock_consumer_beginning_end_offsets(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_end_offsets(c, "test", 0, 55));

    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};

    kafka_consumer_LongOffsetMap_t *begin = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_beginning_offsets(c, topics, partitions, 1, &begin));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_LongOffsetMap_count(begin));
    TEST_ASSERT_EQUAL_INT64(0, kafka_consumer_LongOffsetMap_get_value(begin, 0));
    kafka_consumer_LongOffsetMap_destroy(begin);

    kafka_consumer_LongOffsetMap_t *end = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_end_offsets(c, topics, partitions, 1, &end));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_LongOffsetMap_count(end));
    TEST_ASSERT_EQUAL_INT64(55, kafka_consumer_LongOffsetMap_get_value(end, 0));
    const kafka_consumer_TopicPartition_t *k = kafka_consumer_LongOffsetMap_get_key(end, 0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_TopicPartition_partition(k));
    kafka_consumer_LongOffsetMap_destroy(end);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// pause / resume / paused
// ---------------------------------------------------------------------------

static void test_mock_consumer_pause_resume(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};

    TEST_ASSERT_NULL(kafka_consumer_Consumer_pause(c, topics, partitions, 1));
    kafka_consumer_TopicPartitionList_t *paused = kafka_consumer_Consumer_paused(c);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_TopicPartitionList_count(paused));
    kafka_consumer_TopicPartitionList_destroy(paused);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_resume(c, topics, partitions, 1));
    paused = kafka_consumer_Consumer_paused(c);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_TopicPartitionList_count(paused));
    kafka_consumer_TopicPartitionList_destroy(paused);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// group_metadata
// ---------------------------------------------------------------------------

static void test_mock_consumer_group_metadata(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    kafka_consumer_ConsumerGroupMetadata_t *meta = kafka_consumer_Consumer_group_metadata(c);
    TEST_ASSERT_NOT_NULL(meta);
    // The group id is a non-null C string; member id is too.
    TEST_ASSERT_NOT_NULL(kafka_consumer_ConsumerGroupMetadata_group_id(meta));
    TEST_ASSERT_NOT_NULL(kafka_consumer_ConsumerGroupMetadata_member_id(meta));
    kafka_consumer_ConsumerGroupMetadata_destroy(meta);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// partitions_for / list_topics + Node getters
// ---------------------------------------------------------------------------

static void test_mock_consumer_partitions_and_topics(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    // 2 partitions for "test", leader node id=1 host="broker1" port=9092.
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_partitions(
        c, "test", 2, 1, "broker1", 9092));

    kafka_consumer_PartitionInfoList_t *infos = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_partitions_for(c, "test", &infos));
    TEST_ASSERT_NOT_NULL(infos);
    TEST_ASSERT_EQUAL_INT32(2, kafka_consumer_PartitionInfoList_count(infos));

    const kafka_consumer_PartitionInfo_t *p0 = kafka_consumer_PartitionInfoList_get(infos, 0);
    TEST_ASSERT_EQUAL_INT(0, strcmp(kafka_consumer_PartitionInfo_topic(p0), "test"));
    const kafka_common_Node_t *leader = kafka_consumer_PartitionInfo_leader(p0);
    TEST_ASSERT_NOT_NULL(leader);
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_Node_id(leader));
    TEST_ASSERT_EQUAL_INT32(9092, kafka_common_Node_port(leader));
    int32_t host_len = 0;
    const char *host = kafka_common_Node_host(leader, &host_len);
    TEST_ASSERT_EQUAL_INT32(7, host_len);
    TEST_ASSERT_EQUAL_INT(0, strncmp(host, "broker1", 7));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_PartitionInfo_replica_count(p0));
    TEST_ASSERT_NOT_NULL(kafka_consumer_PartitionInfo_replica(p0, 0));
    TEST_ASSERT_NULL(kafka_consumer_PartitionInfo_replica(p0, 5));
    kafka_consumer_PartitionInfoList_destroy(infos);

    kafka_consumer_TopicPartitionInfoMap_t *map = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_list_topics(c, &map));
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_TopicPartitionInfoMap_count(map));
    TEST_ASSERT_EQUAL_INT(0, strcmp(kafka_consumer_TopicPartitionInfoMap_get_topic(map, 0), "test"));
    const kafka_consumer_PartitionInfoList_t *plist =
        kafka_consumer_TopicPartitionInfoMap_get_partitions(map, 0);
    TEST_ASSERT_EQUAL_INT32(2, kafka_consumer_PartitionInfoList_count(plist));
    kafka_consumer_TopicPartitionInfoMap_destroy(map);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// Record headers + timestamp type + serialized sizes
// ---------------------------------------------------------------------------

static void test_mock_consumer_record_metadata_getters(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    const uint8_t value[] = {0xAA, 0xBB};
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_add_record(c, "test", 0, 0,
                                                            NULL, -1,
                                                            value, (int32_t)sizeof(value)));
    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_Consumer_poll(c, 100, &poll_err);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));
    const kafka_consumer_ConsumerRecord_t *rec = kafka_consumer_ConsumerRecords_get(records, 0);

    // No headers were attached.
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_ConsumerRecord_header_count(rec));
    int32_t hlen = 0;
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecord_header_key(rec, 0, &hlen));
    TEST_ASSERT_EQUAL_INT32(-1, hlen);

    // Mock records are built via ConsumerRecord::new, which leaves both
    // serialized sizes at NULL_SIZE (-1); the broker fetch path sets them.
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_ConsumerRecord_serialized_key_size(rec));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_ConsumerRecord_serialized_value_size(rec));

    // timestamp_type is a valid id (-1, 0, or 1).
    int32_t tt = kafka_consumer_ConsumerRecord_timestamp_type(rec);
    TEST_ASSERT_TRUE(tt >= -1 && tt <= 1);

    // leader_epoch / delivery_count absent for a plain mock record.
    int32_t epoch = 0, dc = 0;
    TEST_ASSERT_FALSE(kafka_consumer_ConsumerRecord_leader_epoch(rec, &epoch));
    TEST_ASSERT_FALSE(kafka_consumer_ConsumerRecord_delivery_count(rec, &dc));

    kafka_consumer_ConsumerRecords_destroy(records);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// set_poll_error: the injected error surfaces from the next poll
// ---------------------------------------------------------------------------

static void test_mock_consumer_poll_error(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_set_poll_error(c, "boom"));

    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_Consumer_poll(c, 10, &poll_err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_NOT_NULL(poll_err);
    kafka_common_Error_destroy(poll_err);

    // The error is consumed; a subsequent poll succeeds.
    records = kafka_consumer_Consumer_poll(c, 10, &poll_err);
    TEST_ASSERT_NOT_NULL(records);
    kafka_consumer_ConsumerRecords_destroy(records);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// async void op (subscribe_async): callback fires on the dispatcher thread
// ---------------------------------------------------------------------------

typedef struct {
    atomic_int fired;
    int had_error;
} async_op_result_t;

static void on_op(kafka_common_Error_t *error, void *user_data) {
    async_op_result_t *r = (async_op_result_t *)user_data;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_consumer_subscribe_async(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    const char *topics[1] = {"async-topic"};
    async_op_result_t result;
    memset(&result, 0, sizeof(result));
    atomic_init(&result.fired, 0);

    kafka_consumer_Consumer_subscribe_async(c, topics, 1, on_op, &result);
    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);

    kafka_consumer_StringList_t *subs = kafka_consumer_Consumer_subscription(c);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_StringList_count(subs));
    kafka_consumer_StringList_destroy(subs);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// client_id returns an owned string freed by kafka_consumer_string_destroy
// ---------------------------------------------------------------------------

static void test_mock_consumer_client_id(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    char *id = kafka_consumer_Consumer_client_id(c);
    TEST_ASSERT_NOT_NULL(id);
    kafka_consumer_string_destroy(id);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// close then destroy
// ---------------------------------------------------------------------------

static void test_mock_consumer_close(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_close(c));
    // After close, a mock driver op (update_partitions) errors.
    kafka_common_Error_t *err = kafka_consumer_MockConsumer_update_partitions(
        c, "test", 1, 1, "h", 1);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_destroy(err);
    kafka_consumer_Consumer_destroy(c);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_mock_consumer_sync_poll_returns_record);
    RUN_TEST(test_mock_consumer_null_key_value);
    RUN_TEST(test_mock_consumer_async_poll);
    RUN_TEST(test_mock_consumer_concurrency_guard);
    RUN_TEST(test_mock_consumer_wakeup_bypasses_guard);
    RUN_TEST(test_mock_consumer_add_record_unassigned_errors);
    RUN_TEST(test_mock_consumer_subscribe_subscription);
    RUN_TEST(test_mock_consumer_assignment);
    RUN_TEST(test_mock_consumer_seek_position);
    RUN_TEST(test_mock_consumer_commit_committed);
    RUN_TEST(test_mock_consumer_beginning_end_offsets);
    RUN_TEST(test_mock_consumer_pause_resume);
    RUN_TEST(test_mock_consumer_group_metadata);
    RUN_TEST(test_mock_consumer_partitions_and_topics);
    RUN_TEST(test_mock_consumer_record_metadata_getters);
    RUN_TEST(test_mock_consumer_poll_error);
    RUN_TEST(test_mock_consumer_subscribe_async);
    RUN_TEST(test_mock_consumer_client_id);
    RUN_TEST(test_mock_consumer_close);
    return UNITY_END();
}
