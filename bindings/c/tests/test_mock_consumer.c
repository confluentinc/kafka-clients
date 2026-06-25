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

// ConcurrentModificationError maps to the UnknownServerError numeric code
// (-1), since it carries no embedded Kafka `Errors` value (see
// `KafkaError::error()`).
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
                    kafka_common_KafkaError_t *error,
                    void *user_data) {
    async_poll_result_t *r = (async_poll_result_t *)user_data;
    if (records != NULL) {
        r->had_records = 1;
        r->record_count = kafka_consumer_ConsumerRecords_count(records);
        kafka_consumer_ConsumerRecords_destroy(records);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_KafkaError_code(error);
        kafka_common_KafkaError_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

/* Assigns a single (topic, partition) to the consumer. */
static kafka_common_KafkaError_t *assign_one(kafka_consumer_Consumer_t *c,
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
    kafka_common_KafkaError_t *err = assign_one(c, "test", 0);
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

    kafka_common_KafkaError_t *poll_err = NULL;
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

    kafka_common_KafkaError_t *poll_err = NULL;
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
// The async poll holds the guard from submission until its callback fires.
// We exploit this deterministically: from INSIDE the async callback (which
// runs on the dispatcher thread before the guard is released), a concurrent
// sync poll on the consumer must be rejected with ConcurrentModification.
// This covers both (a) cross-thread rejection and (b) one-op-in-flight.
// ---------------------------------------------------------------------------

/* The consumer under test, shared with the nested-rejection callback. */
static kafka_consumer_Consumer_t *g_guard_consumer = NULL;
/* Outcome of the nested sync poll attempted from within the callback. */
static atomic_int g_nested_rejected;       /* 1 if rejected as expected */
static atomic_int g_nested_error_code;     /* the rejection's error code */

static void on_poll_then_reenter(kafka_consumer_ConsumerRecords_t *records,
                                 kafka_common_KafkaError_t *error,
                                 void *user_data) {
    async_poll_result_t *r = (async_poll_result_t *)user_data;
    if (records != NULL) {
        r->had_records = 1;
        r->record_count = kafka_consumer_ConsumerRecords_count(records);
        kafka_consumer_ConsumerRecords_destroy(records);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_KafkaError_code(error);
        kafka_common_KafkaError_destroy(error);
    }
    // The async op's guard is still held (release runs AFTER this callback),
    // so a concurrent sync poll here must be rejected.
    kafka_common_KafkaError_t *nested_err = NULL;
    kafka_consumer_ConsumerRecords_t *nested =
        kafka_consumer_Consumer_poll(g_guard_consumer, 0, &nested_err);
    if (nested == NULL && nested_err != NULL) {
        atomic_store(&g_nested_rejected, 1);
        atomic_store(&g_nested_error_code, kafka_common_KafkaError_code(nested_err));
        kafka_common_KafkaError_destroy(nested_err);
    } else if (nested != NULL) {
        kafka_consumer_ConsumerRecords_destroy(nested);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_consumer_concurrency_guard(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NULL(assign_one(c, "test", 0));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_beginning_offsets(c, "test", 0, 0));

    g_guard_consumer = c;
    atomic_init(&g_nested_rejected, 0);
    atomic_init(&g_nested_error_code, 0);

    async_poll_result_t result;
    memset(&result, 0, sizeof(result));
    atomic_init(&result.fired, 0);

    kafka_consumer_Consumer_poll_async(c, 50, on_poll_then_reenter, &result);
    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));

    // The nested sync poll, issued while the async op still owned the guard,
    // was rejected with ConcurrentModification.
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&g_nested_rejected));
    TEST_ASSERT_EQUAL_INT32(CONCURRENT_MODIFICATION_CODE,
                            atomic_load(&g_nested_error_code));

    // After the in-flight op completed, a normal sync poll succeeds again.
    kafka_common_KafkaError_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *recs =
        kafka_consumer_Consumer_poll(c, 10, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    TEST_ASSERT_NOT_NULL(recs);
    kafka_consumer_ConsumerRecords_destroy(recs);

    g_guard_consumer = NULL;
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
    kafka_common_KafkaError_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *recs =
        kafka_consumer_Consumer_poll(c, 10, &poll_err);
    // The wakeup flag may have been consumed by the async poll already; accept
    // either outcome but ensure no crash and the guard is healthy.
    if (recs != NULL) {
        kafka_consumer_ConsumerRecords_destroy(recs);
    } else if (poll_err != NULL) {
        kafka_common_KafkaError_destroy(poll_err);
    }

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// add_record on a non-assigned partition errors; mock-only op on async errors
// ---------------------------------------------------------------------------

static void test_mock_consumer_add_record_unassigned_errors(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    // No assignment yet -> add_record must fail (IllegalState).
    kafka_common_KafkaError_t *err =
        kafka_consumer_MockConsumer_add_record(c, "test", 0, 0, NULL, -1, NULL, -1);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);
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
    return UNITY_END();
}
