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
#include "test_support.h"

void setUp(void) {}
void tearDown(void) {}

// ---------------------------------------------------------------------------
// Async (callback-based) test helpers
//
// Async callbacks fire on the producer's dedicated dispatcher thread, so tests
// must synchronize on a flag rather than assume inline execution; `wait_for`
// (test_support.h) spins until the expected number of callbacks have fired.
// ---------------------------------------------------------------------------

/* Captures the result(s) delivered to an async record callback. */
typedef struct {
    atomic_int fired;       /* number of callback invocations */
    int64_t offset;
    int32_t partition;
    char topic[256];
    int had_metadata;
    int had_error;
    int32_t error_code;
    pthread_t thread_id;    /* dispatcher thread the last callback ran on */
} async_record_result_t;

/* Captures the result delivered to an async operation (flush/close) callback. */
typedef struct {
    atomic_int fired;
    int had_error;
    pthread_t thread_id;
} async_op_result_t;

/* Per-record completion callback. Takes ownership of the handles (sync-call
 * semantics) and frees them after reading. */
static void on_record(kafka_producer_RecordMetadata_t *metadata,
                      kafka_common_KafkaError_t *error,
                      void *user_data) {
    async_record_result_t *r = (async_record_result_t *)user_data;
    if (metadata != NULL) {
        r->had_metadata = 1;
        r->offset = kafka_producer_RecordMetadata_offset(metadata);
        r->partition = kafka_producer_RecordMetadata_partition(metadata);
        const char *t = kafka_producer_RecordMetadata_topic(metadata);
        if (t != NULL) {
            strncpy(r->topic, t, sizeof(r->topic) - 1);
        }
        kafka_producer_RecordMetadata_destroy(metadata);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_KafkaError_code(error);
        kafka_common_KafkaError_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

/* Operation (flush/close) completion callback. */
static void on_operation(kafka_common_KafkaError_t *error, void *user_data) {
    async_op_result_t *r = (async_op_result_t *)user_data;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

void test_create_and_destroy(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    TEST_ASSERT_NOT_NULL(producer);
    kafka_producer_Producer_destroy(producer);
}

void test_destroy_null(void) {
    kafka_producer_Producer_destroy(NULL);
}

// ---------------------------------------------------------------------------
// Send tests
// ---------------------------------------------------------------------------

void test_send_with_key_and_value(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "my-key";
    const uint8_t value[] = "my-value";

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "test-topic", 2, -1,
        key, (int32_t)sizeof(key) - 1,
        value, (int32_t)sizeof(value) - 1,
        &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    /* Get metadata */
    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);

    /* Verify getters */
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(2, kafka_producer_RecordMetadata_partition(metadata));

    const char *returned_topic = kafka_producer_RecordMetadata_topic(metadata);
    TEST_ASSERT_NOT_NULL(returned_topic);
    TEST_ASSERT_EQUAL_STRING("test-topic", returned_topic);

    /* MockProducer always returns NO_TIMESTAMP (-1) */
    TEST_ASSERT_EQUAL_INT64(-1, kafka_producer_RecordMetadata_timestamp(metadata));

    /* History */
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_null_key(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "value-only";

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_null_value(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "key-only";

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        key, (int32_t)sizeof(key) - 1,
        NULL, -1,
        &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Multiple sends (incrementing offsets)
// ---------------------------------------------------------------------------

void test_multiple_sends_incrementing_offsets(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    for (int i = 0; i < 3; i++) {
        kafka_common_KafkaError_t *err = NULL;
        kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
            producer, "topic", 0, -1,
            NULL, -1, NULL, -1, &err);
        TEST_ASSERT_NULL(err);

        err = NULL;
        kafka_producer_RecordMetadata_t *metadata =
            kafka_producer_FutureRecordMetadata_get(future, &err);
        TEST_ASSERT_NULL(err);
        TEST_ASSERT_EQUAL_INT64((int64_t)i, kafka_producer_RecordMetadata_offset(metadata));

        kafka_producer_RecordMetadata_destroy(metadata);
        kafka_producer_FutureRecordMetadata_destroy(future);
    }

    TEST_ASSERT_EQUAL_INT32(3, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Manual complete mode
// ---------------------------------------------------------------------------

void test_manual_complete(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_FALSE(kafka_producer_FutureRecordMetadata_is_done(future));

    /* Complete it */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(producer));
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    /* Get metadata and verify getters */
    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_manual_error(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);

    /* Complete with error (code 2 = CorruptMessage) */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_error_next(producer, 2, "test error"));

    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(metadata);

    /* Inspect error */
    TEST_ASSERT_EQUAL_INT32(2, kafka_common_KafkaError_code(err));

    const char *msg = kafka_common_KafkaError_message(err);
    TEST_ASSERT_NOT_NULL(msg);

    kafka_common_KafkaError_destroy(err);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_complete_next_no_pending(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_complete_next(producer));
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Batch send
// ---------------------------------------------------------------------------

void test_send_batch(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "key";
    const uint8_t value[] = "value";

    kafka_producer_ProducerRecord_t records[2] = {
        { "topic1", -1, -1, key, (int32_t)sizeof(key) - 1,
          value, (int32_t)sizeof(value) - 1 },
        { "topic2", 1, -1, NULL, -1, NULL, -1 },
    };

    kafka_producer_FutureRecordMetadata_t *futures[2] = { NULL, NULL };
    kafka_common_KafkaError_t *errors[2] = { NULL, NULL };

    int32_t sent = kafka_producer_Producer_send_batch(
        producer, records, 2, futures, errors);
    TEST_ASSERT_EQUAL_INT32(2, sent);

    for (int i = 0; i < 2; i++) {
        TEST_ASSERT_NULL(errors[i]);
        TEST_ASSERT_NOT_NULL(futures[i]);
        TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(futures[i]));
        kafka_producer_FutureRecordMetadata_destroy(futures[i]);
    }

    TEST_ASSERT_EQUAL_INT32(2, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

void test_send_batch_partial_failure(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    /* Record at index 1 has null topic -> error */
    kafka_producer_ProducerRecord_t records[3] = {
        { "topic1", -1, -1, NULL, -1, NULL, -1 },
        { NULL, -1, -1, NULL, -1, NULL, -1 },  /* null topic */
        { "topic3", -1, -1, NULL, -1, NULL, -1 },
    };

    kafka_producer_FutureRecordMetadata_t *futures[3] = { NULL, NULL, NULL };
    kafka_common_KafkaError_t *errors[3] = { NULL, NULL, NULL };

    int32_t sent = kafka_producer_Producer_send_batch(
        producer, records, 3, futures, errors);
    TEST_ASSERT_EQUAL_INT32(2, sent);

    /* Index 0: success */
    TEST_ASSERT_NOT_NULL(futures[0]);
    TEST_ASSERT_NULL(errors[0]);

    /* Index 1: error */
    TEST_ASSERT_NULL(futures[1]);
    TEST_ASSERT_NOT_NULL(errors[1]);

    /* Index 2: success */
    TEST_ASSERT_NOT_NULL(futures[2]);
    TEST_ASSERT_NULL(errors[2]);

    kafka_producer_FutureRecordMetadata_destroy(futures[0]);
    kafka_common_KafkaError_destroy(errors[1]);
    kafka_producer_FutureRecordMetadata_destroy(futures[2]);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Close then send
// ---------------------------------------------------------------------------

void test_close_then_send(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);

    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Async send-path correctness (B1/B2/B3)
//
// These exercise the FFI submission channel directly, independent of
// transactions: the delivery-callback obligation on a queued send's error, the
// flush/close ordering against the channel, and destroy racing an in-flight
// queued send.
// ---------------------------------------------------------------------------

void test_send_async_on_closed_producer_fires_callback(void) {
    /* B1: send_async queues the record; the submission task then calls the
     * producer's send, which returns Err (closed) WITHOUT firing the callback.
     * The task must fire it with the error rather than drop it — dropping it
     * would leak user_data and hang an app blocking on the callback. */
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    static const uint8_t value[] = "v";
    async_record_result_t result;
    memset(&result, 0, sizeof(result));
    err = NULL;
    kafka_producer_Producer_send_async(
        producer, "topic", -1, -1, NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    /* The submit itself succeeds (the record is queued); the failure surfaces
     * through the callback, not out_error. */
    TEST_ASSERT_NULL(err);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_EQUAL_INT32(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_error);
    TEST_ASSERT_FALSE(result.had_metadata);

    kafka_producer_Producer_destroy(producer);
}

void test_flush_drains_async_queued_send(void) {
    /* B2: a record queued by send_async must be handed to the producer before
     * flush returns. auto_complete=true so once handed over it completes at once;
     * without the drain, flush would return with history still 0. */
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    static const uint8_t value[] = "v";
    async_record_result_t result;
    memset(&result, 0, sizeof(result));

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_send_async(
        producer, "topic", -1, -1, NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_Producer_flush(producer, &err);
    TEST_ASSERT_NULL(err);
    /* The queued record has been produced by the time flush returns. */
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));
    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);

    kafka_producer_Producer_destroy(producer);
}

void test_close_drains_async_queued_send(void) {
    /* B2: close flushes by default, so a queued send must be produced, not lost
     * to a close/send race. */
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    static const uint8_t value[] = "v";
    async_record_result_t result;
    memset(&result, 0, sizeof(result));

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_send_async(
        producer, "topic", -1, -1, NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));
    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);

    kafka_producer_Producer_destroy(producer);
}

void test_send_async_then_destroy(void) {
    /* B3: destroy immediately after a queued send must not use freed memory.
     * auto_complete=false leaves the send pending at destroy, which is the
     * teardown window the fix addresses (runtime shut down before the producer is
     * dropped, Kafka variant boxed). The callback may or may not fire — the point
     * is no crash. Repeated to widen the race window; Miri would be needed to
     * prove the absence of UB fully, and none is configured in CI. */
    for (int i = 0; i < 50; i++) {
        kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
        static const uint8_t value[] = "v";
        async_record_result_t result;
        memset(&result, 0, sizeof(result));
        kafka_common_KafkaError_t *err = NULL;
        kafka_producer_Producer_send_async(
            producer, "topic", -1, -1, NULL, -1,
            value, (int32_t)sizeof(value) - 1,
            on_record, &result, &err);
        TEST_ASSERT_NULL(err);
        kafka_producer_Producer_destroy(producer);
    }
    TEST_PASS();
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

void test_flush(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_FALSE(kafka_producer_FutureRecordMetadata_is_done(future));

    err = NULL;
    kafka_producer_Producer_flush(producer, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Error inspection
// ---------------------------------------------------------------------------

void test_error_inspection(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);

    TEST_ASSERT_NOT_EQUAL(0, kafka_common_KafkaError_code(err));

    const char *msg = kafka_common_KafkaError_message(err);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_TRUE(strlen(msg) > 0);

    /* is_retriable and is_fatal should be callable */
    (void)kafka_common_KafkaError_is_retriable(err);
    (void)kafka_common_KafkaError_is_fatal(err);

    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);
}

void test_error_null_safety(void) {
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_KafkaError_code(NULL));
    TEST_ASSERT_NULL(kafka_common_KafkaError_message(NULL));
    TEST_ASSERT_FALSE(kafka_common_KafkaError_is_retriable(NULL));
    TEST_ASSERT_FALSE(kafka_common_KafkaError_is_fatal(NULL));
    kafka_common_KafkaError_destroy(NULL);  /* no-op */
}

// ---------------------------------------------------------------------------
// Mock-specific: clear and history
// ---------------------------------------------------------------------------

void test_mock_clear(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_MockProducer_clear(producer);
    TEST_ASSERT_EQUAL_INT32(0, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// RecordMetadata getters from batch send
// ---------------------------------------------------------------------------

void test_record_metadata_getters_from_batch(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "key";
    const uint8_t value[] = "value";

    kafka_producer_ProducerRecord_t records[1] = {
        { "getter-topic", 7, -1, key, (int32_t)sizeof(key) - 1,
          value, (int32_t)sizeof(value) - 1 },
    };

    kafka_producer_FutureRecordMetadata_t *futures[1] = { NULL };
    kafka_common_KafkaError_t *errors[1] = { NULL };

    int32_t sent = kafka_producer_Producer_send_batch(
        producer, records, 1, futures, errors);
    TEST_ASSERT_EQUAL_INT32(1, sent);
    TEST_ASSERT_NULL(errors[0]);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(futures[0], &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);

    /* Verify all getters */
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(7, kafka_producer_RecordMetadata_partition(metadata));

    const char *topic = kafka_producer_RecordMetadata_topic(metadata);
    TEST_ASSERT_NOT_NULL(topic);
    TEST_ASSERT_EQUAL_STRING("getter-topic", topic);

    TEST_ASSERT_EQUAL_INT64(-1, kafka_producer_RecordMetadata_timestamp(metadata));

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(futures[0]);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Async (callback-based) tests
// ---------------------------------------------------------------------------

void test_send_async_with_key_and_value(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "my-key";
    const uint8_t value[] = "my-value";

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_send_async(
        producer, "async-topic", 3, -1,
        key, (int32_t)sizeof(key) - 1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT64(0, result.offset);
    TEST_ASSERT_EQUAL_INT32(3, result.partition);
    TEST_ASSERT_EQUAL_STRING("async-topic", result.topic);

    kafka_producer_Producer_destroy(producer);
}

void test_send_async_null_key(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "value-only";

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_send_async(
        producer, "topic", -1, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_FALSE(result.had_error);

    kafka_producer_Producer_destroy(producer);
}

void test_send_async_validation_error(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "v";

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    /* NULL topic is a synchronous validation error: out_error is set and the
     * callback is NOT invoked. */
    kafka_producer_Producer_send_async(
        producer, NULL, -1, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NOT_NULL(err);

    /* Give any (erroneously dispatched) callback a chance to fire, then assert
     * none did. */
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);
}

void test_send_batch_async(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    const uint8_t v0[] = "v0";
    const uint8_t v1[] = "v1";
    const uint8_t v2[] = "v2";
    kafka_producer_ProducerRecord_t records[3] = {
        {"batch-topic", -1, -1, NULL, -1, v0, (int32_t)sizeof(v0) - 1},
        {"batch-topic", -1, -1, NULL, -1, v1, (int32_t)sizeof(v1) - 1},
        {"batch-topic", -1, -1, NULL, -1, v2, (int32_t)sizeof(v2) - 1},
    };

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *out_errors[3] = {NULL, NULL, NULL};
    int32_t accepted = kafka_producer_Producer_send_batch_async(
        producer, records, 3, on_record, &result, out_errors);
    TEST_ASSERT_EQUAL_INT32(3, accepted);
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_NULL(out_errors[i]);
    }

    /* The callback fires once per record. */
    TEST_ASSERT_TRUE(wait_for(&result.fired, 3));
    TEST_ASSERT_EQUAL_INT(3, atomic_load(&result.fired));

    kafka_producer_Producer_destroy(producer);
}

void test_future_get_async(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "fv";

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "fut-topic", 1, -1, NULL, -1, value, (int32_t)sizeof(value) - 1, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);

    async_record_result_t result = {0};
    kafka_producer_FutureRecordMetadata_get_async(future, on_record, &result);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT32(1, result.partition);
    TEST_ASSERT_EQUAL_STRING("fut-topic", result.topic);

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_flush_async(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    async_op_result_t result = {0};
    kafka_producer_Producer_flush_async(producer, on_operation, &result);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);

    kafka_producer_Producer_destroy(producer);
}

void test_close_async(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    async_op_result_t result = {0};
    kafka_producer_Producer_close_async(producer, on_operation, &result);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);

    kafka_producer_Producer_destroy(producer);
}

void test_async_callbacks_single_thread(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "v";

    /* Fire several async sends; all completions must run on the same dispatcher
     * thread. */
    async_record_result_t results[5];
    memset(results, 0, sizeof(results));
    for (int i = 0; i < 5; i++) {
        kafka_common_KafkaError_t *err = NULL;
        kafka_producer_Producer_send_async(
            producer, "thread-topic", -1, -1, NULL, -1,
            value, (int32_t)sizeof(value) - 1,
            on_record, &results[i], &err);
        TEST_ASSERT_NULL(err);
    }

    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(wait_for(&results[i].fired, 1));
    }
    for (int i = 1; i < 5; i++) {
        TEST_ASSERT_TRUE(pthread_equal(results[0].thread_id, results[i].thread_id));
    }

    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Transactions
//
// The transaction-control surface is exercised against the mock because the
// whole lifecycle then runs broker-free and deterministically. `history_count`
// is the transactional-isolation probe: MockProducer only moves a record into
// the sent history when the transaction commits, so a record sent inside an open
// transaction is invisible there until `commit_transaction` returns.
//
// The concurrency guard around these five functions needs a control call to be
// slow enough to overlap with a second one, which no mock call is; that test
// therefore lives in test_kafka_producer.c. See
// design/history/Milestone-11/producer-transactions-ffi-plan.md.
// ---------------------------------------------------------------------------

/* True if `err` is the transaction-control guard rejection rather than an ordinary
 * state error. Both share error code -1, so the message is the only separator —
 * which matters because a leaked transaction-control flag would turn every
 * expected-error assertion below into a silently passing guard rejection. */
static int is_txn_guard_error(kafka_common_KafkaError_t *err) {
    if (err == NULL) {
        return 0;
    }
    const char *msg = kafka_common_KafkaError_message(err);
    return msg != NULL && strstr(msg, "not safe for concurrent access") != NULL;
}

/* Asserts `err` is a real failure carrying `expected_fragment`, not the guard
 * rejection, then frees it. */
static void assert_error_message(kafka_common_KafkaError_t *err, const char *expected_fragment) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    const char *msg = kafka_common_KafkaError_message(err);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(msg, expected_fragment), msg);
    kafka_common_KafkaError_destroy(err);
}

/* Helper: sends one minimal record, asserting success, and frees the future. */
static void send_one(kafka_producer_Producer_t *producer, const char *topic) {
    const uint8_t value[] = "v";
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, topic, -1, -1, NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    kafka_producer_FutureRecordMetadata_destroy(future);
}

void test_transaction_commit_publishes_records(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));

    send_one(producer, "txn-topic");
    /* Uncommitted: not in the sent history yet. */
    TEST_ASSERT_EQUAL_INT32(0, kafka_producer_MockProducer_history_count(producer));

    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(producer));
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

void test_transaction_abort_discards_records(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));

    /* First transaction commits, so the history has a known non-zero baseline. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));
    send_one(producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(producer));
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    /* Second transaction aborts: its record never reaches the history. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));
    send_one(producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(producer));
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

void test_transaction_send_offsets(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    /* The group metadata comes from a consumer, as in Java's
     * producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata()). */
    kafka_consumer_Consumer_t *consumer = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(consumer);
    kafka_consumer_ConsumerGroupMetadata_t *group_metadata =
        kafka_consumer_Consumer_group_metadata(consumer);
    TEST_ASSERT_NOT_NULL(group_metadata);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));
    send_one(producer, "txn-topic");

    const char *topics[] = { "input-topic", "input-topic" };
    const int32_t partitions[] = { 0, 1 };
    const int64_t offsets[] = { 42, 7 };
    const int32_t leader_epochs[] = { 5, -1 }; /* -1 == no epoch */
    const char *metadata[] = { "committed-by-txn", NULL };

    TEST_ASSERT_FALSE(kafka_producer_MockProducer_sent_offsets(producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(
        producer, topics, partitions, offsets, leader_epochs, metadata, 2,
        group_metadata));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_sent_offsets(producer));

    /* Inside an open transaction a zero count is a no-op that succeeds. This is the
     * one state where the two backends agree — see
     * test_transaction_send_offsets_zero_count_outside_transaction. */
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(
        producer, NULL, NULL, NULL, NULL, NULL, 0, group_metadata));

    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(producer));
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    /* Read every field back. Without this the test passes even if the forwarding
     * transposes partitions/offsets, drops leader_epochs, or stages an empty map. */
    const char *group_id = kafka_consumer_ConsumerGroupMetadata_group_id(group_metadata);
    int64_t got_offset = -1;
    int32_t got_epoch = -99;
    char got_metadata[64];

    TEST_ASSERT_TRUE(kafka_producer_MockProducer_committed_offset(
        producer, group_id, "input-topic", 0,
        &got_offset, &got_epoch, got_metadata, (int32_t)sizeof(got_metadata)));
    TEST_ASSERT_EQUAL_INT64(42, got_offset);
    TEST_ASSERT_EQUAL_INT32(5, got_epoch);
    TEST_ASSERT_EQUAL_STRING("committed-by-txn", got_metadata);

    TEST_ASSERT_TRUE(kafka_producer_MockProducer_committed_offset(
        producer, group_id, "input-topic", 1,
        &got_offset, &got_epoch, got_metadata, (int32_t)sizeof(got_metadata)));
    TEST_ASSERT_EQUAL_INT64(7, got_offset);
    TEST_ASSERT_EQUAL_INT32(-1, got_epoch);        /* leader_epoch -1 == absent */
    TEST_ASSERT_EQUAL_STRING("", got_metadata);    /* null metadata entry == empty */

    /* A partition that was never staged is not reported. */
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_committed_offset(
        producer, group_id, "input-topic", 2, NULL, NULL, NULL, 0));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_committed_offset(
        producer, "other-group", "input-topic", 0, NULL, NULL, NULL, 0));

    /* The metadata handle is borrowed, not consumed: still ours to destroy. */
    kafka_consumer_ConsumerGroupMetadata_destroy(group_metadata);
    kafka_consumer_Consumer_destroy(consumer);
    kafka_producer_Producer_destroy(producer);
}

void test_transaction_send_offsets_zero_count_outside_transaction(void) {
    /* A zero count stages nothing, but whether it *succeeds* is backend-specific,
     * and both behaviours are faithful to their Java counterpart:
     *
     *   - MockProducer verifies transaction state before the empty-map check
     *     (Java MockProducer:186-193 then :194-196), so this errors.
     *   - KafkaProducer short-circuits the empty map before consulting transaction
     *     state (Java KafkaProducer:738), so the same call returns success there.
     *
     * The suite asserted only the inside-a-transaction case, the single state where
     * the two agree, so the divergence was invisible. This pins the mock half. */
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    kafka_consumer_Consumer_t *consumer = kafka_consumer_MockConsumer_new("earliest");
    kafka_consumer_ConsumerGroupMetadata_t *group_metadata =
        kafka_consumer_Consumer_group_metadata(consumer);

    /* Before init_transactions. */
    assert_error_message(
        kafka_producer_Producer_send_offsets_to_transaction(
            producer, NULL, NULL, NULL, NULL, NULL, 0, group_metadata),
        "hasn't been initialized for transactions");

    /* Initialized, but no transaction open. */
    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));
    assert_error_message(
        kafka_producer_Producer_send_offsets_to_transaction(
            producer, NULL, NULL, NULL, NULL, NULL, 0, group_metadata),
        "no open transaction");

    /* Inside a transaction it succeeds, and stages nothing. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(
        producer, NULL, NULL, NULL, NULL, NULL, 0, group_metadata));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_sent_offsets(producer));

    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(producer));
    kafka_consumer_ConsumerGroupMetadata_destroy(group_metadata);
    kafka_consumer_Consumer_destroy(consumer);
    kafka_producer_Producer_destroy(producer);
}

void test_transaction_send_offsets_rejection_releases_guard(void) {
    /* send_offsets_to_transaction validates inside the transaction-control guard,
     * so its early returns have to release the flag. If they did not, the producer
     * would be permanently wedged and every later control call would be rejected —
     * which is what the follow-up calls here detect. */
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    kafka_consumer_Consumer_t *consumer = kafka_consumer_MockConsumer_new("earliest");
    kafka_consumer_ConsumerGroupMetadata_t *group_metadata =
        kafka_consumer_Consumer_group_metadata(consumer);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));

    /* Rejected before the guard is taken: a null group_metadata is a pure argument
     * precondition, so it must not even reach the transaction-control flag. */
    assert_error_message(
        kafka_producer_Producer_send_offsets_to_transaction(
            producer, NULL, NULL, NULL, NULL, NULL, 0, NULL),
        "group_metadata must not be null");

    /* Rejected *inside* the guard: marshaling feeds the operation, so its failure
     * is the early return the flag has to survive. */
    const char *topics[] = { "input-topic" };
    const int32_t partitions[] = { 0 };
    const int64_t bad_offsets[] = { -5 };
    assert_error_message(
        kafka_producer_Producer_send_offsets_to_transaction(
            producer, topics, partitions, bad_offsets, NULL, NULL, 1, group_metadata),
        "Invalid negative offset");

    /* The flag must have been released by both, so ordinary use continues. */
    const int64_t good_offsets[] = { 11 };
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(
        producer, topics, partitions, good_offsets, NULL, NULL, 1, group_metadata));
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(producer));

    const char *group_id = kafka_consumer_ConsumerGroupMetadata_group_id(group_metadata);
    int64_t got_offset = -1;
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_committed_offset(
        producer, group_id, "input-topic", 0, &got_offset, NULL, NULL, 0));
    TEST_ASSERT_EQUAL_INT64(11, got_offset);

    kafka_consumer_ConsumerGroupMetadata_destroy(group_metadata);
    kafka_consumer_Consumer_destroy(consumer);
    kafka_producer_Producer_destroy(producer);
}

void test_transaction_commit_error_requires_abort(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));
    send_one(producer, "txn-topic");

    /* 120 == TRANSACTION_ABORTABLE: the commit fails and the caller must abort
     * rather than retry. Setup-only, so it is issued before the control call it
     * affects (see the hook's safety contract). */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_set_commit_transaction_error(
        producer, false, 120, NULL));

    /* Rejected inputs: 0 is Errors::None, and anything outside i16 range would
     * otherwise truncate to an unrelated code. */
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_set_commit_transaction_error(
        producer, false, 0, NULL));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_set_commit_transaction_error(
        producer, false, 65656, NULL)); /* == 120 truncated to i16 */

    kafka_common_KafkaError_t *err = kafka_producer_Producer_commit_transaction(producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(120, kafka_common_KafkaError_code(err));
    TEST_ASSERT_TRUE(kafka_common_KafkaError_txn_requires_abort(err));
    TEST_ASSERT_FALSE(kafka_common_KafkaError_is_fatal(err));
    kafka_common_KafkaError_destroy(err);

    /* -1 is UnknownServerError, a perfectly installable code — clearing is a
     * separate flag, not a reserved code. */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_set_commit_transaction_error(
        producer, false, -1, NULL));
    err = kafka_producer_Producer_commit_transaction(producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(-1, kafka_common_KafkaError_code(err));
    kafka_common_KafkaError_destroy(err);

    /* Clearing must actually remove the installed error: prove it against
     * commit_transaction itself, which is the call the hook affects. */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_set_commit_transaction_error(
        producer, true, 0, NULL)); /* clear */
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(producer));
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    /* And the transaction really was open until then, so a fresh one behaves. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));
    send_one(producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(producer));
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

void test_transaction_success_does_not_require_abort(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));

    /* Begin twice: the second call is an ordinary illegal-state failure, which
     * must NOT be reported as requiring an abort. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));
    kafka_common_KafkaError_t *err = kafka_producer_Producer_begin_transaction(producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_FALSE(kafka_common_KafkaError_txn_requires_abort(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_KafkaError_message(err), "Transaction already started"));
    kafka_common_KafkaError_destroy(err);

    /* Null handle is defined as false, like the other error accessors. */
    TEST_ASSERT_FALSE(kafka_common_KafkaError_txn_requires_abort(NULL));

    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(producer));
    kafka_producer_Producer_destroy(producer);
}

void test_transaction_requires_init_first(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    /* Every control method fails before init_transactions has run. */
    assert_error_message(kafka_producer_Producer_begin_transaction(producer),
                         "hasn't been initialized for transactions");
    assert_error_message(kafka_producer_Producer_commit_transaction(producer),
                         "hasn't been initialized for transactions");
    assert_error_message(kafka_producer_Producer_abort_transaction(producer),
                         "hasn't been initialized for transactions");

    /* init twice is also an error (already initialized). */
    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));
    assert_error_message(kafka_producer_Producer_init_transactions(producer),
                         "already been initialized");

    kafka_producer_Producer_destroy(producer);
}

void test_transaction_commit_flushes_pending_sends(void) {
    /* auto_complete=false leaves every send pending, so a transaction can hold
     * several unresolved sends at once. Neither the sends nor their futures are
     * covered by the transaction-control guard, and the commit is what resolves
     * them: Java's commitTransaction() flushes before committing. */
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(producer));

    const uint8_t value[] = "v";
    kafka_producer_FutureRecordMetadata_t *futures[3];
    for (int i = 0; i < 3; i++) {
        kafka_common_KafkaError_t *err = NULL;
        futures[i] = kafka_producer_Producer_send(
            producer, "txn-topic", -1, -1, NULL, -1,
            value, (int32_t)sizeof(value) - 1,
            &err);
        TEST_ASSERT_NULL(err);
        TEST_ASSERT_NOT_NULL(futures[i]);
        /* Still pending, and invisible in the sent history. */
        TEST_ASSERT_FALSE(kafka_producer_FutureRecordMetadata_is_done(futures[i]));
    }
    TEST_ASSERT_EQUAL_INT32(0, kafka_producer_MockProducer_history_count(producer));

    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(producer));

    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(futures[i]));
        kafka_common_KafkaError_t *err = NULL;
        kafka_producer_RecordMetadata_t *metadata =
            kafka_producer_FutureRecordMetadata_get(futures[i], &err);
        TEST_ASSERT_NULL(err);
        TEST_ASSERT_NOT_NULL(metadata);
        kafka_producer_RecordMetadata_destroy(metadata);
        kafka_producer_FutureRecordMetadata_destroy(futures[i]);
    }
    TEST_ASSERT_EQUAL_INT32(3, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

void test_transaction_null_producer(void) {
    /* Null handle: an error handle, never a crash (mirrors the other ops). */
    kafka_common_KafkaError_t *err = kafka_producer_Producer_init_transactions(NULL);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

    err = kafka_producer_Producer_begin_transaction(NULL);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

    err = kafka_producer_Producer_commit_transaction(NULL);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

    err = kafka_producer_Producer_abort_transaction(NULL);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

    /* A null group_metadata is rejected without touching the producer. */
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    err = kafka_producer_Producer_send_offsets_to_transaction(
        producer, NULL, NULL, NULL, NULL, NULL, 0, NULL);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);

    /* The mock hook is a no-op on a null handle, clearing included. */
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_set_commit_transaction_error(NULL, false, 120, NULL));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_set_commit_transaction_error(NULL, true, 0, NULL));
}

// ---------------------------------------------------------------------------
// Send with callback (future + callback, mirrors Java's send(record, Callback))
// ---------------------------------------------------------------------------

void test_send_with_callback_fires_metadata_on_complete_next(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
    const uint8_t key[] = "cb-key";
    const uint8_t value[] = "cb-value";

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send_with_callback(
        producer, "cb-topic", 4, -1,
        key, (int32_t)sizeof(key) - 1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);

    /* auto_complete=false: nothing has completed yet, so neither the future nor
     * the callback has a result. */
    TEST_ASSERT_FALSE(kafka_producer_FutureRecordMetadata_is_done(future));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(producer));

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT64(0, result.offset);
    TEST_ASSERT_EQUAL_INT32(4, result.partition);
    TEST_ASSERT_EQUAL_STRING("cb-topic", result.topic);

    /* The future reports the same outcome. */
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));
    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);
    TEST_ASSERT_EQUAL_INT64(result.offset, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(result.partition, kafka_producer_RecordMetadata_partition(metadata));
    TEST_ASSERT_EQUAL_STRING(result.topic, kafka_producer_RecordMetadata_topic(metadata));

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_with_callback_fires_error_on_error_next(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send_with_callback(
        producer, "cb-err-topic", -1, -1,
        NULL, -1, NULL, -1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    /* Complete with error (code 2 = CorruptMessage) */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_error_next(producer, 2, "test error"));

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    /* Java's error path still hands the callback a placeholder metadata with
     * -1 for every unknown field (MockProducer.Completion.complete, and the
     * Callback.onCompletion javadoc: "an empty metadata with -1 value for all
     * fields except for topicPartition ... if an error occurred"). */
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_EQUAL_INT64(-1, result.offset);
    TEST_ASSERT_EQUAL_STRING("cb-err-topic", result.topic);
    TEST_ASSERT_TRUE(result.had_error);
    TEST_ASSERT_EQUAL_INT32(2, result.error_code);

    /* The future reports the same failure. */
    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NULL(metadata);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(result.error_code, kafka_common_KafkaError_code(err));

    kafka_common_KafkaError_destroy(err);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_with_callback_future_and_callback_agree(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "agree";

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send_with_callback(
        producer, "agree-topic", 7, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);

    /* auto_complete=true: the record completes during the send. */
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT64(kafka_producer_RecordMetadata_offset(metadata), result.offset);
    TEST_ASSERT_EQUAL_INT32(kafka_producer_RecordMetadata_partition(metadata), result.partition);
    TEST_ASSERT_EQUAL_STRING(kafka_producer_RecordMetadata_topic(metadata), result.topic);
    TEST_ASSERT_EQUAL_INT32(7, result.partition);

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_with_callback_runs_on_dispatcher_thread(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "v";

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send_with_callback(
        producer, "cb-thread-topic", -1, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);

    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));
    /* The callback runs on the producer's dispatcher thread, never on the
     * caller's thread. */
    TEST_ASSERT_FALSE(pthread_equal(pthread_self(), result.thread_id));

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_with_callback_validation_error_no_callback(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "v";

    async_record_result_t result = {0};
    kafka_common_KafkaError_t *err = NULL;
    /* NULL topic is a synchronous validation error: no future is returned,
     * out_error is set and the callback is NOT invoked. */
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send_with_callback(
        producer, NULL, -1, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

    /* A non-null key pointer with a negative length is ignored (no key), but a
     * null key with a non-negative length is a validation error too. */
    err = NULL;
    future = kafka_producer_Producer_send_with_callback(
        producer, "topic", -1, -1,
        NULL, 4,
        value, (int32_t)sizeof(value) - 1,
        on_record, &result, &err);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

    /* Give any (erroneously dispatched) callback a chance to fire, then assert
     * none did. */
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    kafka_producer_Producer_destroy(producer);
}

int main(void) {
    UNITY_BEGIN();

    /* Lifecycle */
    RUN_TEST(test_create_and_destroy);
    RUN_TEST(test_destroy_null);

    /* Send */
    RUN_TEST(test_send_with_key_and_value);
    RUN_TEST(test_send_null_key);
    RUN_TEST(test_send_null_value);
    RUN_TEST(test_multiple_sends_incrementing_offsets);

    /* Manual complete */
    RUN_TEST(test_manual_complete);
    RUN_TEST(test_manual_error);
    RUN_TEST(test_complete_next_no_pending);

    /* Batch */
    RUN_TEST(test_send_batch);
    RUN_TEST(test_send_batch_partial_failure);

    /* Close */
    RUN_TEST(test_close_then_send);

    /* Async send-path correctness (B1/B2/B3) */
    RUN_TEST(test_send_async_on_closed_producer_fires_callback);
    RUN_TEST(test_flush_drains_async_queued_send);
    RUN_TEST(test_close_drains_async_queued_send);
    RUN_TEST(test_send_async_then_destroy);

    /* Flush */
    RUN_TEST(test_flush);

    /* Error */
    RUN_TEST(test_error_inspection);
    RUN_TEST(test_error_null_safety);

    /* Mock-specific */
    RUN_TEST(test_mock_clear);

    /* RecordMetadata getters */
    RUN_TEST(test_record_metadata_getters_from_batch);

    /* Async (callback-based) */
    RUN_TEST(test_send_async_with_key_and_value);
    RUN_TEST(test_send_async_null_key);
    RUN_TEST(test_send_async_validation_error);
    RUN_TEST(test_send_batch_async);
    RUN_TEST(test_future_get_async);
    RUN_TEST(test_flush_async);
    RUN_TEST(test_close_async);
    RUN_TEST(test_async_callbacks_single_thread);

    /* Transactions */
    RUN_TEST(test_transaction_commit_publishes_records);
    RUN_TEST(test_transaction_abort_discards_records);
    RUN_TEST(test_transaction_send_offsets);
    RUN_TEST(test_transaction_send_offsets_zero_count_outside_transaction);
    RUN_TEST(test_transaction_send_offsets_rejection_releases_guard);
    RUN_TEST(test_transaction_commit_error_requires_abort);
    RUN_TEST(test_transaction_success_does_not_require_abort);
    RUN_TEST(test_transaction_requires_init_first);
    RUN_TEST(test_transaction_commit_flushes_pending_sends);
    RUN_TEST(test_transaction_null_producer);

    /* Send with callback (future + callback) */
    RUN_TEST(test_send_with_callback_fires_metadata_on_complete_next);
    RUN_TEST(test_send_with_callback_fires_error_on_error_next);
    RUN_TEST(test_send_with_callback_future_and_callback_agree);
    RUN_TEST(test_send_with_callback_runs_on_dispatcher_thread);
    RUN_TEST(test_send_with_callback_validation_error_no_callback);

    return UNITY_END();
}
