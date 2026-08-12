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

// ---------------------------------------------------------------------------
// Async (callback-based) test helpers
//
// Async callbacks fire on the producer's dedicated dispatcher thread, so tests
// must synchronize on a flag rather than assume inline execution. `wait_for`
// spins (bounded) until the expected number of callbacks have fired.
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

/* Per-record completion callback. Takes ownership of the handles (sync-call
 * semantics) and frees them after reading. */
static void on_record(kafka_producer_RecordMetadata_t *metadata,
                      kafka_common_Error_t *error,
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
        r->error_code = kafka_common_Error_code(error);
        kafka_common_Error_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

/* Operation (flush/close) completion callback. */
static void on_operation(kafka_common_Error_t *error, void *user_data) {
    async_op_result_t *r = (async_op_result_t *)user_data;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
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

    kafka_common_Error_t *err = NULL;
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

    kafka_common_Error_t *err = NULL;
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

    kafka_common_Error_t *err = NULL;
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
        kafka_common_Error_t *err = NULL;
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

    kafka_common_Error_t *err = NULL;
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

    kafka_common_Error_t *err = NULL;
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
    TEST_ASSERT_EQUAL_INT32(2, kafka_common_Error_code(err));

    const char *msg = kafka_common_Error_message(err);
    TEST_ASSERT_NOT_NULL(msg);

    kafka_common_Error_destroy(err);
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
    kafka_common_Error_t *errors[2] = { NULL, NULL };

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
    kafka_common_Error_t *errors[3] = { NULL, NULL, NULL };

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
    kafka_common_Error_destroy(errors[1]);
    kafka_producer_FutureRecordMetadata_destroy(futures[2]);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Close then send
// ---------------------------------------------------------------------------

void test_close_then_send(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);

    kafka_common_Error_destroy(err);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

void test_flush(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    kafka_common_Error_t *err = NULL;
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

    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);

    TEST_ASSERT_NOT_EQUAL(0, kafka_common_Error_code(err));

    const char *msg = kafka_common_Error_message(err);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_TRUE(strlen(msg) > 0);

    /* Every hierarchy predicate (CLAUDE.md §10.4) on a real handle, asserted
     * rather than merely called, so a predicate wired to the wrong Rust method
     * fails here.
     *
     * The producer was closed above, so `send` returns
     * `Error::illegal_state("MockProducer is already closed.")`. That is the
     * GENERIC family — `java.lang.IllegalStateException` is a sibling of
     * `KafkaException`, not a subclass — so every predicate answers false,
     * including `is_kafka_error`. That is the whole point of having it: a
     * caller in C, which cannot see the enum variant, can still tell "you
     * misused the client" from "the broker reported an error". Its code is the
     * unknown-server -1 (asserted non-zero above), which is why the retriable /
     * fatal / metadata / auth predicates are false too. */
    TEST_ASSERT_FALSE(kafka_common_Error_is_kafka_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_api_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_retriable_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_fatal_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_refresh_retriable_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_metadata_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authentication_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authorization_error(err));

    kafka_common_Error_destroy(err);
    kafka_producer_Producer_destroy(producer);
}

void test_error_null_safety(void) {
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Error_code(NULL));
    TEST_ASSERT_NULL(kafka_common_Error_message(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_retriable_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_fatal_error(NULL));
    /* Every predicate is null-tolerant and answers false — the whole §10.4 set,
     * so a newly added one cannot skip this contract. */
    TEST_ASSERT_FALSE(kafka_common_Error_is_kafka_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_api_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_refresh_retriable_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_metadata_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authentication_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authorization_error(NULL));
    kafka_common_Error_destroy(NULL);  /* no-op */
}

// ---------------------------------------------------------------------------
// Mock-specific: clear and history
// ---------------------------------------------------------------------------

void test_mock_clear(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_Error_t *err = NULL;
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
    kafka_common_Error_t *errors[1] = { NULL };

    int32_t sent = kafka_producer_Producer_send_batch(
        producer, records, 1, futures, errors);
    TEST_ASSERT_EQUAL_INT32(1, sent);
    TEST_ASSERT_NULL(errors[0]);

    kafka_common_Error_t *err = NULL;
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
    kafka_common_Error_t *err = NULL;
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
    kafka_common_Error_t *err = NULL;
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
    kafka_common_Error_t *err = NULL;
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

    kafka_common_Error_destroy(err);
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
    kafka_common_Error_t *out_errors[3] = {NULL, NULL, NULL};
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

    kafka_common_Error_t *err = NULL;
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
        kafka_common_Error_t *err = NULL;
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

    return UNITY_END();
}
