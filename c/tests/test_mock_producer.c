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
// Fixtures
//
// The mock is a `kafka_producer_MockProducer_t`; the `Producer` interface is
// reached through the borrowed `__as_Producer` view, which stays valid until
// the mock is destroyed (CLAUDE.md §4, "Traits"). The mock has no
// serializers, so a record's key and value are opaque `void *`s it never
// reads; the tests pass `kafka_Bytes_t` views the way a serializer-less
// KafkaProducer would.
//
// No callback ever runs on a Rust thread: `_cb` completions and delivery
// callbacks are queued on the producer's callback vector and run on the
// thread that calls `kafka_producer_Producer_execute_callbacks`
// (`callback_pump_t`, test_support.h).
// ---------------------------------------------------------------------------

typedef struct {
    kafka_producer_MockProducer_t *mock;
    const kafka_producer_Producer_t *producer;
    callback_pump_t pump;
} fixture_t;

static void fixture_init(fixture_t *f, int8_t auto_complete) {
    f->mock = kafka_producer_MockProducer_with_auto_complete(auto_complete);
    TEST_ASSERT_NOT_NULL(f->mock);
    f->producer = kafka_producer_MockProducer__as_Producer(f->mock);
    TEST_ASSERT_NOT_NULL(f->producer);
    callback_pump_install(&f->pump, f->producer);
}

static void fixture_destroy(fixture_t *f) {
    kafka_producer_MockProducer_destroy(f->mock);
    callback_pump_destroy(&f->pump);
}

static const uint8_t KEY_BYTES[] = "my-key";
static const uint8_t VALUE_BYTES[] = "my-value";
static const kafka_Bytes_t KEY = { KEY_BYTES, (int32_t)sizeof(KEY_BYTES) - 1 };
static const kafka_Bytes_t VALUE = { VALUE_BYTES, (int32_t)sizeof(VALUE_BYTES) - 1 };

/* A record with an optional partition (`-1` for none); owned by the caller. */
static kafka_producer_ProducerRecord_t *make_record(const char *topic, int32_t partition,
                                                     const void *key, const void *value) {
    kafka_producer_ProducerRecord_t *record = NULL;
    kafka_common_Error_t *err = kafka_producer_ProducerRecord_with_partition_key(
        topic, partition, key, value, &record);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(record);
    return record;
}

/* `MockProducer.history().size()`. */
static int32_t history_count(kafka_producer_MockProducer_t *mock) {
    kafka_List_t *history = kafka_producer_MockProducer_history(mock);
    TEST_ASSERT_NOT_NULL(history);
    int32_t count = kafka_List_size(history);
    kafka_List_destroy(history);
    return count;
}

/* Sends one record, asserting success; the caller owns the future. */
static kafka_common_KafkaFuture_t *send_record(const kafka_producer_Producer_t *producer,
                                               const char *topic, int32_t partition,
                                               const void *key, const void *value) {
    kafka_producer_ProducerRecord_t *record = make_record(topic, partition, key, value);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = kafka_producer_Producer_send(producer, record, &future);
    kafka_producer_ProducerRecord_destroy(record);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    return future;
}

/* Sends one minimal record and frees its future. */
static void send_one(const kafka_producer_Producer_t *producer, const char *topic) {
    kafka_common_KafkaFuture_destroy(send_record(producer, topic, -1, NULL, &VALUE));
}

/* `KafkaFuture.get()` on a succeeded future: the metadata is BORROWED from
 * the future and stays valid until the future is destroyed. */
static const kafka_producer_RecordMetadata_t *get_metadata(const kafka_common_KafkaFuture_t *future) {
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(future, &value);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(value);
    return (const kafka_producer_RecordMetadata_t *)value;
}

/* `KafkaFuture.get()` on a failed future: the owned error. */
static kafka_common_Error_t *get_error(const kafka_common_KafkaFuture_t *future) {
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(future, &value);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(value);
    return err;
}

/* True if `err` is the transaction-control guard rejection rather than an
 * ordinary state error: the guard is `LocalConcurrentModification`, a class of
 * its own, so both the code and the message identify it. */
static int is_txn_guard_error(const kafka_common_Error_t *err) {
    if (err == NULL) {
        return 0;
    }
    const char *msg = kafka_common_Error_message(err);
    return kafka_common_Error_code(err) == kafka_common_ErrorCode_e_LOCAL_CONCURRENT_MODIFICATION
        || (msg != NULL && strstr(msg, "not safe for concurrent access") != NULL);
}

/* Asserts `err` is a real failure with exactly `expected_message`, not the
 * guard rejection, then frees it. */
static void assert_error_message(kafka_common_Error_t *err, const char *expected_message) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    const char *msg = kafka_common_Error_message(err);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_EQUAL_STRING(expected_message, msg);
    kafka_common_Error_destroy(err);
}

// ---------------------------------------------------------------------------
// Callback captures
// ---------------------------------------------------------------------------

/* What a `kafka_producer_Callback_t` (Java's delivery `Callback`) saw. Both
 * arguments are borrowed for the call. */
typedef struct {
    atomic_int fired;
    int had_metadata;
    int64_t offset;
    int32_t partition;
    char topic[256];
    int had_error;
    int32_t error_code;
    pthread_t thread_id;  /* the thread the callback ran on */
} record_result_t;

static void on_completion(void *self_, const kafka_producer_RecordMetadata_t *metadata,
                          const kafka_common_Error_t *error) {
    record_result_t *r = (record_result_t *)self_;
    if (metadata != NULL) {
        r->had_metadata = 1;
        r->offset = kafka_producer_RecordMetadata_offset(metadata);
        r->partition = kafka_producer_RecordMetadata_partition(metadata);
        const char *t = kafka_producer_RecordMetadata_topic(metadata);
        if (t != NULL) {
            strncpy(r->topic, t, sizeof(r->topic) - 1);
        }
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_Error_code(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

/* What a `send_cb` completion delivered: the owned future (kept for the test
 * to inspect and destroy) or the owned error (read and freed here). */
typedef struct {
    atomic_int fired;
    kafka_common_KafkaFuture_t *future;
    int had_error;
    int32_t error_code;
    char message[256];
    pthread_t thread_id;
} send_result_t;

static void on_send(kafka_common_KafkaFuture_t *value, kafka_common_Error_t *error, void *opaque) {
    send_result_t *r = (send_result_t *)opaque;
    r->future = value;
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_Error_code(error);
        const char *msg = kafka_common_Error_message(error);
        if (msg != NULL) {
            strncpy(r->message, msg, sizeof(r->message) - 1);
        }
        kafka_common_Error_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

/* What a `void` operation's `_cb` completion delivered. */
typedef struct {
    atomic_int fired;
    int had_error;
    int was_guard_error;
    int32_t error_code;
    char message[256];
    pthread_t thread_id;
} op_result_t;

static void on_operation(kafka_common_Error_t *error, void *opaque) {
    op_result_t *r = (op_result_t *)opaque;
    if (error != NULL) {
        r->had_error = 1;
        r->was_guard_error = is_txn_guard_error(error);
        r->error_code = kafka_common_Error_code(error);
        const char *msg = kafka_common_Error_message(error);
        if (msg != NULL) {
            strncpy(r->message, msg, sizeof(r->message) - 1);
        }
        kafka_common_Error_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

/* What `KafkaFuture_get_cb` delivered: the value is borrowed from the future. */
typedef struct {
    atomic_int fired;
    int had_value;
    int32_t partition;
    char topic[256];
    int had_error;
} get_result_t;

static void on_get(void *value, kafka_common_Error_t *error, void *opaque) {
    get_result_t *r = (get_result_t *)opaque;
    if (value != NULL) {
        const kafka_producer_RecordMetadata_t *metadata = (const kafka_producer_RecordMetadata_t *)value;
        r->had_value = 1;
        r->partition = kafka_producer_RecordMetadata_partition(metadata);
        strncpy(r->topic, kafka_producer_RecordMetadata_topic(metadata), sizeof(r->topic) - 1);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

/* Queues a `send_cb` of a minimal record on `topic`; the record handle is
 * destroyed right away (the FFI copies it), the bytes are static. */
static void send_cb_one(const kafka_producer_Producer_t *producer, const char *topic,
                        int32_t partition, send_result_t *result) {
    kafka_producer_ProducerRecord_t *record = make_record(topic, partition, NULL, &VALUE);
    kafka_producer_Producer_send_cb(producer, record, on_send, result);
    kafka_producer_ProducerRecord_destroy(record);
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

void test_create_and_destroy(void) {
    kafka_producer_MockProducer_t *mock = kafka_producer_MockProducer_with_auto_complete(1);
    TEST_ASSERT_NOT_NULL(mock);
    TEST_ASSERT_NOT_NULL(kafka_producer_MockProducer__as_Producer(mock));
    kafka_producer_MockProducer_destroy(mock);
}

void test_destroy_null(void) {
    kafka_producer_MockProducer_destroy(NULL);
}

// ---------------------------------------------------------------------------
// Send tests
// ---------------------------------------------------------------------------

void test_send_with_key_and_value(void) {
    fixture_t f;
    fixture_init(&f, 1);

    kafka_common_KafkaFuture_t *future = send_record(f.producer, "test-topic", 2, &KEY, &VALUE);
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));

    const kafka_producer_RecordMetadata_t *metadata = get_metadata(future);
    TEST_ASSERT_TRUE(kafka_producer_RecordMetadata_has_offset(metadata));
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(2, kafka_producer_RecordMetadata_partition(metadata));
    TEST_ASSERT_EQUAL_STRING("test-topic", kafka_producer_RecordMetadata_topic(metadata));
    /* MockProducer always returns NO_TIMESTAMP (-1). */
    TEST_ASSERT_FALSE(kafka_producer_RecordMetadata_has_timestamp(metadata));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_producer_RecordMetadata_timestamp(metadata));

    /* The history holds the record as sent. */
    kafka_List_t *history = kafka_producer_MockProducer_history(f.mock);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(history));
    const kafka_producer_ProducerRecord_t *sent = kafka_List_get(history, 0);
    TEST_ASSERT_EQUAL_STRING("test-topic", kafka_producer_ProducerRecord_topic(sent));
    TEST_ASSERT_EQUAL_INT32(2, kafka_producer_ProducerRecord_partition(sent));
    TEST_ASSERT_EQUAL_PTR(&KEY, kafka_producer_ProducerRecord_key(sent));
    TEST_ASSERT_EQUAL_PTR(&VALUE, kafka_producer_ProducerRecord_value(sent));
    kafka_List_destroy(history);

    /* The metadata is borrowed from the future: destroying the future frees it. */
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_send_null_key(void) {
    fixture_t f;
    fixture_init(&f, 1);

    kafka_producer_ProducerRecord_t *record = kafka_producer_ProducerRecord_new("topic", &VALUE);
    TEST_ASSERT_NULL(kafka_producer_ProducerRecord_key(record));
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_producer_Producer_send(f.producer, record, &future));
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));

    kafka_producer_ProducerRecord_destroy(record);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_send_null_value(void) {
    fixture_t f;
    fixture_init(&f, 1);

    kafka_producer_ProducerRecord_t *record = kafka_producer_ProducerRecord_with_key("topic", &KEY, NULL);
    TEST_ASSERT_NULL(kafka_producer_ProducerRecord_value(record));
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_producer_Producer_send(f.producer, record, &future));
    TEST_ASSERT_NOT_NULL(future);

    kafka_producer_ProducerRecord_destroy(record);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Multiple sends (incrementing offsets)
// ---------------------------------------------------------------------------

void test_multiple_sends_incrementing_offsets(void) {
    fixture_t f;
    fixture_init(&f, 1);

    for (int i = 0; i < 3; i++) {
        kafka_common_KafkaFuture_t *future = send_record(f.producer, "topic", 0, NULL, NULL);
        const kafka_producer_RecordMetadata_t *metadata = get_metadata(future);
        TEST_ASSERT_EQUAL_INT64((int64_t)i, kafka_producer_RecordMetadata_offset(metadata));
        kafka_common_KafkaFuture_destroy(future);
    }

    TEST_ASSERT_EQUAL_INT32(3, history_count(f.mock));
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Manual complete mode
// ---------------------------------------------------------------------------

void test_manual_complete(void) {
    fixture_t f;
    fixture_init(&f, 0);

    kafka_common_KafkaFuture_t *first = send_record(f.producer, "topic", -1, NULL, NULL);
    kafka_common_KafkaFuture_t *second = send_record(f.producer, "topic", -1, NULL, NULL);
    TEST_ASSERT_FALSE(kafka_common_KafkaFuture_is_done(first));
    TEST_ASSERT_FALSE(kafka_common_KafkaFuture_is_done(second));

    /* Complete the earliest one. */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(f.mock));
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(first));
    TEST_ASSERT_FALSE(kafka_common_KafkaFuture_is_done(second));

    const kafka_producer_RecordMetadata_t *metadata = get_metadata(first);
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));

    /* As in Java, `errorNext(null)` completes the next send successfully. */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_error_next(f.mock, NULL));
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(second));
    metadata = get_metadata(second);
    TEST_ASSERT_EQUAL_INT64(1, kafka_producer_RecordMetadata_offset(metadata));

    kafka_common_KafkaFuture_destroy(first);
    kafka_common_KafkaFuture_destroy(second);
    fixture_destroy(&f);
}

void test_manual_error(void) {
    fixture_t f;
    fixture_init(&f, 0);

    kafka_common_KafkaFuture_t *future = send_record(f.producer, "topic", -1, NULL, NULL);

    /* Fail it with an error the mock takes ownership of. */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_error_next(
        f.mock, kafka_common_Error_timeout("test error")));
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));

    kafka_common_Error_t *err = get_error(future);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_REQUEST_TIMED_OUT, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("test error", kafka_common_Error_message(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_timeout_error(err));

    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_complete_next_no_pending(void) {
    fixture_t f;
    fixture_init(&f, 0);
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_complete_next(f.mock));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_error_next(f.mock, NULL));
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Several records in a row (the former `send_batch`: one `send` per record)
// ---------------------------------------------------------------------------

void test_send_batch(void) {
    fixture_t f;
    fixture_init(&f, 1);

    kafka_producer_ProducerRecord_t *records[2] = {
        make_record("topic1", -1, &KEY, &VALUE),
        make_record("topic2", 1, NULL, NULL),
    };
    kafka_common_KafkaFuture_t *futures[2] = { NULL, NULL };

    for (int i = 0; i < 2; i++) {
        TEST_ASSERT_NULL(kafka_producer_Producer_send(f.producer, records[i], &futures[i]));
        TEST_ASSERT_NOT_NULL(futures[i]);
        TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(futures[i]));
    }
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_RecordMetadata_partition(get_metadata(futures[1])));
    TEST_ASSERT_EQUAL_INT32(2, history_count(f.mock));

    for (int i = 0; i < 2; i++) {
        kafka_common_KafkaFuture_destroy(futures[i]);
        kafka_producer_ProducerRecord_destroy(records[i]);
    }
    fixture_destroy(&f);
}

void test_send_batch_partial_failure(void) {
    /* The record in the middle cannot be built: a `ProducerRecord` without a
     * topic is rejected by `ProducerRecordOptionsBuilder_build` (a NULL topic
     * to the constructors is a precondition, CLAUDE.md §4). The records around
     * it are unaffected. */
    fixture_t f;
    fixture_init(&f, 1);

    kafka_common_KafkaFuture_t *first = send_record(f.producer, "topic1", -1, NULL, NULL);

    kafka_producer_ProducerRecordOptionsBuilder_t *builder = kafka_producer_ProducerRecordOptionsBuilder_new();
    kafka_producer_ProducerRecordOptionsBuilder_set_value(builder, &VALUE);
    kafka_producer_ProducerRecordOptions_t *options = NULL;
    kafka_common_Error_t *err = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &options);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(options);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_LOCAL_ILLEGAL_ARGUMENT, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("ProducerRecordOptionsBuilder::build: mandatory parameter `topic` was not set",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);

    kafka_common_KafkaFuture_t *third = send_record(f.producer, "topic3", -1, NULL, NULL);

    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(first));
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(third));
    TEST_ASSERT_EQUAL_INT32(2, history_count(f.mock));

    kafka_common_KafkaFuture_destroy(first);
    kafka_common_KafkaFuture_destroy(third);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Close then send
// ---------------------------------------------------------------------------

void test_close_then_send(void) {
    fixture_t f;
    fixture_init(&f, 1);

    TEST_ASSERT_NULL(kafka_producer_Producer_close(f.producer));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_closed(f.mock));

    kafka_producer_ProducerRecord_t *record = make_record("topic", -1, NULL, NULL);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = kafka_producer_Producer_send(f.producer, record, &future);
    /* `MockProducer::send` on a closed producer returns
       `Error::local_illegal_state("MockProducer is already closed.")`; the
       out slot is left untouched. */
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_LOCAL_ILLEGAL_STATE, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("MockProducer is already closed.", kafka_common_Error_message(err));
    TEST_ASSERT_NULL(future);

    kafka_common_Error_destroy(err);
    kafka_producer_ProducerRecord_destroy(record);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Queued send-path correctness (B1/B2/B3)
//
// `send_cb` hands the record to a FIFO submission task that calls the
// producer's `send` later; flush, close and the transaction-control
// operations drain that task first. These exercise the callback obligation
// on a queued send's error, the drain ordering, and destroy racing a queued
// send.
// ---------------------------------------------------------------------------

void test_send_async_on_closed_producer_fires_callback(void) {
    /* B1: the submission task's `send` returns Err (closed) without any
     * delivery; the completion must still reach `cb`, with the error, rather
     * than be dropped — dropping it would leak `opaque` and hang an app
     * waiting on it. */
    fixture_t f;
    fixture_init(&f, 1);
    TEST_ASSERT_NULL(kafka_producer_Producer_close(f.producer));

    send_result_t result;
    memset(&result, 0, sizeof(result));
    send_cb_one(f.producer, "topic", -1, &result);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_error);
    TEST_ASSERT_NULL(result.future);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_LOCAL_ILLEGAL_STATE, result.error_code);
    TEST_ASSERT_EQUAL_STRING("MockProducer is already closed.", result.message);

    fixture_destroy(&f);
}

void test_flush_drains_async_queued_send(void) {
    /* B2: a record queued by send_cb is handed to the producer before flush
     * returns. auto_complete=1, so once handed over it completes at once;
     * without the drain, flush could return with the history still empty. */
    fixture_t f;
    fixture_init(&f, 1);

    send_result_t result;
    memset(&result, 0, sizeof(result));
    send_cb_one(f.producer, "topic", -1, &result);

    TEST_ASSERT_NULL(kafka_producer_Producer_flush(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_NOT_NULL(result.future);
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(result.future));
    kafka_common_KafkaFuture_destroy(result.future);

    fixture_destroy(&f);
}

void test_close_drains_async_queued_send(void) {
    /* B2: close flushes by default, so a queued send is produced, not lost to
     * a close/send race. */
    fixture_t f;
    fixture_init(&f, 1);

    send_result_t result;
    memset(&result, 0, sizeof(result));
    send_cb_one(f.producer, "topic", -1, &result);

    TEST_ASSERT_NULL(kafka_producer_Producer_close(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_NOT_NULL(result.future);
    kafka_common_KafkaFuture_destroy(result.future);

    fixture_destroy(&f);
}

void test_send_async_then_destroy(void) {
    /* B3: destroy right after a queued send must not use freed memory, and
     * the completion must fire exactly once — destroy awaits the `_cb` tasks
     * and runs the pending callbacks (never on a Rust thread, but on the
     * destroying one). auto_complete=0 leaves the send itself pending, the
     * teardown window the fix addressed. Repeated to widen the race window. */
    for (int i = 0; i < 50; i++) {
        fixture_t f;
        fixture_init(&f, 0);
        send_result_t result;
        memset(&result, 0, sizeof(result));
        send_cb_one(f.producer, "topic", -1, &result);
        fixture_destroy(&f);
        TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
        if (result.future != NULL) {
            kafka_common_KafkaFuture_destroy(result.future);
        }
    }
}

void test_destroy_with_pending_cb_fires_once(void) {
    /* A completion still queued when the mock is destroyed is delivered by
     * `_destroy`, exactly once: the notify hook announced it, nobody pumped. */
    fixture_t f;
    fixture_init(&f, 1);

    op_result_t flushed;
    memset(&flushed, 0, sizeof(flushed));
    kafka_producer_Producer_flush_cb(f.producer, on_operation, &flushed);
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&flushed.fired));

    fixture_destroy(&f);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&flushed.fired));
    TEST_ASSERT_FALSE(flushed.had_error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), flushed.thread_id));
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

void test_flush(void) {
    fixture_t f;
    fixture_init(&f, 0);

    kafka_common_KafkaFuture_t *future = send_record(f.producer, "topic", -1, NULL, NULL);
    TEST_ASSERT_FALSE(kafka_common_KafkaFuture_is_done(future));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_flushed(f.mock));

    TEST_ASSERT_NULL(kafka_producer_Producer_flush(f.producer));
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_flushed(f.mock));

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Error inspection
// ---------------------------------------------------------------------------

void test_error_inspection(void) {
    fixture_t f;
    fixture_init(&f, 1);
    TEST_ASSERT_NULL(kafka_producer_Producer_close(f.producer));

    kafka_producer_ProducerRecord_t *record = make_record("topic", -1, NULL, NULL);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = kafka_producer_Producer_send(f.producer, record, &future);
    kafka_producer_ProducerRecord_destroy(record);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);

    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_LOCAL_ILLEGAL_STATE, kafka_common_Error_code(err));
    const char *msg = kafka_common_Error_message(err);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_TRUE(strlen(msg) > 0);

    /* Every hierarchy predicate (CLAUDE.md §12.4) on a real handle, asserted
     * rather than merely called, so a predicate wired to the wrong Rust method
     * fails here.
     *
     * The producer was closed above, so `send` returns
     * `Error::local_illegal_state("MockProducer is already closed.")`. That
     * class is outside the `KafkaException` tree —
     * `java.lang.IllegalStateException` is a sibling, not a subclass — so
     * every hierarchy predicate answers false, including `is_kafka_error`.
     * Code and predicates are complementary, not redundant: the code names the
     * class, while the false predicates tell a C caller "you misused the
     * client" rather than "the broker reported an error". (Fatality is not
     * exported: `common.requests` is not a supported Kafka API — CLAUDE.md §4.) */
    TEST_ASSERT_FALSE(kafka_common_Error_is_kafka_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_api_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_retriable_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_refresh_retriable_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_metadata_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authentication_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authorization_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_configuration_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_application_recoverable_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_offset_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_out_of_order_sequence_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_serialization_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_timeout_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_consumer_invalid_offset_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_consumer_offset_out_of_range_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_transaction_abortable_error(err));

    /* Every class also has its own predicate (CLAUDE.md §3), generated by
     * `cargo xtask generate-error-predicates`: true for this class (and any
     * subclass a later Java release adds), false for a sibling. The Rust test
     * `ffi_every_class_has_its_own_predicate` checks all of them against every
     * class; this spot check proves the generated exports link from C. */
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_local_concurrent_modification_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_topic_authorization_error(err));

    kafka_common_Error_destroy(err);
    fixture_destroy(&f);
}

void test_error_null_safety(void) {
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Error_code(NULL));
    TEST_ASSERT_NULL(kafka_common_Error_message(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_retriable_error(NULL));
    /* Every predicate is null-tolerant and answers false. The hierarchy set is
     * listed here; the Rust test `ffi_every_class_has_its_own_predicate` covers
     * every generated class predicate, so a newly added one cannot skip this
     * contract. */
    TEST_ASSERT_FALSE(kafka_common_Error_is_kafka_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_api_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_refresh_retriable_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_metadata_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authentication_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authorization_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_configuration_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_application_recoverable_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_invalid_offset_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_out_of_order_sequence_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_serialization_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_timeout_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_consumer_invalid_offset_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_consumer_offset_out_of_range_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_transaction_abortable_error(NULL));
    TEST_ASSERT_FALSE(kafka_common_Error_is_local_illegal_state_error(NULL));
    kafka_common_Error_destroy(NULL);  /* no-op */
}

// ---------------------------------------------------------------------------
// Per-variant payload accessors (CLAUDE.md §4: "Exceptions having additional
// fields in Java")
//
// The error is built with the Java static factory of its class, handed to
// `kafka_producer_MockProducer_error_next` (which takes ownership) and read
// back through the failed future, so the payload survives the round trip
// through the producer and `kafka_common_Error_<class>` extracts it. The
// Rust-side `ffi::common::tests` cover the same accessors directly.
// ---------------------------------------------------------------------------

/* Sends one record and fails it with `error` (consumed), returning the error
 * the future reports. Caller owns the returned handle and `*out_future`. */
static kafka_common_Error_t *trigger_error(fixture_t *f, kafka_common_Error_t *error,
                                           kafka_common_KafkaFuture_t **out_future) {
    *out_future = send_record(f->producer, "topic", -1, NULL, NULL);
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_error_next(f->mock, error));
    return get_error(*out_future);
}

void test_error_payload_topic_authorization(void) {
    fixture_t f;
    fixture_init(&f, 0);
    kafka_common_KafkaFuture_t *future = NULL;

    kafka_List_t *unauthorized = kafka_List_new();
    kafka_List_add(unauthorized, (void *)"topic-a");
    kafka_List_add(unauthorized, (void *)"topic-b");
    kafka_common_Error_t *err = trigger_error(&f, kafka_common_Error_topic_authorization(unauthorized), &future);
    kafka_List_destroy(unauthorized);

    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_TOPIC_AUTHORIZATION_FAILED, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_topic_authorization_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_authorization_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(err));

    const kafka_common_TopicAuthorizationError_t *handle = kafka_common_Error_topic_authorization_error(err);
    TEST_ASSERT_NOT_NULL(handle);
    kafka_List_t *topics = kafka_common_TopicAuthorizationError_unauthorized_topics(handle);
    TEST_ASSERT_NOT_NULL(topics);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(topics));
    /* A set in Java: order is not part of the contract. */
    const char *first = kafka_List_get(topics, 0);
    const char *second = kafka_List_get(topics, 1);
    TEST_ASSERT_TRUE((strcmp(first, "topic-a") == 0 && strcmp(second, "topic-b") == 0)
                     || (strcmp(first, "topic-b") == 0 && strcmp(second, "topic-a") == 0));
    kafka_List_destroy(topics);

    /* Wrong-variant extraction returns null. */
    TEST_ASSERT_NULL(kafka_common_Error_group_authorization_error(err));

    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_error_payload_group_authorization(void) {
    fixture_t f;
    fixture_init(&f, 0);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = trigger_error(&f, kafka_common_Error_group_authorization("my-group"), &future);

    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_GROUP_AUTHORIZATION_FAILED, kafka_common_Error_code(err));
    const kafka_common_GroupAuthorizationError_t *handle = kafka_common_Error_group_authorization_error(err);
    TEST_ASSERT_NOT_NULL(handle);
    /* Borrowed from the handle, like every `&str` getter. */
    TEST_ASSERT_EQUAL_STRING("my-group", kafka_common_GroupAuthorizationError_group_id(handle));

    TEST_ASSERT_NULL(kafka_common_Error_topic_authorization_error(err));

    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_error_payload_invalid_topic(void) {
    fixture_t f;
    fixture_init(&f, 0);
    kafka_common_KafkaFuture_t *future = NULL;

    kafka_List_t *invalid = kafka_List_new();
    kafka_List_add(invalid, (void *)"bad topic");
    kafka_common_Error_t *err = trigger_error(&f, kafka_common_Error_invalid_topics(invalid), &future);
    kafka_List_destroy(invalid);

    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_INVALID_TOPIC_ERROR, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_invalid_topic_error(err));
    const kafka_common_InvalidTopicError_t *handle = kafka_common_Error_invalid_topic(err);
    TEST_ASSERT_NOT_NULL(handle);
    kafka_List_t *topics = kafka_common_InvalidTopicError_invalid_topics(handle);
    TEST_ASSERT_NOT_NULL(topics);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(topics));
    TEST_ASSERT_EQUAL_STRING("bad topic", (const char *)kafka_List_get(topics, 0));
    kafka_List_destroy(topics);

    TEST_ASSERT_NULL(kafka_common_Error_topic_authorization_error(err));

    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_error_payload_throttling_quota_exceeded(void) {
    fixture_t f;
    fixture_init(&f, 0);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = trigger_error(
        &f, kafka_common_Error_throttling_quota_exceeded(250, "slow down"), &future);

    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_THROTTLING_QUOTA_EXCEEDED, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("slow down", kafka_common_Error_message(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_throttling_quota_exceeded_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_retriable_error(err));
    const kafka_common_ThrottlingQuotaExceededError_t *handle =
        kafka_common_Error_throttling_quota_exceeded_error(err);
    TEST_ASSERT_NOT_NULL(handle);
    TEST_ASSERT_EQUAL_INT32(250, kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(handle));

    TEST_ASSERT_NULL(kafka_common_Error_record_too_large_error(err));

    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_error_payload_record_too_large(void) {
    fixture_t f;
    fixture_init(&f, 0);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = trigger_error(&f, kafka_common_Error_record_too_large("too big"), &future);

    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_MESSAGE_TOO_LARGE, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_record_too_large_error(err));
    const kafka_common_RecordTooLargeError_t *handle = kafka_common_Error_record_too_large_error(err);
    TEST_ASSERT_NOT_NULL(handle);
    /* Java's `recordTooLargePartitions` defaults to `null`, not an empty map —
     * the accessor must return NULL, not an empty map. */
    TEST_ASSERT_NULL(kafka_common_RecordTooLargeError_record_too_large_partitions(handle));

    TEST_ASSERT_NULL(kafka_common_Error_throttling_quota_exceeded_error(err));

    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Mock-specific: clear and history
// ---------------------------------------------------------------------------

void test_mock_clear(void) {
    fixture_t f;
    fixture_init(&f, 1);

    kafka_common_KafkaFuture_t *future = send_record(f.producer, "topic", -1, NULL, NULL);
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    kafka_producer_MockProducer_clear(f.mock);
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// RecordMetadata getters
// ---------------------------------------------------------------------------

void test_record_metadata_getters(void) {
    fixture_t f;
    fixture_init(&f, 1);

    kafka_common_KafkaFuture_t *future = send_record(f.producer, "getter-topic", 7, &KEY, &VALUE);
    const kafka_producer_RecordMetadata_t *metadata = get_metadata(future);

    TEST_ASSERT_TRUE(kafka_producer_RecordMetadata_has_offset(metadata));
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(7, kafka_producer_RecordMetadata_partition(metadata));
    TEST_ASSERT_EQUAL_STRING("getter-topic", kafka_producer_RecordMetadata_topic(metadata));
    TEST_ASSERT_FALSE(kafka_producer_RecordMetadata_has_timestamp(metadata));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_producer_RecordMetadata_timestamp(metadata));

    /* `toString()`, owned. */
    char *text = kafka_producer_RecordMetadata_to_string(metadata);
    TEST_ASSERT_NOT_NULL(text);
    TEST_ASSERT_EQUAL_STRING("getter-topic-7@0", text);
    kafka_string_destroy(text);

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Queued (`_cb`) variants and the callback pump
// ---------------------------------------------------------------------------

void test_send_async_with_key_and_value(void) {
    fixture_t f;
    fixture_init(&f, 1);

    send_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_producer_ProducerRecord_t *record = make_record("async-topic", 3, &KEY, &VALUE);
    kafka_producer_Producer_send_cb(f.producer, record, on_send, &result);
    kafka_producer_ProducerRecord_destroy(record);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_NOT_NULL(result.future);

    const kafka_producer_RecordMetadata_t *metadata = get_metadata(result.future);
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(3, kafka_producer_RecordMetadata_partition(metadata));
    TEST_ASSERT_EQUAL_STRING("async-topic", kafka_producer_RecordMetadata_topic(metadata));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    kafka_common_KafkaFuture_destroy(result.future);
    fixture_destroy(&f);
}

void test_send_async_null_key(void) {
    fixture_t f;
    fixture_init(&f, 1);

    send_result_t result;
    memset(&result, 0, sizeof(result));
    send_cb_one(f.producer, "topic", -1, &result);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_NOT_NULL(result.future);
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(result.future));

    kafka_common_KafkaFuture_destroy(result.future);
    fixture_destroy(&f);
}

void test_send_async_validation_error(void) {
    /* A record that fails validation never reaches `send_cb`: the builder
     * rejects it synchronously, so there is nothing to queue and no callback
     * to fire. (A NULL topic to the constructors is a precondition now.) */
    fixture_t f;
    fixture_init(&f, 1);

    kafka_producer_ProducerRecordOptionsBuilder_t *builder = kafka_producer_ProducerRecordOptionsBuilder_new();
    kafka_producer_ProducerRecordOptionsBuilder_set_value(builder, &VALUE);
    kafka_producer_ProducerRecordOptionsBuilder_set_partition(builder, 2);
    kafka_producer_ProducerRecordOptions_t *options = NULL;
    kafka_common_Error_t *err = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &options);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("ProducerRecordOptionsBuilder::build: mandatory parameter `topic` was not set",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);

    /* Nothing was queued: the pump has nothing to run. */
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&f.pump.notified));

    fixture_destroy(&f);
}

void test_send_batch_async(void) {
    /* Three queued sends complete in order through one or more pumps; the
     * returned counts add up to the three completions. */
    fixture_t f;
    fixture_init(&f, 1);

    send_result_t results[3];
    memset(results, 0, sizeof(results));
    atomic_int fired;
    atomic_init(&fired, 0);
    for (int i = 0; i < 3; i++) {
        send_cb_one(f.producer, "batch-topic", -1, &results[i]);
    }

    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &results[i].fired, 1));
    }
    TEST_ASSERT_EQUAL_INT(3, atomic_load(&f.pump.executed));
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_EQUAL_INT(1, atomic_load(&results[i].fired));
        TEST_ASSERT_FALSE(results[i].had_error);
        TEST_ASSERT_NOT_NULL(results[i].future);
        /* FIFO: the i-th queued send got offset i. */
        TEST_ASSERT_EQUAL_INT64((int64_t)i, kafka_producer_RecordMetadata_offset(get_metadata(results[i].future)));
        kafka_common_KafkaFuture_destroy(results[i].future);
    }
    (void)fired;

    fixture_destroy(&f);
}

void test_future_get_async(void) {
    fixture_t f;
    fixture_init(&f, 1);

    kafka_common_KafkaFuture_t *future = send_record(f.producer, "fut-topic", 1, NULL, &VALUE);

    get_result_t result;
    memset(&result, 0, sizeof(result));
    /* The future belongs to the producer, so the delivery is queued on its
     * callback vector, not run inline. */
    kafka_common_KafkaFuture_get_cb(future, on_get, &result);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_TRUE(result.had_value);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT32(1, result.partition);
    TEST_ASSERT_EQUAL_STRING("fut-topic", result.topic);

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_flush_async(void) {
    fixture_t f;
    fixture_init(&f, 0);

    kafka_common_KafkaFuture_t *future = send_record(f.producer, "topic", -1, NULL, NULL);
    TEST_ASSERT_FALSE(kafka_common_KafkaFuture_is_done(future));

    op_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_producer_Producer_flush_cb(f.producer, on_operation, &result);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_flushed(f.mock));

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_close_async(void) {
    fixture_t f;
    fixture_init(&f, 1);

    op_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_producer_Producer_close_cb(f.producer, on_operation, &result);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_closed(f.mock));

    /* `close_with_timeout` rejects a negative timeout before touching the
     * producer. */
    kafka_common_Error_t *err = kafka_producer_Producer_close_with_timeout(f.producer, -1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("The timeout cannot be negative.", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    fixture_destroy(&f);
}

void test_async_callbacks_single_thread(void) {
    /* Every completion runs on the thread that calls `execute_callbacks`,
     * and nothing runs before that call. */
    fixture_t f;
    fixture_init(&f, 1);

    send_result_t results[5];
    memset(results, 0, sizeof(results));
    for (int i = 0; i < 5; i++) {
        send_cb_one(f.producer, "thread-topic", -1, &results[i]);
    }

    /* The hook announces the first completion; none has fired yet. */
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_EQUAL_INT(0, atomic_load(&results[i].fired));
    }

    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &results[i].fired, 1));
    }
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(pthread_equal(pthread_self(), results[i].thread_id));
        kafka_common_KafkaFuture_destroy(results[i].future);
    }

    fixture_destroy(&f);
}

void test_callbacks_notify_fires_once_per_transition(void) {
    /* `set_callbacks_notify` fires once each time the vector goes from empty
     * to non-empty; `execute_callbacks` returns how many it ran. The mock
     * completes sends on the calling thread, so the sequence is
     * deterministic. */
    fixture_t f;
    fixture_init(&f, 0);

    record_result_t results[3];
    memset(results, 0, sizeof(results));
    kafka_common_KafkaFuture_t *futures[3];
    for (int i = 0; i < 3; i++) {
        kafka_producer_ProducerRecord_t *record = make_record("notify-topic", -1, NULL, &VALUE);
        kafka_producer_Callback_t *callback = kafka_producer_Callback_new(&results[i], on_completion);
        TEST_ASSERT_NULL(kafka_producer_Producer_send_with_callback(f.producer, record, callback, &futures[i]));
        kafka_producer_Callback_destroy(callback);
        kafka_producer_ProducerRecord_destroy(record);
    }
    /* Nothing completed: no transition, nothing to run. */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&f.pump.notified));
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));

    /* Three completions queued while the vector is non-empty: one notify. */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(f.mock));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(f.mock));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(f.mock));
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&f.pump.notified));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&results[0].fired));

    TEST_ASSERT_EQUAL_INT32(3, callback_pump_execute(&f.pump));
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_EQUAL_INT(1, atomic_load(&results[i].fired));
        TEST_ASSERT_EQUAL_INT64((int64_t)i, results[i].offset);
    }
    /* Drained: a second pump runs nothing. */
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));

    /* Empty -> non-empty again: a second notify. */
    kafka_common_KafkaFuture_t *fourth = send_record(f.producer, "notify-topic", -1, NULL, &VALUE);
    op_result_t flushed;
    memset(&flushed, 0, sizeof(flushed));
    kafka_producer_Producer_flush_cb(f.producer, on_operation, &flushed);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &flushed.fired, 1));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&f.pump.notified));
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(fourth));

    for (int i = 0; i < 3; i++) {
        kafka_common_KafkaFuture_destroy(futures[i]);
    }
    kafka_common_KafkaFuture_destroy(fourth);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Transactions
//
// The transaction-control surface is exercised against the mock because the
// whole lifecycle then runs broker-free and deterministically. The history
// is the transactional-isolation probe: MockProducer only moves a record into
// the sent history when the transaction commits, so a record sent inside an
// open transaction is invisible there until `commit_transaction` returns.
//
// The concurrency guard around these five functions needs a control call to
// be slow enough to overlap with a second one, which no mock call is; that
// test therefore lives in test_kafka_producer.c.
// ---------------------------------------------------------------------------

/* A consumer whose group metadata the tests hand to
 * `send_offsets_to_transaction`, as in Java's
 * `producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())`. */
typedef struct {
    kafka_consumer_Consumer_t *consumer;
    kafka_consumer_ConsumerGroupMetadata_t *group_metadata;
} group_fixture_t;

static void group_init(group_fixture_t *g) {
    g->consumer = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(g->consumer);
    g->group_metadata = kafka_consumer_Consumer_group_metadata(g->consumer);
    TEST_ASSERT_NOT_NULL(g->group_metadata);
}

static void group_destroy(group_fixture_t *g) {
    /* The metadata handle is borrowed by the producer calls, not consumed:
     * still ours to destroy. */
    kafka_consumer_ConsumerGroupMetadata_destroy(g->group_metadata);
    kafka_consumer_Consumer_destroy(g->consumer);
}

/* A C-built offsets map: `kafka_common_TopicPartition_t *` ->
 * `kafka_consumer_OffsetAndMetadata_t *`, both owned by the caller (a map
 * built in C borrows its elements). */
typedef struct {
    kafka_Map_t *map;
    kafka_common_TopicPartition_t *partitions[4];
    kafka_consumer_OffsetAndMetadata_t *offsets[4];
    int count;
} offsets_fixture_t;

static void offsets_init(offsets_fixture_t *o) {
    o->map = kafka_Map_new();
    o->count = 0;
}

static void offsets_add(offsets_fixture_t *o, const char *topic, int32_t partition,
                        kafka_consumer_OffsetAndMetadata_t *oam) {
    TEST_ASSERT_NOT_NULL(oam);
    o->partitions[o->count] = kafka_common_TopicPartition_new(topic, partition);
    o->offsets[o->count] = oam;
    kafka_Map_put(o->map, o->partitions[o->count], oam);
    o->count++;
}

static void offsets_destroy(offsets_fixture_t *o) {
    kafka_Map_destroy(o->map);
    for (int i = 0; i < o->count; i++) {
        kafka_common_TopicPartition_destroy(o->partitions[i]);
        kafka_consumer_OffsetAndMetadata_destroy(o->offsets[i]);
    }
}

static kafka_consumer_OffsetAndMetadata_t *offset_and_metadata(int64_t offset, int32_t leader_epoch,
                                                               const char *metadata) {
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    kafka_common_Error_t *err = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
        offset, leader_epoch, metadata, &oam);
    TEST_ASSERT_NULL(err);
    return oam;
}

/* `MockProducer.committedOffset(group, tp)`: the owned entry, or NULL. */
static kafka_consumer_OffsetAndMetadata_t *committed_offset(kafka_producer_MockProducer_t *mock,
                                                            const char *group, const char *topic,
                                                            int32_t partition) {
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new(topic, partition);
    kafka_consumer_OffsetAndMetadata_t *oam = kafka_producer_MockProducer_committed_offset(mock, group, tp);
    kafka_common_TopicPartition_destroy(tp);
    return oam;
}

/* Reads every field of the committed offset back. Without this the test
 * passes even if the forwarding transposes partitions/offsets, drops
 * leader_epochs, or stages an empty map. */
static void assert_committed(kafka_producer_MockProducer_t *mock, const char *group,
                             const char *topic, int32_t partition, int64_t offset,
                             int32_t leader_epoch, const char *metadata) {
    kafka_consumer_OffsetAndMetadata_t *oam = committed_offset(mock, group, topic, partition);
    TEST_ASSERT_NOT_NULL(oam);
    TEST_ASSERT_EQUAL_INT64(offset, kafka_consumer_OffsetAndMetadata_offset(oam));
    int32_t got_epoch = -99;
    bool has_epoch = kafka_consumer_OffsetAndMetadata_leader_epoch(oam, &got_epoch);
    if (leader_epoch < 0) {
        TEST_ASSERT_FALSE(has_epoch);
    } else {
        TEST_ASSERT_TRUE(has_epoch);
        TEST_ASSERT_EQUAL_INT32(leader_epoch, got_epoch);
    }
    TEST_ASSERT_EQUAL_STRING(metadata, kafka_consumer_OffsetAndMetadata_metadata(oam));
    kafka_consumer_OffsetAndMetadata_destroy(oam);
}

void test_transaction_commit_publishes_records(void) {
    fixture_t f;
    fixture_init(&f, 1);

    TEST_ASSERT_FALSE(kafka_producer_MockProducer_transaction_initialized(f.mock));
    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_transaction_initialized(f.mock));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_transaction_in_flight(f.mock));

    send_one(f.producer, "txn-topic");
    /* Uncommitted: not in the sent history yet, but in the uncommitted one. */
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));
    kafka_List_t *uncommitted = kafka_producer_MockProducer_uncommitted_records(f.mock);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(uncommitted));
    kafka_List_destroy(uncommitted);

    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_transaction_committed(f.mock));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_transaction_in_flight(f.mock));
    TEST_ASSERT_EQUAL_INT64(1, kafka_producer_MockProducer_commit_count(f.mock));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_abort_discards_records(void) {
    fixture_t f;
    fixture_init(&f, 1);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));

    /* First transaction commits, so the history has a known non-zero baseline. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    /* Second transaction aborts: its record never reaches the history. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(f.producer));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_transaction_aborted(f.mock));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_commit_drains_async_queued_send(void) {
    /* A record queued with send_cb that had *returned* before
     * commit_transaction is included in the transaction: commit drains the
     * submission queue first (the same drain flush/close use), so the queued
     * record is handed to the producer and committed. Without the drain the
     * record would race the commit and the history could still be 0 when
     * commit returns. */
    fixture_t f;
    fixture_init(&f, 1);
    send_result_t result;
    memset(&result, 0, sizeof(result));

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));

    send_cb_one(f.producer, "txn-topic", -1, &result);
    /* Uncommitted (and possibly still queued): not in the sent history yet. */
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    kafka_common_KafkaFuture_destroy(result.future);

    fixture_destroy(&f);
}

void test_transaction_abort_drains_async_queued_send(void) {
    /* Counterpart: a record queued with send_cb before abort is drained
     * (handed to the producer) and then discarded with the aborted
     * transaction, exactly as a synchronous send would be — it never reaches
     * the committed history, nor the *next* transaction's. */
    fixture_t f;
    fixture_init(&f, 1);
    send_result_t result;
    memset(&result, 0, sizeof(result));

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));

    send_cb_one(f.producer, "txn-topic", -1, &result);

    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));
    /* Handed over before the abort, so its completion arrives; it is simply
     * not committed. */
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    kafka_common_KafkaFuture_destroy(result.future);

    /* A following transaction commits a synchronous record; the aborted
     * record must not reappear, so the history is exactly 1. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_send_offsets(void) {
    fixture_t f;
    fixture_init(&f, 1);
    group_fixture_t g;
    group_init(&g);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");

    offsets_fixture_t o;
    offsets_init(&o);
    offsets_add(&o, "input-topic", 0, offset_and_metadata(42, 5, "committed-by-txn"));
    kafka_consumer_OffsetAndMetadata_t *plain = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_new(7, &plain)); /* no epoch, no metadata */
    offsets_add(&o, "input-topic", 1, plain);

    TEST_ASSERT_FALSE(kafka_producer_MockProducer_sent_offsets(f.mock));
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(f.producer, o.map, g.group_metadata));
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_sent_offsets(f.mock));
    /* The map was copied during the call. */
    offsets_destroy(&o);

    /* Staged, not yet committed. */
    kafka_Map_t *uncommitted = kafka_producer_MockProducer_uncommitted_offsets(f.mock);
    TEST_ASSERT_NOT_NULL(uncommitted);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(uncommitted)); /* one group */
    kafka_Map_destroy(uncommitted);
    const char *group_id = kafka_consumer_ConsumerGroupMetadata_group_id(g.group_metadata);
    TEST_ASSERT_NULL(committed_offset(f.mock, group_id, "input-topic", 0));

    /* Inside an open transaction an empty map is a no-op that succeeds. This
     * is the one state where the two backends agree — see
     * test_transaction_send_offsets_zero_count_outside_transaction. */
    kafka_Map_t *empty = kafka_Map_new();
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(f.producer, empty, g.group_metadata));
    kafka_Map_destroy(empty);

    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    assert_committed(f.mock, group_id, "input-topic", 0, 42, 5, "committed-by-txn");
    assert_committed(f.mock, group_id, "input-topic", 1, 7, -1, "");

    /* A partition or group that was never staged is not reported. */
    TEST_ASSERT_NULL(committed_offset(f.mock, group_id, "input-topic", 2));
    TEST_ASSERT_NULL(committed_offset(f.mock, "other-group", "input-topic", 0));

    /* The commit is in the group-offsets history: one map per commit. */
    kafka_List_t *history = kafka_producer_MockProducer_consumer_group_offsets_history(f.mock);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(history));
    kafka_List_destroy(history);

    group_destroy(&g);
    fixture_destroy(&f);
}

void test_transaction_send_offsets_zero_count_outside_transaction(void) {
    /* An empty map stages nothing, but whether it *succeeds* is backend-specific,
     * and both behaviours are faithful to their Java counterpart:
     *
     *   - MockProducer verifies transaction state before the empty-map check
     *     (Java MockProducer:186-193 then :194-196), so this errors.
     *   - KafkaProducer short-circuits the empty map before consulting transaction
     *     state (Java KafkaProducer:738), so the same call returns success there.
     *
     * This pins the mock half. */
    fixture_t f;
    fixture_init(&f, 1);
    group_fixture_t g;
    group_init(&g);
    kafka_Map_t *empty = kafka_Map_new();

    /* Before init_transactions. */
    assert_error_message(
        kafka_producer_Producer_send_offsets_to_transaction(f.producer, empty, g.group_metadata),
        "MockProducer hasn't been initialized for transactions.");

    /* Initialized, but no transaction open. */
    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    assert_error_message(
        kafka_producer_Producer_send_offsets_to_transaction(f.producer, empty, g.group_metadata),
        "There is no open transaction.");

    /* Inside a transaction it succeeds, and stages nothing. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(f.producer, empty, g.group_metadata));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_sent_offsets(f.mock));

    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(f.producer));
    kafka_Map_destroy(empty);
    group_destroy(&g);
    fixture_destroy(&f);
}

void test_transaction_send_offsets_rejection_releases_guard(void) {
    /* send_offsets_to_transaction runs inside the transaction-control guard,
     * so an early failure has to release the flag. If it did not, the
     * producer would be permanently wedged and every later control call
     * rejected — which is what the follow-up calls here detect. */
    fixture_t f;
    fixture_init(&f, 1);
    group_fixture_t g;
    group_init(&g);

    /* An invalid offset is rejected when the `OffsetAndMetadata` is built,
     * before any producer call, with Java's message. */
    kafka_consumer_OffsetAndMetadata_t *bad = NULL;
    kafka_common_Error_t *err = kafka_consumer_OffsetAndMetadata_new(-5, &bad);
    TEST_ASSERT_NULL(bad);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("Invalid negative offset", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    offsets_fixture_t o;
    offsets_init(&o);
    offsets_add(&o, "input-topic", 0, offset_and_metadata(11, -1, NULL));

    /* Rejected *inside* the guard: the mock's own state check fails before a
     * transaction is open. */
    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    assert_error_message(
        kafka_producer_Producer_send_offsets_to_transaction(f.producer, o.map, g.group_metadata),
        "There is no open transaction.");

    /* The flag must have been released, so ordinary use continues. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_send_offsets_to_transaction(f.producer, o.map, g.group_metadata));
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));

    const char *group_id = kafka_consumer_ConsumerGroupMetadata_group_id(g.group_metadata);
    assert_committed(f.mock, group_id, "input-topic", 0, 11, -1, "");

    offsets_destroy(&o);
    group_destroy(&g);
    fixture_destroy(&f);
}

void test_transaction_commit_error_requires_abort(void) {
    fixture_t f;
    fixture_init(&f, 1);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");

    /* The installed error fails the next commit; the mock takes ownership of
     * it. A timed-out commit is the case a caller must abort rather than
     * retry. Setup-only, so it is issued before the control call it affects. */
    kafka_producer_MockProducer_set_commit_transaction_error(
        f.mock, kafka_common_Error_timeout("commit timed out"));

    kafka_common_Error_t *err = kafka_producer_Producer_commit_transaction(f.producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_REQUEST_TIMED_OUT, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("commit timed out", kafka_common_Error_message(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_timeout_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_retriable_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_transaction_abortable_error(err));
    /* Fatality is deliberately not exported (CLAUDE.md §4: `common.requests`
     * is not a supported Kafka API). `request_utils::is_fatal_error` is
     * authentication || authorization || one of five standalone classes, and
     * a timeout is none of them — so assert the exported halves. */
    TEST_ASSERT_FALSE(kafka_common_Error_is_authentication_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_authorization_error(err));
    kafka_common_Error_destroy(err);
    /* The failed commit left the transaction open and the records uncommitted. */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_transaction_in_flight(f.mock));
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    /* The error stays installed until cleared. */
    kafka_producer_MockProducer_set_commit_transaction_error(
        f.mock, kafka_common_Error_transaction_aborted_message("aborted by broker"));
    err = kafka_producer_Producer_commit_transaction(f.producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_TRANSACTION_ABORTED, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("aborted by broker", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* Clearing (NULL) must actually remove the installed error: prove it
     * against commit_transaction itself, which is the call the hook affects. */
    kafka_producer_MockProducer_set_commit_transaction_error(f.mock, NULL);
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    /* And the transaction really was open until then, so a fresh one behaves. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_success_does_not_require_abort(void) {
    fixture_t f;
    fixture_init(&f, 1);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));

    /* Begin twice: the second call is an ordinary illegal-state failure, which
     * must NOT be reported as requiring an abort. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    kafka_common_Error_t *err = kafka_producer_Producer_begin_transaction(f.producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_transaction_abortable_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("Transaction already started", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* Null handle is defined as false, like the other error accessors. */
    TEST_ASSERT_FALSE(kafka_common_Error_is_transaction_abortable_error(NULL));

    TEST_ASSERT_NULL(kafka_producer_Producer_abort_transaction(f.producer));
    fixture_destroy(&f);
}

void test_transaction_requires_init_first(void) {
    fixture_t f;
    fixture_init(&f, 1);

    /* Every control method fails before init_transactions has run. */
    assert_error_message(kafka_producer_Producer_begin_transaction(f.producer),
                         "MockProducer hasn't been initialized for transactions.");
    assert_error_message(kafka_producer_Producer_commit_transaction(f.producer),
                         "MockProducer hasn't been initialized for transactions.");
    assert_error_message(kafka_producer_Producer_abort_transaction(f.producer),
                         "MockProducer hasn't been initialized for transactions.");

    /* init twice is also an error (already initialized). */
    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    assert_error_message(kafka_producer_Producer_init_transactions(f.producer),
                         "MockProducer has already been initialized for transactions.");

    fixture_destroy(&f);
}

void test_transaction_commit_flushes_pending_sends(void) {
    /* auto_complete=0 leaves every send pending, so a transaction can hold
     * several unresolved sends at once. Neither the sends nor their futures
     * are covered by the transaction-control guard, and the commit is what
     * resolves them: Java's commitTransaction() flushes before committing. */
    fixture_t f;
    fixture_init(&f, 0);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));

    kafka_common_KafkaFuture_t *futures[3];
    for (int i = 0; i < 3; i++) {
        futures[i] = send_record(f.producer, "txn-topic", -1, NULL, &VALUE);
        /* Still pending, and invisible in the sent history. */
        TEST_ASSERT_FALSE(kafka_common_KafkaFuture_is_done(futures[i]));
    }
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));

    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(futures[i]));
        TEST_ASSERT_EQUAL_INT64((int64_t)i, kafka_producer_RecordMetadata_offset(get_metadata(futures[i])));
        kafka_common_KafkaFuture_destroy(futures[i]);
    }
    TEST_ASSERT_EQUAL_INT32(3, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_fenced_producer(void) {
    /* `fenceProducer()`: every later control call fails with
     * `ProducerFencedException`, a `send` with a `KafkaException` whose cause
     * is the fenced error (Java MockProducer:293). */
    fixture_t f;
    fixture_init(&f, 1);

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    TEST_ASSERT_NULL(kafka_producer_MockProducer_fence_producer(f.mock));

    kafka_common_Error_t *err = kafka_producer_Producer_commit_transaction(f.producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_PRODUCER_FENCED, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("MockProducer is fenced.", kafka_common_Error_message(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(err));
    kafka_common_Error_destroy(err);

    kafka_producer_ProducerRecord_t *record = make_record("topic", -1, NULL, NULL);
    kafka_common_KafkaFuture_t *future = NULL;
    err = kafka_producer_Producer_send(f.producer, record, &future);
    kafka_producer_ProducerRecord_destroy(record);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_EQUAL_STRING("MockProducer is fenced.", kafka_common_Error_message(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_api_error(err));
    kafka_common_Error_destroy(err);

    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Queued (`_cb`) transaction control
//
// The non-blocking twins of the blocking transaction-control functions
// (`beginTransaction` does not block in Java, so it has none). Each returns
// immediately and reports its outcome through a completion queued on the
// callback vector, so every assertion here pumps the vector first.
// ---------------------------------------------------------------------------

void test_transaction_async_full_lifecycle(void) {
    /* init -> begin -> send -> commit through the queued entry points, in the
     * order the Python client drives them. A record sent inside the open
     * transaction is invisible in the sent history until the commit completes. */
    fixture_t f;
    fixture_init(&f, 1);

    op_result_t init;
    memset(&init, 0, sizeof(init));
    kafka_producer_Producer_init_transactions_cb(f.producer, on_operation, &init);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &init.fired, 1));
    TEST_ASSERT_FALSE(init.had_error);

    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));

    send_one(f.producer, "txn-topic");
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    op_result_t commit;
    memset(&commit, 0, sizeof(commit));
    kafka_producer_Producer_commit_transaction_cb(f.producer, on_operation, &commit);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &commit.fired, 1));
    TEST_ASSERT_FALSE(commit.had_error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), commit.thread_id));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_async_abort(void) {
    /* The abort_transaction_cb happy path: a record sent inside an aborted
     * transaction never reaches the sent history. */
    fixture_t f;
    fixture_init(&f, 1);

    op_result_t init;
    memset(&init, 0, sizeof(init));
    kafka_producer_Producer_init_transactions_cb(f.producer, on_operation, &init);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &init.fired, 1));
    TEST_ASSERT_FALSE(init.had_error);

    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");

    op_result_t abort;
    memset(&abort, 0, sizeof(abort));
    kafka_producer_Producer_abort_transaction_cb(f.producer, on_operation, &abort);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &abort.fired, 1));
    TEST_ASSERT_FALSE(abort.had_error);
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_async_commit_drains_async_queued_send(void) {
    /* The queued commit variant, like the blocking one, drains the submission
     * queue before it runs, so a record queued by send_cb that had *returned*
     * before commit_transaction_cb is handed to the producer and committed.
     * This proves the drain lives in the shared control path, not only in
     * the blocking one. */
    fixture_t f;
    fixture_init(&f, 1);
    send_result_t record;
    memset(&record, 0, sizeof(record));

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));

    send_cb_one(f.producer, "txn-topic", -1, &record);
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    op_result_t commit;
    memset(&commit, 0, sizeof(commit));
    kafka_producer_Producer_commit_transaction_cb(f.producer, on_operation, &commit);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &commit.fired, 1));
    TEST_ASSERT_FALSE(commit.had_error);
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &record.fired, 1));
    TEST_ASSERT_FALSE(record.had_error);
    kafka_common_KafkaFuture_destroy(record.future);

    fixture_destroy(&f);
}

void test_transaction_async_abort_drains_async_queued_send(void) {
    /* Counterpart: a record queued by send_cb before abort_transaction_cb is
     * drained and then discarded with the aborted transaction — never in the
     * committed history. A following blocking transaction commits exactly one
     * record, proving the aborted record was discarded, not deferred. */
    fixture_t f;
    fixture_init(&f, 1);
    send_result_t record;
    memset(&record, 0, sizeof(record));

    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));

    send_cb_one(f.producer, "txn-topic", -1, &record);

    op_result_t abort;
    memset(&abort, 0, sizeof(abort));
    kafka_producer_Producer_abort_transaction_cb(f.producer, on_operation, &abort);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &abort.fired, 1));
    TEST_ASSERT_FALSE(abort.had_error);
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &record.fired, 1));
    TEST_ASSERT_FALSE(record.had_error);
    kafka_common_KafkaFuture_destroy(record.future);

    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    fixture_destroy(&f);
}

void test_transaction_async_send_offsets(void) {
    /* send_offsets_to_transaction_cb happy path: the offsets map is copied on
     * the calling thread (so it may be destroyed once the call returns) and
     * staged against the group; the queued commit then confirms them. Read
     * every field back, as the blocking test does. */
    fixture_t f;
    fixture_init(&f, 1);
    group_fixture_t g;
    group_init(&g);

    op_result_t init;
    memset(&init, 0, sizeof(init));
    kafka_producer_Producer_init_transactions_cb(f.producer, on_operation, &init);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &init.fired, 1));
    TEST_ASSERT_FALSE(init.had_error);

    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    send_one(f.producer, "txn-topic");

    offsets_fixture_t o;
    offsets_init(&o);
    offsets_add(&o, "input-topic", 0, offset_and_metadata(42, 5, "committed-by-txn"));
    kafka_consumer_OffsetAndMetadata_t *plain = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_new(7, &plain));
    offsets_add(&o, "input-topic", 1, plain);

    TEST_ASSERT_FALSE(kafka_producer_MockProducer_sent_offsets(f.mock));
    op_result_t offsets_done;
    memset(&offsets_done, 0, sizeof(offsets_done));
    kafka_producer_Producer_send_offsets_to_transaction_cb(
        f.producer, o.map, g.group_metadata, on_operation, &offsets_done);
    offsets_destroy(&o);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &offsets_done.fired, 1));
    TEST_ASSERT_FALSE(offsets_done.had_error);
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_sent_offsets(f.mock));

    op_result_t commit;
    memset(&commit, 0, sizeof(commit));
    kafka_producer_Producer_commit_transaction_cb(f.producer, on_operation, &commit);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &commit.fired, 1));
    TEST_ASSERT_FALSE(commit.had_error);
    TEST_ASSERT_EQUAL_INT32(1, history_count(f.mock));

    const char *group_id = kafka_consumer_ConsumerGroupMetadata_group_id(g.group_metadata);
    assert_committed(f.mock, group_id, "input-topic", 0, 42, 5, "committed-by-txn");
    assert_committed(f.mock, group_id, "input-topic", 1, 7, -1, "");

    group_destroy(&g);
    fixture_destroy(&f);
}

void test_transaction_async_send_offsets_rejection_releases_guard(void) {
    /* send_offsets_to_transaction_cb runs inside the transaction-control
     * guard, so a failure is an early return that must release the flag —
     * otherwise the producer would wedge and every later control call would
     * be rejected. The queued mirror of the blocking test. */
    fixture_t f;
    fixture_init(&f, 1);
    group_fixture_t g;
    group_init(&g);

    offsets_fixture_t o;
    offsets_init(&o);
    offsets_add(&o, "input-topic", 0, offset_and_metadata(11, -1, NULL));

    /* Rejected inside the guard: no transaction is open. The error must be
     * the state error, not the guard rejection (which would mean the flag
     * leaked from a prior call). */
    TEST_ASSERT_NULL(kafka_producer_Producer_init_transactions(f.producer));
    op_result_t rejected;
    memset(&rejected, 0, sizeof(rejected));
    kafka_producer_Producer_send_offsets_to_transaction_cb(
        f.producer, o.map, g.group_metadata, on_operation, &rejected);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &rejected.fired, 1));
    TEST_ASSERT_TRUE(rejected.had_error);
    TEST_ASSERT_FALSE(rejected.was_guard_error);
    TEST_ASSERT_EQUAL_STRING("There is no open transaction.", rejected.message);

    /* The flag must have been released, so ordinary use continues. */
    TEST_ASSERT_NULL(kafka_producer_Producer_begin_transaction(f.producer));
    op_result_t ok;
    memset(&ok, 0, sizeof(ok));
    kafka_producer_Producer_send_offsets_to_transaction_cb(
        f.producer, o.map, g.group_metadata, on_operation, &ok);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &ok.fired, 1));
    TEST_ASSERT_FALSE(ok.had_error);
    TEST_ASSERT_NULL(kafka_producer_Producer_commit_transaction(f.producer));

    const char *group_id = kafka_consumer_ConsumerGroupMetadata_group_id(g.group_metadata);
    assert_committed(f.mock, group_id, "input-topic", 0, 11, -1, "");

    offsets_destroy(&o);
    group_destroy(&g);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Send with callback (future + Callback, mirrors Java's send(record, Callback))
// ---------------------------------------------------------------------------

/* `send_with_callback` of one record whose completion lands in `result`; the
 * registration handle is destroyed right after the send (it is read during
 * the call). The caller owns the future. */
static kafka_common_KafkaFuture_t *send_with_callback(const kafka_producer_Producer_t *producer,
                                                      const char *topic, int32_t partition,
                                                      const void *key, const void *value,
                                                      record_result_t *result) {
    kafka_producer_ProducerRecord_t *record = make_record(topic, partition, key, value);
    kafka_producer_Callback_t *callback = kafka_producer_Callback_new(result, on_completion);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = kafka_producer_Producer_send_with_callback(producer, record, callback, &future);
    kafka_producer_Callback_destroy(callback);
    kafka_producer_ProducerRecord_destroy(record);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    return future;
}

void test_send_with_callback_fires_metadata_on_complete_next(void) {
    fixture_t f;
    fixture_init(&f, 0);

    record_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_common_KafkaFuture_t *future = send_with_callback(f.producer, "cb-topic", 4, &KEY, &VALUE, &result);

    /* auto_complete=0: nothing has completed yet, so neither the future nor
     * the callback has a result. */
    TEST_ASSERT_FALSE(kafka_common_KafkaFuture_is_done(future));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));

    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(f.mock));
    /* Queued, not fired inline: the pump delivers exactly one callback. */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));
    TEST_ASSERT_EQUAL_INT32(1, callback_pump_execute(&f.pump));

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT64(0, result.offset);
    TEST_ASSERT_EQUAL_INT32(4, result.partition);
    TEST_ASSERT_EQUAL_STRING("cb-topic", result.topic);

    /* The future reports the same outcome. */
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));
    const kafka_producer_RecordMetadata_t *metadata = get_metadata(future);
    TEST_ASSERT_EQUAL_INT64(result.offset, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(result.partition, kafka_producer_RecordMetadata_partition(metadata));
    TEST_ASSERT_EQUAL_STRING(result.topic, kafka_producer_RecordMetadata_topic(metadata));

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_send_with_callback_fires_error_on_error_next(void) {
    fixture_t f;
    fixture_init(&f, 0);

    record_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_common_KafkaFuture_t *future = send_with_callback(f.producer, "cb-err-topic", -1, NULL, NULL, &result);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    TEST_ASSERT_TRUE(kafka_producer_MockProducer_error_next(f.mock, kafka_common_Error_timeout("test error")));

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    /* Java's error path still hands the callback a placeholder metadata with
     * -1 for every unknown field (MockProducer.Completion.complete, and the
     * Callback.onCompletion javadoc: "an empty metadata with -1 value for all
     * fields except for topicPartition ... if an error occurred"). */
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_EQUAL_INT64(-1, result.offset);
    TEST_ASSERT_EQUAL_STRING("cb-err-topic", result.topic);
    TEST_ASSERT_TRUE(result.had_error);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_REQUEST_TIMED_OUT, result.error_code);

    /* The future reports the same failure. */
    kafka_common_Error_t *err = get_error(future);
    TEST_ASSERT_EQUAL_INT32(result.error_code, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("test error", kafka_common_Error_message(err));

    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_send_with_callback_future_and_callback_agree(void) {
    fixture_t f;
    fixture_init(&f, 1);

    record_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_common_KafkaFuture_t *future = send_with_callback(f.producer, "agree-topic", 7, NULL, &VALUE, &result);

    /* auto_complete=1: the record completes during the send. */
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));
    const kafka_producer_RecordMetadata_t *metadata = get_metadata(future);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(result.had_metadata);
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT64(kafka_producer_RecordMetadata_offset(metadata), result.offset);
    TEST_ASSERT_EQUAL_INT32(kafka_producer_RecordMetadata_partition(metadata), result.partition);
    TEST_ASSERT_EQUAL_STRING(kafka_producer_RecordMetadata_topic(metadata), result.topic);
    TEST_ASSERT_EQUAL_INT32(7, result.partition);

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

/* Pumps `producer` once from a helper thread and reports that thread's id. */
typedef struct {
    const kafka_producer_Producer_t *producer;
    int32_t executed;
    pthread_t thread_id;
} pump_thread_t;

static void *pump_thread(void *arg) {
    pump_thread_t *p = (pump_thread_t *)arg;
    p->thread_id = pthread_self();
    p->executed = kafka_producer_Producer_execute_callbacks(p->producer);
    return NULL;
}

void test_send_with_callback_runs_on_pumping_thread(void) {
    /* The delivery callback runs on whichever thread calls
     * `execute_callbacks` — here a helper thread, never the sending one and
     * never a Rust thread — and not before that call. */
    fixture_t f;
    fixture_init(&f, 1);

    record_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_common_KafkaFuture_t *future = send_with_callback(f.producer, "cb-thread-topic", -1, NULL, &VALUE, &result);

    /* Completed during the send (auto_complete=1), queued, announced... */
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    /* ...and not fired. */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    pump_thread_t pumper = { f.producer, -1, pthread_self() };
    pthread_t thread;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, NULL, pump_thread, &pumper));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(thread, NULL));

    TEST_ASSERT_EQUAL_INT32(1, pumper.executed);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(pthread_equal(pumper.thread_id, result.thread_id));
    TEST_ASSERT_FALSE(pthread_equal(pthread_self(), result.thread_id));

    kafka_common_KafkaFuture_destroy(future);
    fixture_destroy(&f);
}

void test_send_with_callback_validation_error_no_callback(void) {
    /* A record that fails validation is rejected synchronously by the
     * builder: no future, no send, and the callback is never invoked. (A
     * NULL topic to the constructors is a precondition now.) */
    fixture_t f;
    fixture_init(&f, 1);

    record_result_t result;
    memset(&result, 0, sizeof(result));

    /* No topic. */
    kafka_producer_ProducerRecordOptionsBuilder_t *builder = kafka_producer_ProducerRecordOptionsBuilder_new();
    kafka_producer_ProducerRecordOptionsBuilder_set_value(builder, &VALUE);
    kafka_producer_ProducerRecordOptions_t *options = NULL;
    kafka_common_Error_t *err = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &options);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(options);
    TEST_ASSERT_EQUAL_STRING("ProducerRecordOptionsBuilder::build: mandatory parameter `topic` was not set",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);

    /* No value either: `value` is mandatory too (NULL is a value, "unset" is
     * not). */
    builder = kafka_producer_ProducerRecordOptionsBuilder_new();
    kafka_producer_ProducerRecordOptionsBuilder_set_topic(builder, "topic");
    err = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &options);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("ProducerRecordOptionsBuilder::build: mandatory parameter `value` was not set",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);

    /* Both set: the record builds, and a NULL value is Java's null. */
    builder = kafka_producer_ProducerRecordOptionsBuilder_new();
    kafka_producer_ProducerRecordOptionsBuilder_set_topic(builder, "topic");
    kafka_producer_ProducerRecordOptionsBuilder_set_value(builder, NULL);
    TEST_ASSERT_NULL(kafka_producer_ProducerRecordOptionsBuilder_build(builder, &options));
    TEST_ASSERT_NOT_NULL(options);
    kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);
    kafka_producer_ProducerRecord_t *record = NULL;
    TEST_ASSERT_NULL(kafka_producer_ProducerRecord_with_options(options, &record));
    kafka_producer_ProducerRecordOptions_destroy(options);
    TEST_ASSERT_EQUAL_STRING("topic", kafka_producer_ProducerRecord_topic(record));
    TEST_ASSERT_NULL(kafka_producer_ProducerRecord_value(record));
    kafka_producer_ProducerRecord_destroy(record);

    /* Nothing was sent: no callback was queued. */
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));
    TEST_ASSERT_EQUAL_INT32(0, history_count(f.mock));

    fixture_destroy(&f);
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

    /* Several records */
    RUN_TEST(test_send_batch);
    RUN_TEST(test_send_batch_partial_failure);

    /* Close */
    RUN_TEST(test_close_then_send);

    /* Queued send-path correctness (B1/B2/B3) */
    RUN_TEST(test_send_async_on_closed_producer_fires_callback);
    RUN_TEST(test_flush_drains_async_queued_send);
    RUN_TEST(test_close_drains_async_queued_send);
    RUN_TEST(test_send_async_then_destroy);
    RUN_TEST(test_destroy_with_pending_cb_fires_once);

    /* Flush */
    RUN_TEST(test_flush);

    /* Error */
    RUN_TEST(test_error_inspection);
    RUN_TEST(test_error_null_safety);
    RUN_TEST(test_error_payload_topic_authorization);
    RUN_TEST(test_error_payload_group_authorization);
    RUN_TEST(test_error_payload_invalid_topic);
    RUN_TEST(test_error_payload_throttling_quota_exceeded);
    RUN_TEST(test_error_payload_record_too_large);

    /* Mock-specific */
    RUN_TEST(test_mock_clear);

    /* RecordMetadata getters */
    RUN_TEST(test_record_metadata_getters);

    /* Queued (`_cb`) variants and the callback pump */
    RUN_TEST(test_send_async_with_key_and_value);
    RUN_TEST(test_send_async_null_key);
    RUN_TEST(test_send_async_validation_error);
    RUN_TEST(test_send_batch_async);
    RUN_TEST(test_future_get_async);
    RUN_TEST(test_flush_async);
    RUN_TEST(test_close_async);
    RUN_TEST(test_async_callbacks_single_thread);
    RUN_TEST(test_callbacks_notify_fires_once_per_transition);

    /* Transactions */
    RUN_TEST(test_transaction_commit_publishes_records);
    RUN_TEST(test_transaction_abort_discards_records);
    RUN_TEST(test_transaction_commit_drains_async_queued_send);
    RUN_TEST(test_transaction_abort_drains_async_queued_send);
    RUN_TEST(test_transaction_send_offsets);
    RUN_TEST(test_transaction_send_offsets_zero_count_outside_transaction);
    RUN_TEST(test_transaction_send_offsets_rejection_releases_guard);
    RUN_TEST(test_transaction_commit_error_requires_abort);
    RUN_TEST(test_transaction_success_does_not_require_abort);
    RUN_TEST(test_transaction_requires_init_first);
    RUN_TEST(test_transaction_commit_flushes_pending_sends);
    RUN_TEST(test_transaction_fenced_producer);
    RUN_TEST(test_transaction_async_full_lifecycle);
    RUN_TEST(test_transaction_async_abort);
    RUN_TEST(test_transaction_async_commit_drains_async_queued_send);
    RUN_TEST(test_transaction_async_abort_drains_async_queued_send);
    RUN_TEST(test_transaction_async_send_offsets);
    RUN_TEST(test_transaction_async_send_offsets_rejection_releases_guard);

    /* Send with callback (future + Callback) */
    RUN_TEST(test_send_with_callback_fires_metadata_on_complete_next);
    RUN_TEST(test_send_with_callback_fires_error_on_error_next);
    RUN_TEST(test_send_with_callback_future_and_callback_agree);
    RUN_TEST(test_send_with_callback_runs_on_pumping_thread);
    RUN_TEST(test_send_with_callback_validation_error_no_callback);

    return UNITY_END();
}
