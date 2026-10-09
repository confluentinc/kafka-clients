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
// The mock is a `kafka_consumer_MockConsumer_t`; the `Consumer` interface is
// reached through the borrowed `__as_Consumer` view, which stays valid until
// the mock is destroyed and is never passed to `Consumer_destroy`
// (CLAUDE.md §4, "Traits"). The mock has no deserializers, so a record's key
// and value are opaque `void *`s it never reads: the tests pass
// `kafka_Bytes_t` views the way a deserializer-less KafkaConsumer would.
//
// No callback ever runs on a Rust thread: `_cb` completions and the listener
// invocations a `_cb` operation triggers are queued on the consumer's callback
// vector and run on the thread that calls
// `kafka_consumer_Consumer__execute_callbacks` (`callback_pump_t`,
// test_support.h). A blocking entry point runs everything on the calling
// thread instead.
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_MockConsumer_t *mock;
    kafka_consumer_Consumer_t *consumer;
    callback_pump_t pump;
} fixture_t;

static void fixture_init(fixture_t *f) {
    f->mock = NULL;
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_new("earliest", &f->mock));
    TEST_ASSERT_NOT_NULL(f->mock);
    f->consumer = kafka_consumer_MockConsumer__as_Consumer(f->mock);
    TEST_ASSERT_NOT_NULL(f->consumer);
    consumer_callback_pump_install(&f->pump, f->consumer);
}

static void fixture_destroy(fixture_t *f) {
    kafka_consumer_MockConsumer_destroy(f->mock);
    callback_pump_destroy(&f->pump);
}

/* A one-element list of the (topic, partition) pair; the caller owns both the
 * list and `*out_tp` (a C-built list borrows its elements). */
static kafka_List_t *tp_list(const char *topic, int32_t partition, kafka_common_TopicPartition_t **out_tp) {
    *out_tp = kafka_common_TopicPartition_new(topic, partition);
    TEST_ASSERT_NOT_NULL(*out_tp);
    kafka_List_t *list = kafka_List_new();
    kafka_List_add(list, *out_tp);
    return list;
}

/* `assign(Collections.singleton(new TopicPartition(topic, partition)))`. */
static kafka_common_Error_t *assign_one(kafka_consumer_Consumer_t *c, const char *topic, int32_t partition) {
    kafka_common_TopicPartition_t *tp = NULL;
    kafka_List_t *list = tp_list(topic, partition, &tp);
    kafka_common_Error_t *err = kafka_consumer_Consumer_assign(c, list);
    kafka_List_destroy(list);
    kafka_common_TopicPartition_destroy(tp);
    return err;
}

/* Builds the `Map<TopicPartition, Long>` the mock's `update*Offsets` take and
 * hands it to `update`; the map borrows the pair, which is freed here. */
static void update_offset(kafka_consumer_MockConsumer_t *mock,
                          void (*update)(kafka_consumer_MockConsumer_t *, const kafka_Map_t *),
                          const char *topic, int32_t partition, int64_t offset) {
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new(topic, partition);
    kafka_Map_t *map = kafka_Map_new();
    kafka_Map_put(map, tp, &offset);
    update(mock, map);
    kafka_Map_destroy(map);
    kafka_common_TopicPartition_destroy(tp);
}

/* A one-(topic,partition) assigned, earliest-positioned mock consumer. */
static void make_assigned_mock(fixture_t *f, const char *topic, int32_t partition) {
    fixture_init(f);
    TEST_ASSERT_NULL(assign_one(f->consumer, topic, partition));
    // EARLIEST reset uses the beginning offset to position the partition.
    update_offset(f->mock, kafka_consumer_MockConsumer_update_beginning_offsets, topic, partition, 0);
}

/* `addRecord(new ConsumerRecord<>(topic, partition, offset, key, value))`:
 * the record is copied by the mock, its `void *` key and value shared with
 * the caller. */
static void add_record(kafka_consumer_MockConsumer_t *mock, const char *topic, int32_t partition,
                       int64_t offset, const kafka_Bytes_t *key, const kafka_Bytes_t *value) {
    kafka_consumer_ConsumerRecord_t *record =
        kafka_consumer_ConsumerRecord_new(topic, partition, offset, key, value);
    TEST_ASSERT_NOT_NULL(record);
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_add_record(mock, record));
    kafka_consumer_ConsumerRecord_destroy(record);
}

/* Polls, asserting success; the caller owns the records. */
static kafka_consumer_ConsumerRecords_t *poll_ok(kafka_consumer_Consumer_t *c, int64_t timeout_ms) {
    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_poll(c, timeout_ms, &records);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(records);
    return records;
}

/* The first record of `records` for `tp`: a BORROWED pointer valid until the
 * records handle is destroyed (the list is freed here). */
static const kafka_consumer_ConsumerRecord_t *first_record(const kafka_consumer_ConsumerRecords_t *records,
                                                           const kafka_common_TopicPartition_t *tp) {
    kafka_List_t *list = kafka_consumer_ConsumerRecords_records_with_partition(records, tp);
    TEST_ASSERT_NOT_NULL(list);
    TEST_ASSERT_TRUE(kafka_List_size(list) >= 1);
    const kafka_consumer_ConsumerRecord_t *rec = (const kafka_consumer_ConsumerRecord_t *)kafka_List_get(list, 0);
    TEST_ASSERT_NOT_NULL(rec);
    kafka_List_destroy(list);
    return rec;
}

/* Asserts `err` is Java's `ConcurrentModificationException` translation —
 * class and message — then frees it. */
static void assert_concurrent_modification(kafka_common_Error_t *err) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_concurrent_modification_error(err));
    TEST_ASSERT_EQUAL_STRING("KafkaConsumer is not safe for multi-threaded access.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
}

static const uint8_t KEY_BYTES[] = {0x6b, 0x65, 0x79};   /* "key" */
static const uint8_t VALUE_BYTES[] = {0x76, 0x61, 0x6c}; /* "val" */
static const kafka_Bytes_t KEY = { KEY_BYTES, (int32_t)sizeof(KEY_BYTES) };
static const kafka_Bytes_t VALUE = { VALUE_BYTES, (int32_t)sizeof(VALUE_BYTES) };

// ---------------------------------------------------------------------------
// Completions and the listener probe used by the `_cb` tests
// ---------------------------------------------------------------------------

/* What a `_cb` completion observed. */
typedef struct {
    atomic_int fired;
    int had_error;
    kafka_common_Error_t *error;   /* kept for the test to classify and free */
    int32_t record_count;          /* poll completions */
    pthread_t thread_id;
} completion_t;

static void completion_init(completion_t *r) {
    memset(r, 0, sizeof(*r));
    atomic_init(&r->fired, 0);
}

static void completion_free(completion_t *r) {
    kafka_common_Error_destroy(r->error);
    r->error = NULL;
}

/* `kafka_consumer_Consumer_poll_cb_t`: takes ownership of both handles. */
static void on_poll(kafka_consumer_ConsumerRecords_t *records, kafka_common_Error_t *error, void *opaque) {
    completion_t *r = (completion_t *)opaque;
    r->thread_id = pthread_self();
    if (records != NULL) {
        r->record_count = kafka_consumer_ConsumerRecords_count(records);
        kafka_consumer_ConsumerRecords_destroy(records);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error = error;
    }
    atomic_fetch_add(&r->fired, 1);
}

/* The completion of every void `_cb` operation. */
static void on_void_op(kafka_common_Error_t *error, void *opaque) {
    completion_t *r = (completion_t *)opaque;
    r->thread_id = pthread_self();
    if (error != NULL) {
        r->had_error = 1;
        r->error = error;
    }
    atomic_fetch_add(&r->fired, 1);
}

/* The `self` of a C `ConsumerRebalanceListener` that does NOT report from
 * inside its methods: it leaves the `callback_id` for the test, which keeps
 * the operation that invoked it in flight for as long as the test wants
 * (the deterministic way to hold the single-owner flag). */
typedef struct {
    const kafka_consumer_Consumer_t *consumer;
    atomic_int assigned_calls;
    atomic_int revoked_calls;
    _Atomic(int64_t) pending_id;
} holding_listener_t;

static void holding_listener_init(holding_listener_t *l, const kafka_consumer_Consumer_t *consumer) {
    l->consumer = consumer;
    atomic_init(&l->assigned_calls, 0);
    atomic_init(&l->revoked_calls, 0);
    atomic_init(&l->pending_id, 0);
}

static void holding_on_revoked(void *self_, const kafka_List_t *partitions, int64_t callback_id) {
    (void)partitions;
    holding_listener_t *l = (holding_listener_t *)self_;
    atomic_fetch_add(&l->revoked_calls, 1);
    atomic_store(&l->pending_id, callback_id);
}

static void holding_on_assigned(void *self_, const kafka_List_t *partitions, int64_t callback_id) {
    (void)partitions;
    holding_listener_t *l = (holding_listener_t *)self_;
    atomic_fetch_add(&l->assigned_calls, 1);
    atomic_store(&l->pending_id, callback_id);
}

/* Reports the held invocation as a success. */
static void holding_listener_release(holding_listener_t *l) {
    int64_t id = atomic_exchange(&l->pending_id, 0);
    TEST_ASSERT_NOT_EQUAL(0, id);
    kafka_consumer_Consumer__set_callback_result(l->consumer, id, NULL);
}

/* Subscribes `f` to `topic` with a holding listener; the listener handle is
 * destroyed at once, its registration having been copied. */
static void subscribe_holding(fixture_t *f, const char *topic, holding_listener_t *l) {
    holding_listener_init(l, f->consumer);
    kafka_consumer_ConsumerRebalanceListener_t *listener =
        kafka_consumer_ConsumerRebalanceListener_new(l, holding_on_revoked, holding_on_assigned, NULL);
    TEST_ASSERT_NOT_NULL(listener);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)topic);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_topics_listener(f->consumer, topics, listener));
    kafka_List_destroy(topics);
    kafka_consumer_ConsumerRebalanceListener_destroy(listener);
}

/* Starts a `rebalance_cb` to {topic-0} whose listener invocation is queued and,
 * once pumped, held until `holding_listener_release`: from the return of this
 * function until the completion is pumped the consumer has an operation in
 * flight. The caller owns `*out_list` / `*out_tp`. */
static void start_held_rebalance(fixture_t *f, const char *topic, holding_listener_t *l,
                                 completion_t *completion, kafka_List_t **out_list,
                                 kafka_common_TopicPartition_t **out_tp) {
    subscribe_holding(f, topic, l);
    completion_init(completion);
    *out_list = tp_list(topic, 0, out_tp);
    kafka_consumer_MockConsumer_rebalance_cb(f->mock, *out_list, on_void_op, completion);
}

// ---------------------------------------------------------------------------
// Sync poll: add_record -> poll -> records_with_partition -> assert bytes
//
// With no deserializer the record's `void *`s are exactly the `kafka_Bytes_t *`
// pointers `add_record` was given (zero-copy: the mock copies nothing).
// ---------------------------------------------------------------------------

static void test_mock_consumer_sync_poll_returns_record(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    add_record(f.mock, "test", 0, 0, &KEY, &VALUE);

    kafka_consumer_ConsumerRecords_t *records = poll_ok(f.consumer, 100);
    TEST_ASSERT_EQUAL_INT8(0, kafka_consumer_ConsumerRecords_is_empty(records));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));

    /* `partitions()`: an owned list of owned TopicPartitions. */
    kafka_List_t *partitions = kafka_consumer_ConsumerRecords_partitions(records);
    TEST_ASSERT_NOT_NULL(partitions);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(partitions));
    const kafka_common_TopicPartition_t *seen = (const kafka_common_TopicPartition_t *)kafka_List_get(partitions, 0);
    TEST_ASSERT_EQUAL_STRING("test", kafka_common_TopicPartition_topic(seen));
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartition_partition(seen));
    kafka_List_destroy(partitions);

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    const kafka_consumer_ConsumerRecord_t *rec = first_record(records, tp);
    TEST_ASSERT_EQUAL_STRING("test", kafka_consumer_ConsumerRecord_topic(rec));
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_ConsumerRecord_partition(rec));
    TEST_ASSERT_EQUAL_INT64(0, kafka_consumer_ConsumerRecord_offset(rec));

    /* The very same pointers, not copies. */
    TEST_ASSERT_EQUAL_PTR(&KEY, kafka_consumer_ConsumerRecord_key(rec));
    TEST_ASSERT_EQUAL_PTR(&VALUE, kafka_consumer_ConsumerRecord_value(rec));
    const kafka_Bytes_t *got_key = (const kafka_Bytes_t *)kafka_consumer_ConsumerRecord_key(rec);
    TEST_ASSERT_EQUAL_INT32((int32_t)sizeof(KEY_BYTES), got_key->len);
    TEST_ASSERT_EQUAL_MEMORY(KEY_BYTES, got_key->data, sizeof(KEY_BYTES));
    const kafka_Bytes_t *got_value = (const kafka_Bytes_t *)kafka_consumer_ConsumerRecord_value(rec);
    TEST_ASSERT_EQUAL_INT32((int32_t)sizeof(VALUE_BYTES), got_value->len);
    TEST_ASSERT_EQUAL_MEMORY(VALUE_BYTES, got_value->data, sizeof(VALUE_BYTES));

    /* `records(topic)` sees the same record; a partition nobody produced to
     * yields an empty list. */
    kafka_List_t *by_topic = kafka_consumer_ConsumerRecords_records_with_topic(records, "test");
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(by_topic));
    TEST_ASSERT_NULL(kafka_List_get(by_topic, 5)); /* out of range */
    kafka_List_destroy(by_topic);
    kafka_common_TopicPartition_t *other = kafka_common_TopicPartition_new("test", 7);
    kafka_List_t *none = kafka_consumer_ConsumerRecords_records_with_partition(records, other);
    TEST_ASSERT_NOT_NULL(none);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(none));
    kafka_List_destroy(none);
    kafka_common_TopicPartition_destroy(other);

    /* `nextOffsets()`: the position after the offset-0 record is 1 (Java's
     * MockConsumer.poll fills `nextOffsetAndMetadata` per partition). */
    kafka_Map_t *next = kafka_consumer_ConsumerRecords_next_offsets(records);
    TEST_ASSERT_NOT_NULL(next);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(next));
    const kafka_consumer_OffsetAndMetadata_t *next_oam =
        (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_get(next, tp);
    TEST_ASSERT_NOT_NULL(next_oam);
    TEST_ASSERT_EQUAL_INT64(1, kafka_consumer_OffsetAndMetadata_offset(next_oam));
    kafka_Map_destroy(next);

    kafka_consumer_ConsumerRecords_destroy(records);

    /* Nothing left: an empty, non-null batch. */
    records = poll_ok(f.consumer, 10);
    TEST_ASSERT_EQUAL_INT8(1, kafka_consumer_ConsumerRecords_is_empty(records));
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_ConsumerRecords_count(records));
    kafka_consumer_ConsumerRecords_destroy(records);

    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Null key/value round-trip as NULL
// ---------------------------------------------------------------------------

static void test_mock_consumer_null_key_value(void) {
    fixture_t f;
    make_assigned_mock(&f, "t", 0);
    add_record(f.mock, "t", 0, 0, NULL, NULL);

    kafka_consumer_ConsumerRecords_t *records = poll_ok(f.consumer, 100);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("t", 0);
    const kafka_consumer_ConsumerRecord_t *rec = first_record(records, tp);
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecord_key(rec));
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecord_value(rec));
    /* `ConsumerRecord(topic, partition, offset, key, value)` leaves both
     * serialized sizes at NULL_SIZE (-1). */
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_ConsumerRecord_serialized_key_size(rec));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_ConsumerRecord_serialized_value_size(rec));

    kafka_common_TopicPartition_destroy(tp);
    kafka_consumer_ConsumerRecords_destroy(records);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// poll_cb: the completion is queued and runs on the pumping thread
// ---------------------------------------------------------------------------

static void test_mock_consumer_poll_cb(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    add_record(f.mock, "test", 0, 0, NULL, &VALUE);

    completion_t result;
    completion_init(&result);
    kafka_consumer_Consumer_poll_cb(f.consumer, 100, on_poll, &result);

    /* Nothing runs until this thread pumps. */
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));
    TEST_ASSERT_EQUAL_INT32(1, callback_pump_execute(&f.pump));

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT32(1, result.record_count);
    /* The completion ran on the pumping thread: this one. */
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), result.thread_id));

    completion_free(&result);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Single-owner guard: a blocking call while an operation is in flight is
// Java's ConcurrentModificationException
//
// A `rebalance_cb` whose listener holds its report keeps the operation in
// flight deterministically. Meanwhile a blocking call is rejected with
// LocalConcurrentModification, a `_cb` call delivers the same rejection
// through its completion, the sync getters answer empty / -1 / NULL, and the
// always-allowed functions still work. Once the listener reports and the
// completion is pumped the consumer is free again.
// ---------------------------------------------------------------------------

static void test_mock_consumer_concurrency_guard(void) {
    fixture_t f;
    fixture_init(&f);

    holding_listener_t listener;
    completion_t completion;
    kafka_List_t *assignment = NULL;
    kafka_common_TopicPartition_t *tp = NULL;
    start_held_rebalance(&f, "test", &listener, &completion, &assignment, &tp);

    /* In flight from the submission on (the flag is taken before anything is
     * spawned): rejected at once, with the exact Java message. */
    assert_concurrent_modification(kafka_consumer_Consumer_commit_sync(f.consumer));
    assert_concurrent_modification(kafka_consumer_Consumer_unsubscribe(f.consumer));
    kafka_consumer_ConsumerRecords_t *records = (kafka_consumer_ConsumerRecords_t *)&f; /* sentinel */
    assert_concurrent_modification(kafka_consumer_Consumer_poll(f.consumer, 10, &records));
    TEST_ASSERT_EQUAL_PTR(&f, records); /* the out-param is untouched on failure */
    assert_concurrent_modification(kafka_consumer_MockConsumer_rebalance(f.mock, assignment));
    assert_concurrent_modification(kafka_consumer_MockConsumer_set_max_poll_records(f.mock, 5));

    /* The sync getters have no error slot: empty containers, -1, NULL. */
    kafka_List_t *asg = kafka_consumer_Consumer_assignment(f.consumer);
    TEST_ASSERT_NOT_NULL(asg);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(asg));
    kafka_List_destroy(asg);
    kafka_List_t *sub = kafka_consumer_Consumer_subscription(f.consumer);
    TEST_ASSERT_NOT_NULL(sub);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(sub));
    kafka_List_destroy(sub);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_group_metadata(f.consumer));
    TEST_ASSERT_EQUAL_INT8(-1, kafka_consumer_MockConsumer_closed(f.mock));
    TEST_ASSERT_EQUAL_INT8(-1, kafka_consumer_MockConsumer_should_rebalance(f.mock));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_consumer_MockConsumer_last_poll_timeout(f.mock));

    /* Always allowed. */
    TEST_ASSERT_NOT_NULL(kafka_consumer_Consumer_client_id(f.consumer));
    kafka_consumer_ConsumerHandle_t *handle = kafka_consumer_Consumer_handle(f.consumer);
    TEST_ASSERT_NOT_NULL(handle);
    kafka_consumer_ConsumerHandle_destroy(handle);

    /* A `_cb` call is rejected through its completion, queued like any other. */
    completion_t rejected;
    completion_init(&rejected);
    kafka_consumer_Consumer_commit_sync_cb(f.consumer, on_void_op, &rejected);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &rejected.fired, 1));
    TEST_ASSERT_TRUE(rejected.had_error);
    assert_concurrent_modification(rejected.error);
    rejected.error = NULL;

    /* The queued listener invocation runs on the pump too (it may be queued
     * after the rejection: the rebalance task runs concurrently with this
     * thread); once it ran it holds the operation, so the completion cannot
     * have fired. */
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &listener.assigned_calls, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&listener.assigned_calls));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired));

    holding_listener_release(&listener);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_FALSE(completion.had_error);

    /* Free again, and the rebalance did apply. */
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_sync(f.consumer));
    asg = kafka_consumer_Consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    kafka_List_destroy(asg);
    TEST_ASSERT_EQUAL_INT8(0, kafka_consumer_MockConsumer_closed(f.mock));

    kafka_List_destroy(assignment);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// wakeup() bypasses the guard (never rejected) and is callable any time
// ---------------------------------------------------------------------------

static void test_mock_consumer_wakeup_bypasses_guard(void) {
    fixture_t f;
    fixture_init(&f);
    /* The rebalance below assigns test-0; its EARLIEST position needs a
     * beginning offset once the polls reach the fetch-position update. */
    update_offset(f.mock, kafka_consumer_MockConsumer_update_beginning_offsets, "test", 0, 0);

    holding_listener_t listener;
    completion_t completion;
    kafka_List_t *assignment = NULL;
    kafka_common_TopicPartition_t *tp = NULL;
    start_held_rebalance(&f, "test", &listener, &completion, &assignment, &tp);

    /* An operation is in flight (control), yet wakeup() is not rejected: it
     * returns void and arms the mock's wakeup flag. */
    assert_concurrent_modification(kafka_consumer_Consumer_commit_sync(f.consumer));
    kafka_consumer_Consumer_wakeup(f.consumer);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &listener.assigned_calls, 1));
    holding_listener_release(&listener);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_FALSE(completion.had_error);

    /* The rebalance never polls, so nothing consumed the flag: the next poll
     * is interrupted with exactly Wakeup — never a LocalConcurrentModification
     * from a guard the `_cb` operation failed to release. */
    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_poll(f.consumer, 10, &records);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);

    /* The flag was consumed. */
    records = poll_ok(f.consumer, 10);
    kafka_consumer_ConsumerRecords_destroy(records);

    kafka_List_destroy(assignment);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// add_record and position on a non-assigned partition each error, with
// different classes: LocalIllegalState vs LocalIllegalArgument
// ---------------------------------------------------------------------------

static void test_mock_consumer_add_record_unassigned_errors(void) {
    fixture_t f;
    fixture_init(&f);

    // No assignment yet -> add_record fails with LocalIllegalState, the same
    // class the Rust twin asserts (Java throws IllegalStateException).
    kafka_consumer_ConsumerRecord_t *record = kafka_consumer_ConsumerRecord_new("test", 0, 0, NULL, NULL);
    kafka_common_Error_t *err = kafka_consumer_MockConsumer_add_record(f.mock, record);
    kafka_consumer_ConsumerRecord_destroy(record);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("Cannot add records for a partition that is not assigned to the consumer",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    // `position` on the same unassigned partition fails with the sibling
    // LocalIllegalArgument instead, matching the granularity the Rust tests
    // assert with `matches!(err, Error::LocalIllegalArgument(_))`.
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    int64_t pos = -7;
    kafka_common_Error_t *pos_err = kafka_consumer_Consumer_position(f.consumer, tp, &pos);
    TEST_ASSERT_NOT_NULL(pos_err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(pos_err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_local_illegal_state_error(pos_err));
    TEST_ASSERT_EQUAL_STRING("You can only check the position for partitions assigned to this consumer.",
                             kafka_common_Error_message(pos_err));
    TEST_ASSERT_EQUAL_INT64(-7, pos); /* untouched on failure */
    kafka_common_Error_destroy(pos_err);

    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// A negative Duration is rejected before anything runs
// ---------------------------------------------------------------------------

static void test_mock_consumer_negative_timeout_rejected(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);

    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_poll(f.consumer, -1, &records);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("Timeout must not be negative", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* The mock never saw the call. */
    TEST_ASSERT_EQUAL_INT64(-1, kafka_consumer_MockConsumer_last_poll_timeout(f.mock));

    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// subscribe / subscription / unsubscribe
// ---------------------------------------------------------------------------

static void test_mock_consumer_subscribe_subscription(void) {
    fixture_t f;
    fixture_init(&f);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"beta");
    kafka_List_add(topics, (void *)"alpha");
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_topics(f.consumer, topics));
    kafka_List_destroy(topics);

    /* An owned, sorted list of owned strings. */
    kafka_List_t *subs = kafka_consumer_Consumer_subscription(f.consumer);
    TEST_ASSERT_NOT_NULL(subs);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(subs));
    TEST_ASSERT_EQUAL_STRING("alpha", (const char *)kafka_List_get(subs, 0));
    TEST_ASSERT_EQUAL_STRING("beta", (const char *)kafka_List_get(subs, 1));
    kafka_List_destroy(subs);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_unsubscribe(f.consumer));
    subs = kafka_consumer_Consumer_subscription(f.consumer);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(subs));
    kafka_List_destroy(subs);

    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// subscribe_with_pattern: the SubscriptionPattern round-trips
// ---------------------------------------------------------------------------

static void test_mock_consumer_subscribe_with_pattern(void) {
    fixture_t f;
    fixture_init(&f);

    kafka_consumer_SubscriptionPattern_t *pattern = kafka_consumer_SubscriptionPattern_new("topic-.*");
    TEST_ASSERT_NOT_NULL(pattern);
    TEST_ASSERT_EQUAL_STRING("topic-.*", kafka_consumer_SubscriptionPattern_pattern(pattern));
    char *s = kafka_consumer_SubscriptionPattern_to_string(pattern);
    TEST_ASSERT_NOT_NULL(s);
    kafka_string_destroy(s);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_pattern(f.consumer, pattern));
    kafka_consumer_SubscriptionPattern_destroy(pattern);

    /* A pattern subscription names no topics until the broker resolves it. */
    kafka_List_t *subs = kafka_consumer_Consumer_subscription(f.consumer);
    TEST_ASSERT_NOT_NULL(subs);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(subs));
    kafka_List_destroy(subs);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_unsubscribe(f.consumer));

    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// assignment reflects assign()
// ---------------------------------------------------------------------------

static void test_mock_consumer_assignment(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 3);
    kafka_List_t *as = kafka_consumer_Consumer_assignment(f.consumer);
    TEST_ASSERT_NOT_NULL(as);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(as));
    const kafka_common_TopicPartition_t *tp = (const kafka_common_TopicPartition_t *)kafka_List_get(as, 0);
    TEST_ASSERT_EQUAL_STRING("test", kafka_common_TopicPartition_topic(tp));
    TEST_ASSERT_EQUAL_INT32(3, kafka_common_TopicPartition_partition(tp));
    kafka_List_destroy(as);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// seek + position round-trip
// ---------------------------------------------------------------------------

static void test_mock_consumer_seek_position(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_seek_with_offset(f.consumer, tp, 42));
    int64_t pos = -1;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_position(f.consumer, tp, &pos));
    TEST_ASSERT_EQUAL_INT64(42, pos);

    // seek(TopicPartition, OffsetAndMetadata) also sets the position.
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_with_metadata(100, "meta", &oam));
    TEST_ASSERT_NULL(kafka_consumer_Consumer_seek_with_offset_and_metadata(f.consumer, tp, oam));
    kafka_consumer_OffsetAndMetadata_destroy(oam);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_position_with_timeout(f.consumer, tp, 100, &pos));
    TEST_ASSERT_EQUAL_INT64(100, pos);

    /* seekToBeginning / seekToEnd use the offsets the mock was given. */
    kafka_List_t *list = kafka_List_new();
    kafka_List_add(list, tp);
    update_offset(f.mock, kafka_consumer_MockConsumer_update_end_offsets, "test", 0, 55);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_seek_to_end(f.consumer, list));
    TEST_ASSERT_NULL(kafka_consumer_Consumer_position(f.consumer, tp, &pos));
    TEST_ASSERT_EQUAL_INT64(55, pos);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_seek_to_beginning(f.consumer, list));
    TEST_ASSERT_NULL(kafka_consumer_Consumer_position(f.consumer, tp, &pos));
    TEST_ASSERT_EQUAL_INT64(0, pos);
    kafka_List_destroy(list);

    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// commit_sync_with_offsets + committed round-trip
// ---------------------------------------------------------------------------

static void test_mock_consumer_commit_committed(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    kafka_common_TopicPartition_t *tp = NULL;
    kafka_List_t *list = tp_list("test", 0, &tp);

    /* A C-built offsets map borrows its elements. */
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_with_metadata(7, "checkpoint", &oam));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, tp, oam);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_sync_with_offsets(f.consumer, offsets));
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);

    /* `committed(Set)`: an owned map of owned TopicPartition -> owned
     * OffsetAndMetadata, whose `get` compares keys by content. */
    kafka_Map_t *committed = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_committed(f.consumer, list, &committed));
    TEST_ASSERT_NOT_NULL(committed);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(committed));
    const kafka_common_TopicPartition_t *k = (const kafka_common_TopicPartition_t *)kafka_Map_key(committed, 0);
    TEST_ASSERT_EQUAL_STRING("test", kafka_common_TopicPartition_topic(k));
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartition_partition(k));
    const kafka_consumer_OffsetAndMetadata_t *v =
        (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_get(committed, tp);
    TEST_ASSERT_NOT_NULL(v);
    TEST_ASSERT_EQUAL_PTR(kafka_Map_value(committed, 0), v);
    TEST_ASSERT_EQUAL_INT64(7, kafka_consumer_OffsetAndMetadata_offset(v));
    TEST_ASSERT_EQUAL_STRING("checkpoint", kafka_consumer_OffsetAndMetadata_metadata(v));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_OffsetAndMetadata_leader_epoch(v)); /* none */
    char *s = kafka_consumer_OffsetAndMetadata_to_string(v);
    TEST_ASSERT_NOT_NULL(s);
    kafka_string_destroy(s);
    kafka_Map_destroy(committed);

    /* The timed forms and the no-offsets commits just succeed on the mock. */
    TEST_ASSERT_NULL(kafka_consumer_Consumer_committed_with_timeout(f.consumer, list, 100, &committed));
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(committed));
    kafka_Map_destroy(committed);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_sync(f.consumer));
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_sync_with_timeout(f.consumer, 100));
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_async(f.consumer));

    /* A negative offset is rejected when the OffsetAndMetadata is built. */
    kafka_consumer_OffsetAndMetadata_t *bad = NULL;
    kafka_common_Error_t *err = kafka_consumer_OffsetAndMetadata_new(-5, &bad);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(bad);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "negative offset"));
    kafka_common_Error_destroy(err);

    kafka_List_destroy(list);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// beginning / end offsets
// ---------------------------------------------------------------------------

static void test_mock_consumer_beginning_end_offsets(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    update_offset(f.mock, kafka_consumer_MockConsumer_update_end_offsets, "test", 0, 55);
    kafka_common_TopicPartition_t *tp = NULL;
    kafka_List_t *list = tp_list("test", 0, &tp);

    /* Owned maps of owned TopicPartition -> owned int64_t. */
    kafka_Map_t *begin = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_beginning_offsets(f.consumer, list, &begin));
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(begin));
    const int64_t *b = (const int64_t *)kafka_Map_get(begin, tp);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT64(0, *b);
    kafka_Map_destroy(begin);

    kafka_Map_t *end = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_end_offsets_with_timeout(f.consumer, list, 100, &end));
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(end));
    const kafka_common_TopicPartition_t *k = (const kafka_common_TopicPartition_t *)kafka_Map_key(end, 0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartition_partition(k));
    TEST_ASSERT_EQUAL_INT64(55, *(const int64_t *)kafka_Map_value(end, 0));
    kafka_Map_destroy(end);

    /* `setOffsetsException`: the next offsets query fails with the injected
     * error, which the mock takes over and consumes. */
    kafka_consumer_MockConsumer_set_offsets_error(f.mock, kafka_common_Error_kafka_message("offsets boom"));
    begin = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_beginning_offsets(f.consumer, list, &begin);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(begin);
    TEST_ASSERT_EQUAL_STRING("offsets boom", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_beginning_offsets(f.consumer, list, &begin));
    kafka_Map_destroy(begin);

    kafka_List_destroy(list);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// pause / resume / paused
// ---------------------------------------------------------------------------

static void test_mock_consumer_pause_resume(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    kafka_common_TopicPartition_t *tp = NULL;
    kafka_List_t *list = tp_list("test", 0, &tp);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_pause(f.consumer, list));
    kafka_List_t *paused = kafka_consumer_Consumer_paused(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(paused));
    kafka_List_destroy(paused);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_resume(f.consumer, list));
    paused = kafka_consumer_Consumer_paused(f.consumer);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(paused));
    kafka_List_destroy(paused);

    kafka_List_destroy(list);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// group_metadata
// ---------------------------------------------------------------------------

static void test_mock_consumer_group_metadata(void) {
    fixture_t f;
    fixture_init(&f);
    kafka_consumer_ConsumerGroupMetadata_t *meta = kafka_consumer_Consumer_group_metadata(f.consumer);
    TEST_ASSERT_NOT_NULL(meta);
    // The group id is a non-null C string; member id is too.
    TEST_ASSERT_NOT_NULL(kafka_consumer_ConsumerGroupMetadata_group_id(meta));
    TEST_ASSERT_NOT_NULL(kafka_consumer_ConsumerGroupMetadata_member_id(meta));
    kafka_consumer_ConsumerGroupMetadata_destroy(meta);
    fixture_destroy(&f);
}

// The group-metadata handle's four invokers read back the consumer's values:
// MockConsumer reports Java's MockConsumer.groupMetadata() fields, a dynamic
// member with no group instance id (null). The handle caches NUL-terminated
// copies of the strings, so it stays readable after the consumer is destroyed.
static void test_consumer_group_metadata_accessors(void) {
    fixture_t f;
    fixture_init(&f);
    kafka_consumer_ConsumerGroupMetadata_t *meta = kafka_consumer_Consumer_group_metadata(f.consumer);
    TEST_ASSERT_NOT_NULL(meta);
    fixture_destroy(&f);

    TEST_ASSERT_EQUAL_STRING("dummy.group.id", kafka_consumer_ConsumerGroupMetadata_group_id(meta));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerGroupMetadata_generation_id(meta));
    TEST_ASSERT_EQUAL_STRING("1", kafka_consumer_ConsumerGroupMetadata_member_id(meta));
    TEST_ASSERT_NULL(kafka_consumer_ConsumerGroupMetadata_group_instance_id(meta));
    kafka_consumer_ConsumerGroupMetadata_destroy(meta);
}

/* A C implementation of the interface (rule 3): the invokers call its
 * function pointers with `self`. */
typedef struct {
    const char *group_id;
    int32_t generation_id;
} c_group_metadata_t;

static const char *c_group_id(void *self_) { return ((c_group_metadata_t *)self_)->group_id; }
static int32_t c_generation_id(void *self_) { return ((c_group_metadata_t *)self_)->generation_id; }
static const char *c_member_id(void *self_) { (void)self_; return "member-7"; }
static const char *c_group_instance_id(void *self_) { (void)self_; return NULL; }

static void test_consumer_group_metadata_c_implementation(void) {
    c_group_metadata_t impl = { "c-group", 9 };
    kafka_consumer_ConsumerGroupMetadata_t *meta = kafka_consumer_ConsumerGroupMetadata_new(
        &impl, c_group_id, c_generation_id, c_member_id, c_group_instance_id);
    TEST_ASSERT_NOT_NULL(meta);
    TEST_ASSERT_EQUAL_STRING("c-group", kafka_consumer_ConsumerGroupMetadata_group_id(meta));
    TEST_ASSERT_EQUAL_INT32(9, kafka_consumer_ConsumerGroupMetadata_generation_id(meta));
    TEST_ASSERT_EQUAL_STRING("member-7", kafka_consumer_ConsumerGroupMetadata_member_id(meta));
    TEST_ASSERT_NULL(kafka_consumer_ConsumerGroupMetadata_group_instance_id(meta));
    kafka_consumer_ConsumerGroupMetadata_destroy(meta);
}

// ---------------------------------------------------------------------------
// partitions_for / list_topics + Node getters
// ---------------------------------------------------------------------------

static void test_mock_consumer_partitions_and_topics(void) {
    fixture_t f;
    fixture_init(&f);

    // 2 partitions for "test", leader node id=1 host="broker1" port=9092. The
    // PartitionInfos are copied by `updatePartitions`, so they (and the nodes
    // they borrow) are freed right after.
    kafka_common_Node_t *leader = kafka_common_Node_new(1, "broker1", 9092);
    kafka_List_t *replicas = kafka_List_new();
    kafka_List_add(replicas, leader);
    kafka_List_t *infos = kafka_List_new();
    for (int32_t p = 0; p < 2; p++) {
        kafka_List_add(infos, kafka_common_PartitionInfo_new("test", p, leader, replicas, replicas));
    }
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_partitions(f.mock, "test", infos));
    for (int32_t i = 0; i < kafka_List_size(infos); i++) {
        kafka_common_PartitionInfo_destroy((kafka_common_PartitionInfo_t *)kafka_List_get(infos, i));
    }
    kafka_List_destroy(infos);
    kafka_List_destroy(replicas);
    kafka_common_Node_destroy(leader);

    /* `partitionsFor`: an owned list of owned PartitionInfos. */
    kafka_List_t *got = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_partitions_for(f.consumer, "test", &got));
    TEST_ASSERT_NOT_NULL(got);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(got));
    const kafka_common_PartitionInfo_t *p0 = (const kafka_common_PartitionInfo_t *)kafka_List_get(got, 0);
    TEST_ASSERT_EQUAL_STRING("test", kafka_common_PartitionInfo_topic(p0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_PartitionInfo_partition(p0));
    const kafka_common_Node_t *got_leader = kafka_common_PartitionInfo_leader(p0);
    TEST_ASSERT_NOT_NULL(got_leader);
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_Node_id(got_leader));
    TEST_ASSERT_EQUAL_INT32(9092, kafka_common_Node_port(got_leader));
    TEST_ASSERT_EQUAL_STRING("broker1", kafka_common_Node_host(got_leader));
    kafka_List_t *got_replicas = kafka_common_PartitionInfo_replicas(p0);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(got_replicas));
    TEST_ASSERT_NOT_NULL(kafka_List_get(got_replicas, 0));
    TEST_ASSERT_NULL(kafka_List_get(got_replicas, 5));
    kafka_List_destroy(got_replicas);
    kafka_List_destroy(got);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_partitions_for_with_timeout(f.consumer, "test", 100, &got));
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(got));
    kafka_List_destroy(got);

    /* `listTopics`: an owned map of owned string -> owned list, keyed by
     * content. */
    kafka_Map_t *map = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_list_topics(f.consumer, &map));
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(map));
    TEST_ASSERT_EQUAL_STRING("test", (const char *)kafka_Map_key(map, 0));
    const kafka_List_t *plist = (const kafka_List_t *)kafka_Map_get(map, (void *)"test");
    TEST_ASSERT_NOT_NULL(plist);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(plist));
    kafka_Map_destroy(map);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_list_topics_with_timeout(f.consumer, 100, &map));
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(map));
    kafka_Map_destroy(map);

    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// Record headers + timestamp type + serialized sizes
// ---------------------------------------------------------------------------

static void test_mock_consumer_record_metadata_getters(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    add_record(f.mock, "test", 0, 0, NULL, &VALUE);

    kafka_consumer_ConsumerRecords_t *records = poll_ok(f.consumer, 100);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    const kafka_consumer_ConsumerRecord_t *rec = first_record(records, tp);

    // No headers were attached: a borrowed, empty RecordHeaders.
    const kafka_common_header_internals_RecordHeaders_t *headers = kafka_consumer_ConsumerRecord_headers(rec);
    TEST_ASSERT_NOT_NULL(headers);
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_header_internals_RecordHeaders_is_read_only(headers));

    // Mock records are built via ConsumerRecord(topic, partition, offset, key,
    // value), which leaves both serialized sizes at NULL_SIZE (-1), the
    // timestamp at NO_TIMESTAMP (-1) with NO_TIMESTAMP_TYPE, and no leader
    // epoch / delivery count; the broker fetch path sets them.
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_ConsumerRecord_serialized_key_size(rec));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_ConsumerRecord_serialized_value_size(rec));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_consumer_ConsumerRecord_timestamp(rec));
    const kafka_common_record_TimestampType_t *tt = kafka_consumer_ConsumerRecord_timestamp_type(rec);
    TEST_ASSERT_NOT_NULL(tt);
    TEST_ASSERT_EQUAL_PTR(kafka_common_record_TimestampType_no_timestamp_type(), tt); /* a singleton */
    TEST_ASSERT_EQUAL_INT(kafka_common_record_TimestampType_NO_TIMESTAMP_TYPE,
                          kafka_common_record_TimestampType__enum(tt));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_ConsumerRecord_leader_epoch(rec));
    TEST_ASSERT_EQUAL_INT16(-1, kafka_consumer_ConsumerRecord_delivery_count(rec));

    char *s = kafka_consumer_ConsumerRecord_to_string(rec);
    TEST_ASSERT_NOT_NULL(s);
    TEST_ASSERT_NOT_NULL(strstr(s, "test"));
    kafka_string_destroy(s);

    kafka_common_TopicPartition_destroy(tp);
    kafka_consumer_ConsumerRecords_destroy(records);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// ConsumerRecord built through the options builder keeps every field
// ---------------------------------------------------------------------------

static void test_consumer_record_with_options(void) {
    kafka_consumer_ConsumerRecordOptionsBuilder_t *b = kafka_consumer_ConsumerRecordOptionsBuilder_new();
    TEST_ASSERT_NOT_NULL(b);

    /* The builder requires topic, partition, offset, key and value: a build
     * with a missing mandatory field is an IllegalArgument naming it. A build
     * consumes the builder (a second one is an IllegalState), so the
     * successful build below uses a fresh one. */
    kafka_consumer_ConsumerRecordOptions_t *options = NULL;
    kafka_common_Error_t *err = kafka_consumer_ConsumerRecordOptionsBuilder_build(b, &options);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(options);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("ConsumerRecordOptionsBuilder::build: mandatory parameter `topic` was not set",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    err = kafka_consumer_ConsumerRecordOptionsBuilder_build(b, &options);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("ConsumerRecordOptionsBuilder already built", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_consumer_ConsumerRecordOptionsBuilder_destroy(b);

    b = kafka_consumer_ConsumerRecordOptionsBuilder_new();
    kafka_consumer_ConsumerRecordOptionsBuilder_set_topic(b, "opts");
    kafka_consumer_ConsumerRecordOptionsBuilder_set_partition(b, 3);
    kafka_consumer_ConsumerRecordOptionsBuilder_set_offset(b, 77);
    kafka_consumer_ConsumerRecordOptionsBuilder_set_timestamp(b, 123456);
    kafka_consumer_ConsumerRecordOptionsBuilder_set_timestamp_type(b, kafka_common_record_TimestampType_create_time());
    kafka_consumer_ConsumerRecordOptionsBuilder_set_serialized_key_size(b, 3);
    kafka_consumer_ConsumerRecordOptionsBuilder_set_serialized_value_size(b, 3);
    kafka_consumer_ConsumerRecordOptionsBuilder_set_key(b, &KEY);
    kafka_consumer_ConsumerRecordOptionsBuilder_set_value(b, NULL); /* Java null */
    kafka_consumer_ConsumerRecordOptionsBuilder_set_leader_epoch(b, 5);
    kafka_consumer_ConsumerRecordOptionsBuilder_set_delivery_count(b, 2);
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecordOptionsBuilder_build(b, &options));
    TEST_ASSERT_NOT_NULL(options);
    kafka_consumer_ConsumerRecordOptionsBuilder_destroy(b);

    kafka_consumer_ConsumerRecord_t *rec = kafka_consumer_ConsumerRecord_with_options(options);
    kafka_consumer_ConsumerRecordOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(rec);
    TEST_ASSERT_EQUAL_STRING("opts", kafka_consumer_ConsumerRecord_topic(rec));
    TEST_ASSERT_EQUAL_INT32(3, kafka_consumer_ConsumerRecord_partition(rec));
    TEST_ASSERT_EQUAL_INT64(77, kafka_consumer_ConsumerRecord_offset(rec));
    TEST_ASSERT_EQUAL_INT64(123456, kafka_consumer_ConsumerRecord_timestamp(rec));
    TEST_ASSERT_EQUAL_INT(kafka_common_record_TimestampType_CREATE_TIME,
                          kafka_common_record_TimestampType__enum(kafka_consumer_ConsumerRecord_timestamp_type(rec)));
    TEST_ASSERT_EQUAL_INT32(3, kafka_consumer_ConsumerRecord_serialized_key_size(rec));
    TEST_ASSERT_EQUAL_INT32(3, kafka_consumer_ConsumerRecord_serialized_value_size(rec));
    TEST_ASSERT_EQUAL_PTR(&KEY, kafka_consumer_ConsumerRecord_key(rec));
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRecord_value(rec));
    TEST_ASSERT_EQUAL_INT32(5, kafka_consumer_ConsumerRecord_leader_epoch(rec));
    TEST_ASSERT_EQUAL_INT16(2, kafka_consumer_ConsumerRecord_delivery_count(rec));
    kafka_consumer_ConsumerRecord_destroy(rec);
}

// ---------------------------------------------------------------------------
// set_poll_error: the injected error surfaces from the next poll
// ---------------------------------------------------------------------------

static void test_mock_consumer_poll_error(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    /* Ownership of the error moves to the mock. */
    kafka_consumer_MockConsumer_set_poll_error(f.mock, kafka_common_Error_kafka_message("boom"));

    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *poll_err = kafka_consumer_Consumer_poll(f.consumer, 10, &records);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_NOT_NULL(poll_err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_error(poll_err));
    TEST_ASSERT_EQUAL_STRING("boom", kafka_common_Error_message(poll_err));
    kafka_common_Error_destroy(poll_err);

    // The error is consumed; a subsequent poll succeeds.
    records = poll_ok(f.consumer, 10);
    kafka_consumer_ConsumerRecords_destroy(records);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// setMaxPollRecords caps a poll; lastPollTimeout records what poll was given
// ---------------------------------------------------------------------------

static void test_mock_consumer_max_poll_records_and_last_poll_timeout(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    TEST_ASSERT_EQUAL_INT64(-1, kafka_consumer_MockConsumer_last_poll_timeout(f.mock)); /* never polled */
    for (int64_t offset = 0; offset < 3; offset++) {
        add_record(f.mock, "test", 0, offset, NULL, &VALUE);
    }
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_set_max_poll_records(f.mock, 2));

    kafka_consumer_ConsumerRecords_t *records = poll_ok(f.consumer, 250);
    TEST_ASSERT_EQUAL_INT32(2, kafka_consumer_ConsumerRecords_count(records));
    kafka_consumer_ConsumerRecords_destroy(records);
    TEST_ASSERT_EQUAL_INT64(250, kafka_consumer_MockConsumer_last_poll_timeout(f.mock));

    records = poll_ok(f.consumer, 0);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));
    kafka_consumer_ConsumerRecords_destroy(records);
    TEST_ASSERT_EQUAL_INT64(0, kafka_consumer_MockConsumer_last_poll_timeout(f.mock));

    /* shouldRebalance is a plain flag the test code drives. */
    TEST_ASSERT_EQUAL_INT8(0, kafka_consumer_MockConsumer_should_rebalance(f.mock));
    kafka_consumer_MockConsumer_reset_should_rebalance(f.mock);
    TEST_ASSERT_EQUAL_INT8(0, kafka_consumer_MockConsumer_should_rebalance(f.mock));

    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// subscribe_with_topics_cb: the completion is queued for the pump
// ---------------------------------------------------------------------------

static void test_mock_consumer_subscribe_cb(void) {
    fixture_t f;
    fixture_init(&f);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"cb-topic");

    completion_t result;
    completion_init(&result);
    kafka_consumer_Consumer_subscribe_with_topics_cb(f.consumer, topics, on_void_op, &result);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), result.thread_id));
    kafka_List_destroy(topics);

    kafka_List_t *subs = kafka_consumer_Consumer_subscription(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(subs));
    TEST_ASSERT_EQUAL_STRING("cb-topic", (const char *)kafka_List_get(subs, 0));
    kafka_List_destroy(subs);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// The notify hook fires once per empty -> non-empty transition and
// execute_callbacks returns how many ran
//
// The shared queue's "several completions queued while non-empty fire one
// notify" case is pinned by test_mock_producer.c's
// `test_callbacks_notify_fires_once_per_transition`; here each poll_cb is
// pumped before the next, so every submission is its own transition.
// ---------------------------------------------------------------------------

static void test_mock_consumer_callbacks_notify_once_per_transition(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&f.pump.notified));

    completion_t first, second;
    completion_init(&first);
    completion_init(&second);

    kafka_consumer_Consumer_poll_cb(f.consumer, 10, on_poll, &first);
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&f.pump.notified));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&first.fired)); /* announced, not run */
    TEST_ASSERT_EQUAL_INT32(1, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&first.fired));
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump)); /* drained */

    /* Empty -> non-empty again: a second notify. */
    kafka_consumer_Consumer_poll_cb(f.consumer, 10, on_poll, &second);
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&f.pump.notified));
    TEST_ASSERT_EQUAL_INT32(1, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&second.fired));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&f.pump.executed));

    completion_free(&first);
    completion_free(&second);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// destroy runs a never-pumped completion exactly once
// ---------------------------------------------------------------------------

static void test_mock_consumer_destroy_runs_pending_completion_once(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);

    completion_t result;
    completion_init(&result);
    kafka_consumer_Consumer_poll_cb(f.consumer, 10, on_poll, &result);
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));

    /* Never pumped: `destroy` runs it, on the destroying thread. */
    fixture_destroy(&f);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), result.thread_id));
    completion_free(&result);
}

// ---------------------------------------------------------------------------
// client_id is a borrowed string that lives with the handle
// ---------------------------------------------------------------------------

static void test_mock_consumer_client_id(void) {
    fixture_t f;
    fixture_init(&f);
    const char *id = kafka_consumer_Consumer_client_id(f.consumer);
    TEST_ASSERT_NOT_NULL(id);
    TEST_ASSERT_TRUE(strlen(id) > 0);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// current_lag is -1 when unknown; metrics is a map
// ---------------------------------------------------------------------------

static void test_mock_consumer_current_lag_and_metrics(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    /* The Rust MockConsumer's documented model: an assigned partition with
     * no end offset known is "caught up" (0); with one it is end - position;
     * an unassigned partition has no lag (-1 for Java's empty OptionalLong). */
    TEST_ASSERT_EQUAL_INT64(0, kafka_consumer_Consumer_current_lag(f.consumer, tp));
    update_offset(f.mock, kafka_consumer_MockConsumer_update_end_offsets, "test", 0, 55);
    kafka_consumer_ConsumerRecords_t *records = poll_ok(f.consumer, 10); /* positions test-0 at 0 */
    kafka_consumer_ConsumerRecords_destroy(records);
    TEST_ASSERT_EQUAL_INT64(55, kafka_consumer_Consumer_current_lag(f.consumer, tp));
    kafka_common_TopicPartition_destroy(tp);
    kafka_common_TopicPartition_t *other = kafka_common_TopicPartition_new("test", 9);
    TEST_ASSERT_EQUAL_INT64(-1, kafka_consumer_Consumer_current_lag(f.consumer, other));
    kafka_common_TopicPartition_destroy(other);

    kafka_Map_t *metrics = kafka_consumer_Consumer_metrics(f.consumer);
    TEST_ASSERT_NOT_NULL(metrics);
    kafka_Map_destroy(metrics);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// close then destroy; CloseOptions
// ---------------------------------------------------------------------------

static void test_mock_consumer_close(void) {
    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    TEST_ASSERT_EQUAL_INT8(0, kafka_consumer_MockConsumer_closed(f.mock));
    TEST_ASSERT_NULL(kafka_consumer_Consumer_close(f.consumer));
    TEST_ASSERT_EQUAL_INT8(1, kafka_consumer_MockConsumer_closed(f.mock));

    // After close, a mock driver op (update_partitions) errors.
    kafka_List_t *infos = kafka_List_new();
    kafka_common_Error_t *err = kafka_consumer_MockConsumer_update_partitions(f.mock, "test", infos);
    kafka_List_destroy(infos);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("This consumer has already been closed.", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    fixture_destroy(&f);
}

static void test_mock_consumer_close_with_options(void) {
    kafka_consumer_CloseOptions_t *options = kafka_consumer_CloseOptions_new_with_timeout(100);
    TEST_ASSERT_NOT_NULL(options);
    TEST_ASSERT_EQUAL_INT64(100, kafka_consumer_CloseOptions_timeout(options));
    /* The enum singletons compare with `==`. */
    TEST_ASSERT_EQUAL_PTR(kafka_consumer_CloseOptions_GroupMembershipOperation_default(),
                          kafka_consumer_CloseOptions_group_membership_operation(options));
    kafka_consumer_CloseOptions_with_group_membership_operation(
        options, kafka_consumer_CloseOptions_GroupMembershipOperation_leave_group());
    TEST_ASSERT_EQUAL_INT(kafka_consumer_CloseOptions_GroupMembershipOperation_LEAVE_GROUP,
                          kafka_consumer_CloseOptions_GroupMembershipOperation__enum(
                              kafka_consumer_CloseOptions_group_membership_operation(options)));

    fixture_t f;
    make_assigned_mock(&f, "test", 0);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_close_with_options(f.consumer, options));
    TEST_ASSERT_EQUAL_INT8(1, kafka_consumer_MockConsumer_closed(f.mock));
    kafka_consumer_CloseOptions_destroy(options);
    fixture_destroy(&f);

    /* The operation-first constructor. */
    options = kafka_consumer_CloseOptions_with_operation(
        kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group());
    TEST_ASSERT_EQUAL_PTR(kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group(),
                          kafka_consumer_CloseOptions_group_membership_operation(options));
    kafka_consumer_CloseOptions_with_timeout(options, 5);
    TEST_ASSERT_EQUAL_INT64(5, kafka_consumer_CloseOptions_timeout(options));
    kafka_consumer_CloseOptions_destroy(options);

    /* A `_cb` close completes through the pump too. */
    make_assigned_mock(&f, "test", 0);
    completion_t result;
    completion_init(&result);
    kafka_consumer_Consumer_close_cb(f.consumer, on_void_op, &result);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT8(1, kafka_consumer_MockConsumer_closed(f.mock));
    fixture_destroy(&f);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_mock_consumer_sync_poll_returns_record);
    RUN_TEST(test_mock_consumer_null_key_value);
    RUN_TEST(test_mock_consumer_poll_cb);
    RUN_TEST(test_mock_consumer_concurrency_guard);
    RUN_TEST(test_mock_consumer_wakeup_bypasses_guard);
    RUN_TEST(test_mock_consumer_add_record_unassigned_errors);
    RUN_TEST(test_mock_consumer_negative_timeout_rejected);
    RUN_TEST(test_mock_consumer_subscribe_subscription);
    RUN_TEST(test_mock_consumer_subscribe_with_pattern);
    RUN_TEST(test_mock_consumer_assignment);
    RUN_TEST(test_mock_consumer_seek_position);
    RUN_TEST(test_mock_consumer_commit_committed);
    RUN_TEST(test_mock_consumer_beginning_end_offsets);
    RUN_TEST(test_mock_consumer_pause_resume);
    RUN_TEST(test_mock_consumer_group_metadata);
    RUN_TEST(test_consumer_group_metadata_accessors);
    RUN_TEST(test_consumer_group_metadata_c_implementation);
    RUN_TEST(test_mock_consumer_partitions_and_topics);
    RUN_TEST(test_mock_consumer_record_metadata_getters);
    RUN_TEST(test_consumer_record_with_options);
    RUN_TEST(test_mock_consumer_poll_error);
    RUN_TEST(test_mock_consumer_max_poll_records_and_last_poll_timeout);
    RUN_TEST(test_mock_consumer_subscribe_cb);
    RUN_TEST(test_mock_consumer_callbacks_notify_once_per_transition);
    RUN_TEST(test_mock_consumer_destroy_runs_pending_completion_once);
    RUN_TEST(test_mock_consumer_client_id);
    RUN_TEST(test_mock_consumer_current_lag_and_metrics);
    RUN_TEST(test_mock_consumer_close);
    RUN_TEST(test_mock_consumer_close_with_options);
    return UNITY_END();
}
