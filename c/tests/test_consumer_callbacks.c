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

// The consumer's callback interfaces through the C API: the
// `ConsumerRebalanceListener` and `OffsetCommitCallback` registrations
// (CLAUDE.md §4, "Traits"), their `callback_id` reporting protocol, the
// thread each one runs on, and the `ConsumerHandle` a listener calls the
// consumer back through (consumer-threading.md §31, §41).
//
// The thread contract (CLAUDE.md §4, "Async variants of blocking methods"):
// a blocking entry point (`MockConsumer_rebalance`, `commit_async_with_callback`)
// invokes the interface methods directly on the calling thread; a `_cb` entry
// point queues them on the consumer's callbacks vector, run by whoever calls
// `kafka_consumer_Consumer_execute_callbacks`; `_destroy` runs what is still
// queued exactly once. An interface method reports its outcome through
// `kafka_consumer_Consumer__set_callback_result`, inline or later from any
// thread; the operation that invoked it does not complete until it does.
//
// Unity's asserts are not thread-safe, so the probes only record what they
// observe (thread ids via `pthread_self`, counters, snapshots) and the test
// body asserts on the main thread.

#include <confluent_kafka.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include "unity.h"
#include "test_support.h"

void setUp(void) {}
void tearDown(void) {}

static void sleep_ms(long ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_MockConsumer_t *mock;
    kafka_consumer_Consumer_t *consumer;
    callback_pump_t pump;
} fixture_t;

static void fixture_init(fixture_t *f) {
    f->mock = NULL;
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_new("earliest", &f->mock));
    f->consumer = kafka_consumer_MockConsumer__as_Consumer(f->mock);
    TEST_ASSERT_NOT_NULL(f->consumer);
    consumer_callback_pump_install(&f->pump, f->consumer);
}

static void fixture_destroy(fixture_t *f) {
    kafka_consumer_MockConsumer_destroy(f->mock);
    callback_pump_destroy(&f->pump);
}

/* A list of `count` partitions of `topic` (0..count-1); the caller frees the
 * pairs through `tp_list_destroy`. */
static kafka_List_t *tp_list(const char *topic, int32_t count) {
    kafka_List_t *list = kafka_List_new();
    for (int32_t p = 0; p < count; p++) {
        kafka_List_add(list, kafka_common_TopicPartition_new(topic, p));
    }
    return list;
}

static void tp_list_destroy(kafka_List_t *list) {
    for (int32_t i = 0; i < kafka_List_size(list); i++) {
        kafka_common_TopicPartition_destroy((kafka_common_TopicPartition_t *)kafka_List_get(list, i));
    }
    kafka_List_destroy(list);
}

/* `assign(singleton(topic-partition))`. */
static kafka_common_Error_t *assign_one(kafka_consumer_Consumer_t *c, const char *topic, int32_t partition) {
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new(topic, partition);
    kafka_List_t *list = kafka_List_new();
    kafka_List_add(list, tp);
    kafka_common_Error_t *err = kafka_consumer_Consumer_assign(c, list);
    kafka_List_destroy(list);
    kafka_common_TopicPartition_destroy(tp);
    return err;
}

static void update_beginning_offset(kafka_consumer_MockConsumer_t *mock, const char *topic, int32_t partition) {
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new(topic, partition);
    int64_t zero = 0;
    kafka_Map_t *map = kafka_Map_new();
    kafka_Map_put(map, tp, &zero);
    kafka_consumer_MockConsumer_update_beginning_offsets(mock, map);
    kafka_Map_destroy(map);
    kafka_common_TopicPartition_destroy(tp);
}

/* An assigned, earliest-positioned mock holding one polled record, so
 * `allConsumed()` has something to commit (position 1 for topic-0). */
static void make_consumed_mock(fixture_t *f, const char *topic) {
    fixture_init(f);
    TEST_ASSERT_NULL(assign_one(f->consumer, topic, 0));
    update_beginning_offset(f->mock, topic, 0);
    kafka_consumer_ConsumerRecord_t *record = kafka_consumer_ConsumerRecord_new(topic, 0, 0, NULL, NULL);
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_add_record(f->mock, record));
    kafka_consumer_ConsumerRecord_destroy(record);
    kafka_consumer_ConsumerRecords_t *records = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_poll(f->consumer, 10, &records));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));
    kafka_consumer_ConsumerRecords_destroy(records);
}

/* Writes "t-0,t-1" for a (borrowed) list of TopicPartitions. */
static void snapshot_partitions(const kafka_List_t *partitions, char *out, size_t cap) {
    out[0] = '\0';
    size_t used = 0;
    for (int32_t i = 0; i < kafka_List_size(partitions); i++) {
        const kafka_common_TopicPartition_t *tp = (const kafka_common_TopicPartition_t *)kafka_List_get(partitions, i);
        int n = snprintf(out + used, cap - used, "%s%s-%d", i == 0 ? "" : ",",
                         kafka_common_TopicPartition_topic(tp), kafka_common_TopicPartition_partition(tp));
        if (n < 0 || (size_t)n >= cap - used) {
            break;
        }
        used += (size_t)n;
    }
}

/* The completion of a void `_cb` operation. */
typedef struct {
    atomic_int fired;
    kafka_common_Error_t *error;
    pthread_t thread;
    int sequence; /* the global sequence number when it fired */
} completion_t;

static atomic_int g_sequence; /* orders callback and completion firings */

static void completion_init(completion_t *c) {
    memset(c, 0, sizeof(*c));
    atomic_init(&c->fired, 0);
}

static void on_void_op(kafka_common_Error_t *error, void *opaque) {
    completion_t *c = (completion_t *)opaque;
    c->error = error;
    c->thread = pthread_self();
    c->sequence = atomic_fetch_add(&g_sequence, 1);
    atomic_fetch_add(&c->fired, 1);
}

// ---------------------------------------------------------------------------
// The rebalance-listener probe
//
// A C `ConsumerRebalanceListener` implementation whose methods record the
// thread, the (sorted, borrowed) partition list and the call counts. With
// `report` set each method reports at once through `__set_callback_result`,
// `assigned_error` (owned) standing in for the success; otherwise it stores
// the `callback_id` in `pending_id` for the test to report later, which keeps
// the invoking operation in flight for as long as the test wants.
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_Consumer_t *consumer;    /* informational for the report; NULL is fine */
    kafka_consumer_ConsumerHandle_t *handle; /* when set, assigned calls back through it */
    int report;
    kafka_common_Error_t *assigned_error;   /* reported by the next assigned call, ownership moves */
    atomic_int revoked;
    atomic_int assigned;
    atomic_int lost;
    _Atomic(int64_t) pending_id;
    pthread_t last_thread;
    char last_partitions[256];          /* of the last call, whichever method */
    char last_revoked_partitions[256];  /* of the last revoked call */
    int last_count;
    int sequence;                   /* the global sequence number of the last call */
    /* What a reentrant call from inside the listener observed. */
    int handle_assignment_size;
    int handle_commit_unsupported;
    int consumer_call_concurrent_modification;
} listener_probe_t;

static void probe_init(listener_probe_t *p, kafka_consumer_Consumer_t *consumer, int report) {
    memset(p, 0, sizeof(*p));
    p->consumer = consumer;
    p->report = report;
    p->handle_assignment_size = -1;
    atomic_init(&p->revoked, 0);
    atomic_init(&p->assigned, 0);
    atomic_init(&p->lost, 0);
    atomic_init(&p->pending_id, 0);
}

static void probe_record(listener_probe_t *p, const kafka_List_t *partitions, int64_t callback_id,
                         kafka_common_Error_t *result) {
    p->last_thread = pthread_self();
    p->last_count = kafka_List_size(partitions);
    snapshot_partitions(partitions, p->last_partitions, sizeof(p->last_partitions));
    p->sequence = atomic_fetch_add(&g_sequence, 1);
    if (p->report) {
        kafka_consumer_Consumer__set_callback_result(p->consumer, callback_id, result);
    } else {
        atomic_store(&p->pending_id, callback_id);
    }
}

static void probe_on_revoked(void *self_, const kafka_List_t *partitions, int64_t callback_id) {
    listener_probe_t *p = (listener_probe_t *)self_;
    atomic_fetch_add(&p->revoked, 1);
    snapshot_partitions(partitions, p->last_revoked_partitions, sizeof(p->last_revoked_partitions));
    probe_record(p, partitions, callback_id, NULL);
}

static void probe_on_assigned(void *self_, const kafka_List_t *partitions, int64_t callback_id) {
    listener_probe_t *p = (listener_probe_t *)self_;
    atomic_fetch_add(&p->assigned, 1);
    if (p->handle != NULL) {
        /* Reentrancy: the handle is usable from inside the callback while the
         * owning operation is in flight. */
        kafka_consumer_ConsumerHandle_wakeup(p->handle);
        kafka_List_t *asg = kafka_consumer_ConsumerHandle_assignment(p->handle);
        p->handle_assignment_size = asg == NULL ? -1 : kafka_List_size(asg);
        kafka_List_destroy(asg);
        kafka_common_Error_t *err = kafka_consumer_ConsumerHandle_commit_sync(p->handle);
        p->handle_commit_unsupported = err != NULL && kafka_common_Error_is_unsupported_version_error(err);
        kafka_common_Error_destroy(err);
        /* The consumer itself is busy with the operation that invoked us. */
        err = kafka_consumer_Consumer_commit_sync(p->consumer);
        p->consumer_call_concurrent_modification =
            err != NULL && kafka_common_Error_is_local_concurrent_modification_error(err);
        kafka_common_Error_destroy(err);
    }
    kafka_common_Error_t *result = p->assigned_error;
    p->assigned_error = NULL;
    probe_record(p, partitions, callback_id, result);
}

static void probe_on_lost(void *self_, const kafka_List_t *partitions, int64_t callback_id) {
    listener_probe_t *p = (listener_probe_t *)self_;
    atomic_fetch_add(&p->lost, 1);
    probe_record(p, partitions, callback_id, NULL);
}

/* Reports the held invocation (success) from whichever thread calls this. */
static void probe_release(listener_probe_t *p) {
    int64_t id = atomic_exchange(&p->pending_id, 0);
    TEST_ASSERT_NOT_EQUAL(0, id);
    kafka_consumer_Consumer__set_callback_result(p->consumer, id, NULL);
}

static kafka_consumer_ConsumerRebalanceListener_t *probe_listener(listener_probe_t *p, int with_lost) {
    kafka_consumer_ConsumerRebalanceListener_t *l = kafka_consumer_ConsumerRebalanceListener_new(
        p, probe_on_revoked, probe_on_assigned, with_lost ? probe_on_lost : NULL);
    TEST_ASSERT_NOT_NULL(l);
    return l;
}

/* `subscribe(topics, listener)`; the registration is copied, so the handle is
 * destroyed right away. */
static void subscribe_probe(fixture_t *f, const char *topic, listener_probe_t *p) {
    kafka_consumer_ConsumerRebalanceListener_t *listener = probe_listener(p, 0);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)topic);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_topics_listener(f->consumer, topics, listener));
    kafka_List_destroy(topics);
    kafka_consumer_ConsumerRebalanceListener_destroy(listener);
}

/* Blocking `rebalance` to `count` partitions of `topic`, freeing the list. */
static kafka_common_Error_t *rebalance_to(fixture_t *f, const char *topic, int32_t count) {
    kafka_List_t *list = tp_list(topic, count);
    kafka_common_Error_t *err = kafka_consumer_MockConsumer_rebalance(f->mock, list);
    tp_list_destroy(list);
    return err;
}

// ---------------------------------------------------------------------------
// The commit-callback probe
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_Consumer_t *consumer;
    int report;
    atomic_int calls;
    _Atomic(int64_t) pending_id;
    pthread_t thread;
    int error_was_null;
    int offsets_size;
    int64_t first_offset;         /* of the first entry */
    char first_metadata[64];
    int sequence;
} commit_probe_t;

static void commit_probe_init(commit_probe_t *p, kafka_consumer_Consumer_t *consumer, int report) {
    memset(p, 0, sizeof(*p));
    p->consumer = consumer;
    p->report = report;
    p->first_offset = -1;
    atomic_init(&p->calls, 0);
    atomic_init(&p->pending_id, 0);
}

/* `kafka_consumer_OffsetCommitCallback_on_complete_fn_t`: `offsets` and
 * `error` are borrowed for the call. */
static void commit_probe_on_complete(void *self_, const kafka_Map_t *offsets, const kafka_common_Error_t *error,
                                     int64_t callback_id) {
    commit_probe_t *p = (commit_probe_t *)self_;
    p->thread = pthread_self();
    p->error_was_null = error == NULL;
    p->offsets_size = offsets == NULL ? -1 : kafka_Map_size(offsets);
    if (p->offsets_size > 0) {
        const kafka_consumer_OffsetAndMetadata_t *oam =
            (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_value(offsets, 0);
        p->first_offset = kafka_consumer_OffsetAndMetadata_offset(oam);
        const char *md = kafka_consumer_OffsetAndMetadata_metadata(oam);
        snprintf(p->first_metadata, sizeof(p->first_metadata), "%s", md == NULL ? "" : md);
    }
    p->sequence = atomic_fetch_add(&g_sequence, 1);
    atomic_fetch_add(&p->calls, 1);
    if (p->report) {
        kafka_consumer_Consumer__set_callback_result(p->consumer, callback_id, NULL);
    } else {
        atomic_store(&p->pending_id, callback_id);
    }
}

static void commit_probe_release(commit_probe_t *p) {
    int64_t id = atomic_exchange(&p->pending_id, 0);
    TEST_ASSERT_NOT_EQUAL(0, id);
    kafka_consumer_Consumer__set_callback_result(p->consumer, id, NULL);
}

static kafka_consumer_OffsetCommitCallback_t *commit_probe_callback(commit_probe_t *p) {
    kafka_consumer_OffsetCommitCallback_t *cb = kafka_consumer_OffsetCommitCallback_new(p, commit_probe_on_complete);
    TEST_ASSERT_NOT_NULL(cb);
    return cb;
}

// ===========================================================================
// OffsetCommitCallback
// ===========================================================================

// commitAsync(callback) on the mock fires onComplete(allConsumed(), null),
// directly on the calling thread (blocking form).
static void test_commit_async_with_callback_fires_with_offsets_null_error(void) {
    fixture_t f;
    make_consumed_mock(&f, "test");

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_async_with_callback(f.consumer, cb));
    kafka_consumer_OffsetCommitCallback_destroy(cb);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_TRUE(probe.error_was_null);
    TEST_ASSERT_EQUAL_INT(1, probe.offsets_size);
    TEST_ASSERT_EQUAL_INT64(1, probe.first_offset); /* position after the offset-0 record */
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.thread));

    /* The commit happened before the callback. */
    kafka_List_t *list = tp_list("test", 1);
    kafka_Map_t *committed = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_committed(f.consumer, list, &committed));
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(committed));
    TEST_ASSERT_EQUAL_INT64(1, kafka_consumer_OffsetAndMetadata_offset(
                                   (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_value(committed, 0)));
    kafka_Map_destroy(committed);
    tp_list_destroy(list);
    fixture_destroy(&f);
}

// commitAsync(offsets, callback) hands the callback the very offsets.
static void test_commit_async_with_offsets_callback_echoes_offsets(void) {
    fixture_t f;
    make_consumed_mock(&f, "test");

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_with_metadata(42, "m", &oam));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, tp, oam);

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_async_with_offsets_callback(f.consumer, offsets, cb));
    kafka_consumer_OffsetCommitCallback_destroy(cb);
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_TRUE(probe.error_was_null);
    TEST_ASSERT_EQUAL_INT(1, probe.offsets_size);
    TEST_ASSERT_EQUAL_INT64(42, probe.first_offset);
    TEST_ASSERT_EQUAL_STRING("m", probe.first_metadata);

    kafka_List_t *list = kafka_List_new();
    kafka_List_add(list, tp);
    kafka_Map_t *committed = NULL;
    TEST_ASSERT_NULL(kafka_consumer_Consumer_committed(f.consumer, list, &committed));
    const kafka_consumer_OffsetAndMetadata_t *got =
        (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_get(committed, tp);
    TEST_ASSERT_NOT_NULL(got);
    TEST_ASSERT_EQUAL_INT64(42, kafka_consumer_OffsetAndMetadata_offset(got));
    TEST_ASSERT_EQUAL_STRING("m", kafka_consumer_OffsetAndMetadata_metadata(got));
    kafka_Map_destroy(committed);
    kafka_List_destroy(list);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// The `_cb` form queues the callback: nothing runs until the pump, the
// callback then runs on the pumping thread with the offsets readable, and the
// completion fires only after it reported.
static void test_commit_async_with_callback_cb_queues_the_callback(void) {
    fixture_t f;
    make_consumed_mock(&f, "test");

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    completion_t completion;
    completion_init(&completion);
    kafka_consumer_Consumer_commit_async_with_callback_cb(f.consumer, cb, on_void_op, &completion);

    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.calls)); /* queued, not run */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired));

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    kafka_consumer_OffsetCommitCallback_destroy(cb);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_TRUE(probe.error_was_null);
    TEST_ASSERT_EQUAL_INT(1, probe.offsets_size);
    TEST_ASSERT_EQUAL_INT64(1, probe.first_offset);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.thread));
    TEST_ASSERT_NULL(completion.error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    TEST_ASSERT_TRUE(probe.sequence < completion.sequence); /* callback, then completion */
    fixture_destroy(&f);
}

typedef struct {
    fixture_t *f;
    kafka_consumer_OffsetCommitCallback_t *cb;
    atomic_int returned;
    kafka_common_Error_t *error;
} blocking_commit_t;

static void *blocking_commit_thread(void *arg) {
    blocking_commit_t *b = (blocking_commit_t *)arg;
    b->error = kafka_consumer_Consumer_commit_async_with_callback(b->f->consumer, b->cb);
    atomic_store(&b->returned, 1);
    return NULL;
}

// The blocking commit does not return until the callback reported.
static void test_commit_returns_only_after_callback_reports(void) {
    fixture_t f;
    make_consumed_mock(&f, "test");

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 0); /* holds its report */
    blocking_commit_t b = { &f, commit_probe_callback(&probe), 0, NULL };
    atomic_init(&b.returned, 0);
    pthread_t thread;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, NULL, blocking_commit_thread, &b));

    TEST_ASSERT_TRUE(wait_for(&probe.calls, 1));
    TEST_ASSERT_TRUE(pthread_equal(thread, probe.thread)); /* on the caller's thread */
    sleep_ms(100);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&b.returned)); /* still blocked */

    commit_probe_release(&probe); /* from another thread */
    pthread_join(thread, NULL);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&b.returned));
    TEST_ASSERT_NULL(b.error);
    kafka_consumer_OffsetCommitCallback_destroy(b.cb);
    fixture_destroy(&f);
}

// The standalone invoker runs the C implementation on the calling thread and
// waits for its report, with the map and error it was given.
static void test_offset_commit_callback_standalone_invoker(void) {
    commit_probe_t probe;
    commit_probe_init(&probe, NULL, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("t", 2);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_with_metadata(9, "md", &oam));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, tp, oam);
    kafka_common_Error_t *error = kafka_common_Error_timeout("commit timed out");

    kafka_consumer_OffsetCommitCallback_on_complete(cb, offsets, error);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_FALSE(probe.error_was_null);
    TEST_ASSERT_EQUAL_INT(1, probe.offsets_size);
    TEST_ASSERT_EQUAL_INT64(9, probe.first_offset);
    TEST_ASSERT_EQUAL_STRING("md", probe.first_metadata);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.thread));

    kafka_consumer_OffsetCommitCallback_on_complete(cb, offsets, NULL);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.calls));
    TEST_ASSERT_TRUE(probe.error_was_null);

    kafka_common_Error_destroy(error); /* borrowed by the invoker */
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);
    kafka_common_TopicPartition_destroy(tp);
    kafka_consumer_OffsetCommitCallback_destroy(cb);
}

// ===========================================================================
// ConsumerHandle
// ===========================================================================

static void test_consumer_handle_new_destroy(void) {
    fixture_t f;
    fixture_init(&f);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);
    TEST_ASSERT_NOT_NULL(h);
    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_ConsumerHandle_destroy(NULL); /* a no-op */
    fixture_destroy(&f);
}

// On a mock the handle has no SubscriptionState to read: empty sets.
static void test_consumer_handle_sync_getters_empty_on_mock(void) {
    fixture_t f;
    fixture_init(&f);
    TEST_ASSERT_NULL(assign_one(f.consumer, "test", 0));
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);

    kafka_List_t *l = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_NOT_NULL(l);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(l));
    kafka_List_destroy(l);
    l = kafka_consumer_ConsumerHandle_subscription(h);
    TEST_ASSERT_NOT_NULL(l);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(l));
    kafka_List_destroy(l);
    l = kafka_consumer_ConsumerHandle_paused(h);
    TEST_ASSERT_NOT_NULL(l);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(l));
    kafka_List_destroy(l);

    kafka_consumer_ConsumerHandle_destroy(h);
    fixture_destroy(&f);
}

static void assert_mock_handle_unsupported(kafka_common_Error_t *err) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_unsupported_version_error(err));
    TEST_ASSERT_EQUAL_STRING("ConsumerHandle async operations are not supported on a MockConsumer handle; "
                             "drive the MockConsumer directly.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
}

// Every async handle operation fails on a mock handle with UnsupportedVersion
// and leaves its out-parameter untouched; the empty-assign rejection comes
// first and is an IllegalArgument.
static void test_consumer_handle_async_ops_unsupported_on_mock(void) {
    fixture_t f;
    fixture_init(&f);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);
    kafka_List_t *list = tp_list("test", 1);
    const kafka_common_TopicPartition_t *tp = (const kafka_common_TopicPartition_t *)kafka_List_get(list, 0);

    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_assign(h, list));
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_seek_with_offset(h, tp, 5));
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_seek_to_beginning(h, list));
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_commit_sync(h));
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_commit_async(h));

    int64_t position = -42;
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_position(h, tp, &position));
    TEST_ASSERT_EQUAL_INT64(-42, position);
    kafka_Map_t *committed = (kafka_Map_t *)&f; /* sentinel */
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_committed(h, list, &committed));
    TEST_ASSERT_EQUAL_PTR(&f, committed);
    kafka_Map_t *begin = (kafka_Map_t *)&f;
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_beginning_offsets(h, list, &begin));
    TEST_ASSERT_EQUAL_PTR(&f, begin);

    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_new(3, &oam));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, (void *)tp, oam);
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_commit_sync_with_offsets(h, offsets));
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_commit_async_offsets(h, offsets));
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);

    /* The `_cb` twin delivers the same rejection through its completion,
     * queued on the owning consumer's callbacks vector. */
    completion_t completion;
    completion_init(&completion);
    kafka_consumer_ConsumerHandle_commit_sync_cb(h, on_void_op, &completion);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    assert_mock_handle_unsupported(completion.error);

    /* The handle kind is checked first: even `assign([])` is "unsupported"
     * here (its IllegalArgument rejection is a real consumer's, see
     * test_consumer_handle_shares_state_with_real_consumer). */
    kafka_List_t *empty = kafka_List_new();
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_assign(h, empty));
    kafka_List_destroy(empty);

    tp_list_destroy(list);
    kafka_consumer_ConsumerHandle_destroy(h);
    fixture_destroy(&f);
}

// The handle's wakeup is the consumer's: the next poll returns Wakeup.
static void test_consumer_handle_wakeup(void) {
    fixture_t f;
    fixture_init(&f);
    TEST_ASSERT_NULL(assign_one(f.consumer, "test", 0));
    update_beginning_offset(f.mock, "test", 0);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);
    kafka_consumer_ConsumerHandle_wakeup(h);
    kafka_consumer_ConsumerHandle_destroy(h);

    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_poll(f.consumer, 10, &records);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);

    TEST_ASSERT_NULL(kafka_consumer_Consumer_poll(f.consumer, 10, &records));
    kafka_consumer_ConsumerRecords_destroy(records);
    fixture_destroy(&f);
}

// While a `_cb` operation holds the consumer, `handle()` and the handle's
// operations stay usable, whereas a blocking consumer call is rejected.
static void test_consumer_handle_usable_while_op_in_flight(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 0);
    subscribe_probe(&f, "test", &probe);

    completion_t completion;
    completion_init(&completion);
    kafka_List_t *list = tp_list("test", 1);
    kafka_consumer_MockConsumer_rebalance_cb(f.mock, list, on_void_op, &completion);

    kafka_common_Error_t *err = kafka_consumer_Consumer_commit_sync(f.consumer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_concurrent_modification_error(err));
    kafka_common_Error_destroy(err);

    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);
    TEST_ASSERT_NOT_NULL(h);
    kafka_consumer_ConsumerHandle_wakeup(h);
    kafka_List_t *asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_NOT_NULL(asg);
    kafka_List_destroy(asg);
    assert_mock_handle_unsupported(kafka_consumer_ConsumerHandle_commit_sync(h)); /* not a CM rejection */
    kafka_consumer_ConsumerHandle_destroy(h);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &probe.assigned, 1));
    probe_release(&probe);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_NULL(completion.error);
    tp_list_destroy(list);
    fixture_destroy(&f);
}

/* A KafkaConsumer pointed at an unreachable bootstrap: no broker is needed
 * for the metadata-free calls below. The caller owns the returned handle. */
static kafka_consumer_Consumer_t *create_real_consumer(void) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"localhost:9092");
    kafka_Map_put(props, (void *)"group.id", (void *)"handle-test-group");
    kafka_Map_put(props, (void *)"group.protocol", (void *)"consumer");
    kafka_consumer_ConsumerConfig_t *config = NULL;
    TEST_ASSERT_NULL(kafka_consumer_ConsumerConfig_new(props, &config));
    kafka_Map_destroy(props);
    kafka_consumer_Consumer_t *consumer = NULL;
    TEST_ASSERT_NULL(kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &consumer));
    kafka_consumer_ConsumerConfig_destroy(config);
    TEST_ASSERT_NOT_NULL(consumer);
    return consumer;
}

// On a real consumer the handle reads the shared SubscriptionState, and once
// the consumer is destroyed its operations fail with "consumer destroyed".
static void test_consumer_handle_shares_state_with_real_consumer(void) {
    kafka_consumer_Consumer_t *consumer = create_real_consumer();
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(consumer);
    TEST_ASSERT_NOT_NULL(h);

    kafka_List_t *asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(asg));
    kafka_List_destroy(asg);

    TEST_ASSERT_NULL(assign_one(consumer, "handle-topic", 0));
    asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    const kafka_common_TopicPartition_t *tp = (const kafka_common_TopicPartition_t *)kafka_List_get(asg, 0);
    TEST_ASSERT_EQUAL_STRING("handle-topic", kafka_common_TopicPartition_topic(tp));
    kafka_List_destroy(asg);
    kafka_List_t *paused = kafka_consumer_ConsumerHandle_paused(h);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(paused));
    kafka_List_destroy(paused);

    /* `assign([])` through the handle would leave the group, which only the
     * owning consumer may do: rejected up front, no broker involved. */
    kafka_List_t *empty = kafka_List_new();
    kafka_common_Error_t *err = kafka_consumer_ConsumerHandle_assign(h, empty);
    kafka_List_destroy(empty);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err),
                                "ConsumerHandle::assign with an empty collection is not supported"));
    kafka_common_Error_destroy(err);

    kafka_consumer_Consumer_destroy(consumer);

    err = kafka_consumer_ConsumerHandle_commit_sync(h);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("consumer destroyed", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_consumer_ConsumerHandle_wakeup(h); /* harmless */
    kafka_consumer_ConsumerHandle_destroy(h);
}

// ===========================================================================
// ConsumerRebalanceListener
// ===========================================================================

// MockConsumer.rebalance: onPartitionsRevoked(removed) only when something
// was removed, then onPartitionsAssigned(added) always, with sorted lists.
static void test_rebalance_listener_assigned_then_revoked(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    subscribe_probe(&f, "t", &probe);

    TEST_ASSERT_NULL(rebalance_to(&f, "t", 2));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_INT(2, probe.last_count);
    TEST_ASSERT_EQUAL_STRING("t-0,t-1", probe.last_partitions);
    kafka_List_t *asg = kafka_consumer_Consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(asg));
    kafka_List_destroy(asg);

    /* Shrinking to {t-0}: revoked(t-1), then assigned(nothing new). */
    TEST_ASSERT_NULL(rebalance_to(&f, "t", 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_STRING("t-1", probe.last_revoked_partitions);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.assigned));
    TEST_ASSERT_EQUAL_INT(0, probe.last_count); /* the assigned call came last, with [] */

    /* Everything away: revoked(t-0). */
    kafka_List_t *empty = kafka_List_new();
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_rebalance(f.mock, empty));
    kafka_List_destroy(empty);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_STRING("t-0", probe.last_revoked_partitions);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.lost));
    asg = kafka_consumer_Consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(asg));
    kafka_List_destroy(asg);
    fixture_destroy(&f);
}

// The blocking `rebalance` invokes the listener directly on the calling
// thread, the methods reporting synchronously; a NULL `on_partitions_lost`
// falls back to `on_partitions_revoked` (Java's default method).
static void test_rebalance_listener_runs_on_calling_thread(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    subscribe_probe(&f, "t", &probe);

    TEST_ASSERT_NULL(rebalance_to(&f, "t", 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));
    TEST_ASSERT_EQUAL_STRING("t-0", probe.last_partitions);

    /* Rebalancing the partition away: revoked (then assigned([])), same
     * thread. */
    kafka_List_t *empty = kafka_List_new();
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_rebalance(f.mock, empty));
    kafka_List_destroy(empty);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.revoked));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));
    TEST_ASSERT_EQUAL_STRING("t-0", probe.last_revoked_partitions);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.assigned));
    TEST_ASSERT_EQUAL_INT(0, probe.last_count);

    /* The default `onPartitionsLost`: a listener registered with a NULL lost
     * method routes `on_partitions_lost` to its revoked method. */
    kafka_consumer_ConsumerRebalanceListener_t *defaulted = probe_listener(&probe, 0);
    kafka_List_t *list = tp_list("t", 2);
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(defaulted, list));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.lost));
    TEST_ASSERT_EQUAL_STRING("t-0,t-1", probe.last_revoked_partitions);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));
    kafka_consumer_ConsumerRebalanceListener_destroy(defaulted);

    /* With a lost method of its own, that one runs. */
    kafka_consumer_ConsumerRebalanceListener_t *overridden = probe_listener(&probe, 1);
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(overridden, list));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.lost));
    /* The other two standalone invokers. */
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked(overridden, list));
    TEST_ASSERT_EQUAL_INT(3, atomic_load(&probe.revoked));
    TEST_ASSERT_NULL(kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned(overridden, list));
    TEST_ASSERT_EQUAL_INT(3, atomic_load(&probe.assigned));
    kafka_consumer_ConsumerRebalanceListener_destroy(overridden);
    tp_list_destroy(list);
    fixture_destroy(&f);
}

typedef struct {
    listener_probe_t *probe;
    atomic_int done;
} reporter_t;

static void *reporter_thread(void *arg) {
    reporter_t *r = (reporter_t *)arg;
    int64_t id = atomic_exchange(&r->probe->pending_id, 0);
    if (id != 0) {
        kafka_consumer_Consumer__set_callback_result(r->probe->consumer, id, NULL);
    }
    atomic_store(&r->done, id != 0);
    return NULL;
}

// `rebalance_cb` queues the listener invocation: nothing runs until the pump
// (the notify hook tells when), the listener then runs on the pumping thread,
// and the completion fires only once the result is reported — here from a
// second thread, after which the pump runs the completion.
static void test_rebalance_cb_queues_the_listener_for_the_pump(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 0); /* holds its report */
    subscribe_probe(&f, "t", &probe);

    completion_t completion;
    completion_init(&completion);
    kafka_List_t *list = tp_list("t", 1);
    kafka_consumer_MockConsumer_rebalance_cb(f.mock, list, on_void_op, &completion);

    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&f.pump.notified));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.assigned)); /* announced, not run */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired));

    TEST_ASSERT_EQUAL_INT32(1, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));
    TEST_ASSERT_EQUAL_STRING("t-0", probe.last_partitions);
    TEST_ASSERT_NOT_EQUAL(0, atomic_load(&probe.pending_id));
    sleep_ms(50);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired)); /* waiting for the report */
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));

    /* Report from a second thread. */
    reporter_t reporter = { &probe, 0 };
    atomic_init(&reporter.done, 0);
    pthread_t thread;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, NULL, reporter_thread, &reporter));
    pthread_join(thread, NULL);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&reporter.done));

    /* The completion is queued in turn: a second transition, pumped here. */
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&f.pump.notified));
    TEST_ASSERT_EQUAL_INT32(1, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&completion.fired));
    TEST_ASSERT_NULL(completion.error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));

    kafka_List_t *asg = kafka_consumer_Consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    kafka_List_destroy(asg);
    tp_list_destroy(list);
    fixture_destroy(&f);
}

typedef struct {
    fixture_t *f;
    atomic_int returned;
    kafka_common_Error_t *error;
} blocking_rebalance_t;

static void *blocking_rebalance_thread(void *arg) {
    blocking_rebalance_t *b = (blocking_rebalance_t *)arg;
    b->error = rebalance_to(b->f, "t", 1);
    atomic_store(&b->returned, 1);
    return NULL;
}

// The blocking `rebalance` does not return until the listener reported.
static void test_rebalance_blocks_until_listener_reports(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 0);
    subscribe_probe(&f, "t", &probe);

    blocking_rebalance_t b = { &f, 0, NULL };
    atomic_init(&b.returned, 0);
    pthread_t thread;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, NULL, blocking_rebalance_thread, &b));

    TEST_ASSERT_TRUE(wait_for(&probe.assigned, 1));
    TEST_ASSERT_TRUE(pthread_equal(thread, probe.last_thread)); /* the caller's thread, not ours */
    sleep_ms(100);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&b.returned));

    probe_release(&probe);
    pthread_join(thread, NULL);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&b.returned));
    TEST_ASSERT_NULL(b.error);
    fixture_destroy(&f);
}

// `rebalance` is for subscribed consumers: on a manually assigned one the
// mock rejects it the way SubscriptionState does.
static void test_rebalance_requires_a_subscription(void) {
    fixture_t f;
    fixture_init(&f);
    TEST_ASSERT_NULL(assign_one(f.consumer, "t", 0));

    kafka_common_Error_t *err = rebalance_to(&f, "t", 2);
    TEST_ASSERT_NOT_NULL(err);
    /* SubscriptionState.assignFromSubscribed throws IllegalArgumentException. */
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("Attempt to dynamically assign partitions while manual assignment in use",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    fixture_destroy(&f);
}

// Subscription and manual assignment are mutually exclusive.
static void test_subscribe_with_listener_on_manually_assigned_consumer_fails(void) {
    fixture_t f;
    fixture_init(&f);
    TEST_ASSERT_NULL(assign_one(f.consumer, "t", 0));

    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    kafka_consumer_ConsumerRebalanceListener_t *listener = probe_listener(&probe, 0);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"t");
    kafka_common_Error_t *err = kafka_consumer_Consumer_subscribe_with_topics_listener(f.consumer, topics, listener);
    kafka_List_destroy(topics);
    kafka_consumer_ConsumerRebalanceListener_destroy(listener);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("Subscription to topics, partitions and pattern are mutually exclusive",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.assigned));
    fixture_destroy(&f);
}

// A listener calling the consumer back through its handle from inside
// `on_partitions_assigned` does not deadlock; the consumer itself is busy.
static void test_listener_calls_consumer_handle_no_deadlock(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    probe.handle = kafka_consumer_Consumer_handle(f.consumer);
    subscribe_probe(&f, "t", &probe);

    TEST_ASSERT_NULL(rebalance_to(&f, "t", 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));
    TEST_ASSERT_EQUAL_INT(0, probe.handle_assignment_size); /* a mock handle reads nothing */
    TEST_ASSERT_TRUE(probe.handle_commit_unsupported);
    TEST_ASSERT_TRUE(probe.consumer_call_concurrent_modification);
    kafka_consumer_ConsumerHandle_destroy(probe.handle);

    /* The in-callback `ConsumerHandle_wakeup` armed the next poll. */
    update_beginning_offset(f.mock, "t", 0);
    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_poll(f.consumer, 10, &records);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);
    fixture_destroy(&f);
}

// An error reported by the listener is the rebalance's result, verbatim.
static void test_listener_error_propagates(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    probe.assigned_error = kafka_common_Error_kafka_message("listener failed");
    subscribe_probe(&f, "t", &probe);

    kafka_common_Error_t *err = rebalance_to(&f, "t", 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_error(err));
    TEST_ASSERT_EQUAL_STRING("listener failed", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));

    /* Java replaces the assignment before onPartitionsAssigned runs. */
    kafka_List_t *asg = kafka_consumer_Consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    kafka_List_destroy(asg);

    /* The consumer is free again. */
    TEST_ASSERT_NULL(rebalance_to(&f, "t", 1));
    fixture_destroy(&f);
}

// `subscribe_with_topics_listener_cb` completes through the pump and the
// registered listener then serves the blocking rebalance.
static void test_subscribe_with_topics_listener_cb(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    kafka_consumer_ConsumerRebalanceListener_t *listener = probe_listener(&probe, 0);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"t");

    completion_t completion;
    completion_init(&completion);
    kafka_consumer_Consumer_subscribe_with_topics_listener_cb(f.consumer, topics, listener, on_void_op, &completion);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_NULL(completion.error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    kafka_List_destroy(topics);
    kafka_consumer_ConsumerRebalanceListener_destroy(listener);

    kafka_List_t *subs = kafka_consumer_Consumer_subscription(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(subs));
    TEST_ASSERT_EQUAL_STRING("t", (const char *)kafka_List_get(subs, 0));
    kafka_List_destroy(subs);

    TEST_ASSERT_NULL(rebalance_to(&f, "t", 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));
    fixture_destroy(&f);
}

// `MockConsumer_destroy` with a never-pumped `rebalance_cb` in flight runs
// the queued listener invocation and the completion exactly once, on the
// destroying thread.
static void test_destroy_runs_pending_rebalance_cb_listener_and_completion_once(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    subscribe_probe(&f, "t", &probe);

    completion_t completion;
    completion_init(&completion);
    kafka_List_t *list = tp_list("t", 1);
    kafka_consumer_MockConsumer_rebalance_cb(f.mock, list, on_void_op, &completion);
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.assigned));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired));

    fixture_destroy(&f); /* never pumped */

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&completion.fired));
    TEST_ASSERT_NULL(completion.error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    TEST_ASSERT_TRUE(probe.sequence < completion.sequence);
    tp_list_destroy(list);
}

int main(void) {
    atomic_init(&g_sequence, 0);
    UNITY_BEGIN();
    RUN_TEST(test_commit_async_with_callback_fires_with_offsets_null_error);
    RUN_TEST(test_commit_async_with_offsets_callback_echoes_offsets);
    RUN_TEST(test_commit_async_with_callback_cb_queues_the_callback);
    RUN_TEST(test_commit_returns_only_after_callback_reports);
    RUN_TEST(test_offset_commit_callback_standalone_invoker);
    RUN_TEST(test_consumer_handle_new_destroy);
    RUN_TEST(test_consumer_handle_sync_getters_empty_on_mock);
    RUN_TEST(test_consumer_handle_async_ops_unsupported_on_mock);
    RUN_TEST(test_consumer_handle_wakeup);
    RUN_TEST(test_consumer_handle_usable_while_op_in_flight);
    RUN_TEST(test_consumer_handle_shares_state_with_real_consumer);
    RUN_TEST(test_rebalance_listener_assigned_then_revoked);
    RUN_TEST(test_rebalance_listener_runs_on_calling_thread);
    RUN_TEST(test_rebalance_cb_queues_the_listener_for_the_pump);
    RUN_TEST(test_rebalance_blocks_until_listener_reports);
    RUN_TEST(test_rebalance_requires_a_subscription);
    RUN_TEST(test_subscribe_with_listener_on_manually_assigned_consumer_fails);
    RUN_TEST(test_listener_calls_consumer_handle_no_deadlock);
    RUN_TEST(test_listener_error_propagates);
    RUN_TEST(test_subscribe_with_topics_listener_cb);
    RUN_TEST(test_destroy_runs_pending_rebalance_cb_listener_and_completion_once);
    return UNITY_END();
}
