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
// `kafka_consumer_Consumer__execute_callbacks`; `_destroy` runs what is still
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
#include <stdlib.h>
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
    kafka_common_Error_t *err = NULL;
    f->mock = kafka_consumer_MockConsumer_new("earliest", &err);
    assert_ok(err, "MockConsumer_new");
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

/* A list holding the one partition `topic-partition`; freed through
 * `tp_list_destroy`. */
static kafka_List_t *tp_list_one(const char *topic, int32_t partition) {
    kafka_List_t *list = kafka_List_new();
    kafka_List_add(list, kafka_common_TopicPartition_new(topic, partition));
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
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_assign(c, list, &err);
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
    kafka_common_Error_t *err = NULL;
    fixture_init(f);
    TEST_ASSERT_NULL(assign_one(f->consumer, topic, 0));
    update_beginning_offset(f->mock, topic, 0);
    kafka_consumer_ConsumerRecord_t *record = kafka_consumer_ConsumerRecord_new(topic, 0, 0, NULL, NULL);
    kafka_consumer_MockConsumer_add_record(f->mock, record, &err);
    assert_ok(err, "MockConsumer_add_record");
    kafka_consumer_ConsumerRecord_destroy(record);
    kafka_consumer_ConsumerRecords_t *records = kafka_consumer_Consumer_poll(f->consumer, 10, &err);
    assert_ok(err, "Consumer_poll");
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
    kafka_consumer_Consumer_t *consumer;    /* the reports name it; NULL only for the standalone invokers */
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
    int handle_commit_succeeded;
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
                         kafka_common_Error_t *error) {
    p->last_thread = pthread_self();
    p->last_count = kafka_List_size(partitions);
    snapshot_partitions(partitions, p->last_partitions, sizeof(p->last_partitions));
    p->sequence = atomic_fetch_add(&g_sequence, 1);
    if (p->report) {
        kafka_consumer_Consumer__set_callback_result(p->consumer, callback_id, NULL, error);
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
        kafka_common_Error_t *err = NULL;
        kafka_consumer_ConsumerHandle_commit_sync(p->handle, &err);
        p->handle_commit_succeeded = err == NULL;
        kafka_common_Error_destroy(err);
        /* The consumer itself is busy with the operation that invoked us. */
        err = NULL;
        kafka_consumer_Consumer_commit_sync(p->consumer, &err);
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
    kafka_consumer_Consumer__set_callback_result(p->consumer, id, NULL, NULL);
}

static kafka_consumer_ConsumerRebalanceListener_t *probe_listener(listener_probe_t *p, int with_lost) {
    kafka_consumer_ConsumerRebalanceListener_t *l = kafka_consumer_ConsumerRebalanceListener_new(
        p, probe_on_revoked, probe_on_assigned, with_lost ? probe_on_lost : NULL);
    TEST_ASSERT_NOT_NULL(l);
    return l;
}

/* `subscribe(topics, listener)`, the listener with or without a lost method of
 * its own; the registration is copied, so the handle is destroyed right away. */
static void subscribe_probe_listener(fixture_t *f, const char *topic, listener_probe_t *p, int with_lost) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerRebalanceListener_t *listener = probe_listener(p, with_lost);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)topic);
    kafka_consumer_Consumer_subscribe_with_topics_listener(f->consumer, topics, listener, &err);
    assert_ok(err, "Consumer_subscribe_with_topics_listener");
    kafka_List_destroy(topics);
    kafka_consumer_ConsumerRebalanceListener_destroy(listener);
}

static void subscribe_probe(fixture_t *f, const char *topic, listener_probe_t *p) {
    subscribe_probe_listener(f, topic, p, 0);
}

/* Blocking `rebalance` to `count` partitions of `topic`, freeing the list. */
static kafka_common_Error_t *rebalance_to(fixture_t *f, const char *topic, int32_t count) {
    kafka_List_t *list = tp_list(topic, count);
    kafka_common_Error_t *err = NULL;
    kafka_consumer_MockConsumer_rebalance(f->mock, list, &err);
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
    const char *raise; /* reports this message as the callback's error */
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
        /* Java's onComplete throwing: the error is owned and taken over. */
        kafka_common_Error_t *raised = p->raise == NULL ? NULL : kafka_common_Error_kafka_message(p->raise);
        kafka_consumer_Consumer__set_callback_result(p->consumer, callback_id, NULL, raised);
    } else {
        atomic_store(&p->pending_id, callback_id);
    }
}

static void commit_probe_release(commit_probe_t *p) {
    int64_t id = atomic_exchange(&p->pending_id, 0);
    TEST_ASSERT_NOT_EQUAL(0, id);
    kafka_consumer_Consumer__set_callback_result(p->consumer, id, NULL, NULL);
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
    kafka_common_Error_t *err = NULL;
    fixture_t f;
    make_consumed_mock(&f, "test");

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    kafka_consumer_Consumer_commit_async_with_callback(f.consumer, cb, &err);
    assert_ok(err, "Consumer_commit_async_with_callback");
    kafka_consumer_OffsetCommitCallback_destroy(cb);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_TRUE(probe.error_was_null);
    TEST_ASSERT_EQUAL_INT(1, probe.offsets_size);
    TEST_ASSERT_EQUAL_INT64(1, probe.first_offset); /* position after the offset-0 record */
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.thread));

    /* The commit happened before the callback. */
    kafka_List_t *list = tp_list("test", 1);
    kafka_Map_t *committed = NULL;
    committed = kafka_consumer_Consumer_committed(f.consumer, list, &err);
    assert_ok(err, "Consumer_committed");
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(committed));
    TEST_ASSERT_EQUAL_INT64(1, kafka_consumer_OffsetAndMetadata_offset(
                                   (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_value(committed, 0)));
    kafka_Map_destroy(committed);
    tp_list_destroy(list);
    fixture_destroy(&f);
}

// commitAsync(offsets, callback) hands the callback the very offsets.
static void test_commit_async_with_offsets_callback_echoes_offsets(void) {
    kafka_common_Error_t *err = NULL;
    fixture_t f;
    make_consumed_mock(&f, "test");

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    oam = kafka_consumer_OffsetAndMetadata_with_metadata(42, "m", &err);
    assert_ok(err, "OffsetAndMetadata_with_metadata");
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, tp, oam);

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    kafka_consumer_Consumer_commit_async_with_offsets_callback(f.consumer, offsets, cb, &err);
    assert_ok(err, "Consumer_commit_async_with_offsets_callback");
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
    committed = kafka_consumer_Consumer_committed(f.consumer, list, &err);
    assert_ok(err, "Consumer_committed");
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

// An error the callback reports is Java's onComplete throwing: like Java's
// `MockConsumer.commitAsync`, which calls the callback with no try/catch, the
// commit returns it verbatim, after the offsets were committed.
static void test_commit_callback_error_propagates_from_commit(void) {
    fixture_t f;
    make_consumed_mock(&f, "test");

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    probe.raise = "callback failed";
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_commit_async_with_callback(f.consumer, cb, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_error(err));
    TEST_ASSERT_EQUAL_STRING("callback failed", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    err = NULL;
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));

    kafka_List_t *list = tp_list("test", 1);
    kafka_Map_t *committed = NULL;
    committed = kafka_consumer_Consumer_committed(f.consumer, list, &err);
    assert_ok(err, "Consumer_committed");
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(committed));
    kafka_Map_destroy(committed);
    tp_list_destroy(list);

    /* The consumer is usable again. */
    probe.raise = NULL;
    kafka_consumer_Consumer_commit_async_with_callback(f.consumer, cb, &err);
    assert_ok(err, "Consumer_commit_async_with_callback");
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.calls));
    kafka_consumer_OffsetCommitCallback_destroy(cb);
    fixture_destroy(&f);
}

// The `_cb` form delivers the callback's error through its completion.
static void test_commit_callback_error_propagates_from_commit_cb(void) {
    fixture_t f;
    make_consumed_mock(&f, "test");

    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    probe.raise = "queued callback failed";
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    completion_t completion;
    completion_init(&completion);
    kafka_consumer_Consumer_commit_async_with_callback_cb(f.consumer, cb, on_void_op, &completion);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    kafka_consumer_OffsetCommitCallback_destroy(cb);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_NOT_NULL(completion.error);
    TEST_ASSERT_EQUAL_STRING("queued callback failed", kafka_common_Error_message(completion.error));
    kafka_common_Error_destroy(completion.error);
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
    kafka_consumer_Consumer_commit_async_with_callback(b->f->consumer, b->cb, &b->error);
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
    kafka_common_Error_t *err = NULL;
    commit_probe_t probe;
    /* No consumer: an invoker's callback_id is reported with a NULL one. */
    commit_probe_init(&probe, NULL, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("t", 2);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    oam = kafka_consumer_OffsetAndMetadata_with_metadata(9, "md", &err);
    assert_ok(err, "OffsetAndMetadata_with_metadata");
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, tp, oam);
    kafka_common_Error_t *error = kafka_common_Error_timeout("commit timed out");

    kafka_consumer_OffsetCommitCallback_on_complete(cb, offsets, error, &err);
    assert_ok(err, "OffsetCommitCallback_on_complete");
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_FALSE(probe.error_was_null);
    TEST_ASSERT_EQUAL_INT(1, probe.offsets_size);
    TEST_ASSERT_EQUAL_INT64(9, probe.first_offset);
    TEST_ASSERT_EQUAL_STRING("md", probe.first_metadata);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.thread));

    kafka_consumer_OffsetCommitCallback_on_complete(cb, offsets, NULL, &err);
    assert_ok(err, "OffsetCommitCallback_on_complete");
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.calls));
    TEST_ASSERT_TRUE(probe.error_was_null);

    /* The error the callback reports is the invoker's result. */
    probe.raise = "raised by onComplete";
    kafka_common_Error_t *raised = NULL;
    kafka_consumer_OffsetCommitCallback_on_complete(cb, offsets, NULL, &raised);
    TEST_ASSERT_NOT_NULL(raised);
    TEST_ASSERT_EQUAL_STRING("raised by onComplete", kafka_common_Error_message(raised));
    kafka_common_Error_destroy(raised);
    TEST_ASSERT_EQUAL_INT(3, atomic_load(&probe.calls));

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

// On a mock the handle reads the mock's own state, as a Java callback
// calling `consumer.assignment()` / `paused()` / `groupMetadata()` /
// `currentLag()` does. (`subscription()` is not on the handle: KIP-1306's
// RebalanceConsumer does not carry it.)
static void test_consumer_handle_sync_getters_read_the_mock(void) {
    kafka_common_Error_t *err = NULL;
    fixture_t f;
    fixture_init(&f);
    TEST_ASSERT_NULL(assign_one(f.consumer, "test", 0));
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);

    kafka_List_t *l = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_NOT_NULL(l);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(l));
    const kafka_common_TopicPartition_t *tp = (const kafka_common_TopicPartition_t *)kafka_List_get(l, 0);
    TEST_ASSERT_EQUAL_STRING("test", kafka_common_TopicPartition_topic(tp));
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartition_partition(tp));
    kafka_List_destroy(l);
    l = kafka_consumer_ConsumerHandle_paused(h);
    TEST_ASSERT_NOT_NULL(l);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(l));
    kafka_List_destroy(l);

    kafka_List_t *list = tp_list("test", 1);
    kafka_consumer_Consumer_pause(f.consumer, list, &err);
    assert_ok(err, "Consumer_pause");
    l = kafka_consumer_ConsumerHandle_paused(h);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(l));
    kafka_List_destroy(l);

    /* The sentinels of Java's `org.apache.kafka.clients.consumer.MockConsumer#groupMetadata`. */
    kafka_consumer_ConsumerGroupMetadata_t *gm = kafka_consumer_ConsumerHandle_group_metadata(h);
    TEST_ASSERT_NOT_NULL(gm);
    TEST_ASSERT_EQUAL_STRING("dummy.group.id", kafka_consumer_ConsumerGroupMetadata_group_id(gm));
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerGroupMetadata_generation_id(gm));
    TEST_ASSERT_EQUAL_STRING("1", kafka_consumer_ConsumerGroupMetadata_member_id(gm));
    TEST_ASSERT_NULL(kafka_consumer_ConsumerGroupMetadata_group_instance_id(gm));
    kafka_consumer_ConsumerGroupMetadata_destroy(gm);

    /* `org.apache.kafka.clients.consumer.MockConsumer#currentLag`: no end offset yet -> 0, the
     * test modelling being caught up; then end 10 - position 4 = 6. A mock
     * handle is not gated (Java's MockConsumer has no acquire()), so the
     * blocking form answers outside a listener. */
    const kafka_common_TopicPartition_t *assigned = (const kafka_common_TopicPartition_t *)kafka_List_get(list, 0);
    update_beginning_offset(f.mock, "test", 0);
    int64_t handle_lag = -1;
    TEST_ASSERT_EQUAL_INT8(1, kafka_consumer_ConsumerHandle_current_lag(h, assigned, &handle_lag, &err));
    assert_ok(err, "ConsumerHandle_current_lag");
    TEST_ASSERT_EQUAL_INT64(0, handle_lag);
    int64_t ten = 10;
    kafka_Map_t *ends = kafka_Map_new();
    kafka_Map_put(ends, (void *)assigned, &ten);
    kafka_consumer_MockConsumer_update_end_offsets(f.mock, ends);
    kafka_Map_destroy(ends);
    kafka_consumer_Consumer_seek_with_offset(f.consumer, assigned, 4, &err);
    assert_ok(err, "Consumer_seek_with_offset");
    TEST_ASSERT_EQUAL_INT8(1, kafka_consumer_ConsumerHandle_current_lag(h, assigned, &handle_lag, &err));
    assert_ok(err, "ConsumerHandle_current_lag");
    TEST_ASSERT_EQUAL_INT64(6, handle_lag);

    /* The `_cb` twin delivers the same lag through the consumer's queue. */
    lag_completion_t lag_done;
    lag_completion_init(&lag_done);
    kafka_consumer_ConsumerHandle_current_lag_cb(h, assigned, on_lag, &lag_done);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &lag_done.fired, 1));
    TEST_ASSERT_NULL(lag_done.error);
    TEST_ASSERT_EQUAL_INT8(1, lag_done.present);
    TEST_ASSERT_EQUAL_INT64(6, lag_done.value);

    /* Past the log end the lag is negative and still present. */
    kafka_consumer_Consumer_seek_with_offset(f.consumer, assigned, 13, &err);
    assert_ok(err, "Consumer_seek_with_offset");
    TEST_ASSERT_EQUAL_INT8(1, kafka_consumer_ConsumerHandle_current_lag(h, assigned, &handle_lag, &err));
    assert_ok(err, "ConsumerHandle_current_lag");
    TEST_ASSERT_EQUAL_INT64(-3, handle_lag);
    lag_completion_init(&lag_done);
    kafka_consumer_ConsumerHandle_current_lag_cb(h, assigned, on_lag, &lag_done);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &lag_done.fired, 1));
    TEST_ASSERT_NULL(lag_done.error);
    TEST_ASSERT_EQUAL_INT8(1, lag_done.present);
    TEST_ASSERT_EQUAL_INT64(-3, lag_done.value);

    /* An unassigned partition with an end offset reaches Java's position(tp),
     * which throws IllegalArgumentException: 0 beside the error, *out_lag
     * unwritten. */
    kafka_common_TopicPartition_t *other = kafka_common_TopicPartition_new("other", 0);
    ends = kafka_Map_new();
    kafka_Map_put(ends, other, &ten);
    kafka_consumer_MockConsumer_update_end_offsets(f.mock, ends);
    kafka_Map_destroy(ends);
    handle_lag = 7;
    TEST_ASSERT_EQUAL_INT8(0, kafka_consumer_ConsumerHandle_current_lag(h, other, &handle_lag, &err));
    TEST_ASSERT_EQUAL_INT64(7, handle_lag);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    TEST_ASSERT_EQUAL_STRING("You can only check the position for partitions assigned to this consumer.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    err = NULL;
    kafka_common_TopicPartition_destroy(other);
    tp_list_destroy(list);

    kafka_consumer_ConsumerHandle_destroy(h);
    fixture_destroy(&f);
}

static void assert_mock_handle_unsupported(kafka_common_Error_t *err) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_unsupported_operation_error(err));
    TEST_ASSERT_EQUAL_STRING("ConsumerHandle async operations are not supported on a MockConsumer handle; "
                             "drive the MockConsumer directly.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
}

// Every async handle operation but the commits fails on a mock handle with
// LocalUnsupportedOperation and returns its failure value.
static void test_consumer_handle_async_ops_unsupported_on_mock(void) {
    kafka_common_Error_t *err = NULL;
    fixture_t f;
    fixture_init(&f);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);
    kafka_List_t *list = tp_list("test", 1);
    const kafka_common_TopicPartition_t *tp = (const kafka_common_TopicPartition_t *)kafka_List_get(list, 0);

    kafka_consumer_ConsumerHandle_seek_with_offset(h, tp, 5, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    kafka_consumer_ConsumerHandle_seek_to_beginning(h, list, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;

    int64_t position = kafka_consumer_ConsumerHandle_position(h, tp, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_EQUAL_INT64(-1, position); /* the failure value */
    kafka_Map_t *committed = kafka_consumer_ConsumerHandle_committed(h, list, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(committed);
    kafka_Map_t *begin = kafka_consumer_ConsumerHandle_beginning_offsets(h, list, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(begin);
    kafka_Map_t *committed_timed = kafka_consumer_ConsumerHandle_committed_with_timeout(h, list, 1000, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(committed_timed);
    kafka_Map_t *ends_timed = kafka_consumer_ConsumerHandle_end_offsets_with_timeout(h, list, 1000, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(ends_timed);
    kafka_List_t *infos = kafka_consumer_ConsumerHandle_partitions_for(h, "test", &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(infos);
    infos = kafka_consumer_ConsumerHandle_partitions_for_with_timeout(h, "test", 1000, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(infos);
    kafka_Map_t *topics = kafka_consumer_ConsumerHandle_list_topics(h, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(topics);
    topics = kafka_consumer_ConsumerHandle_list_topics_with_timeout(h, 1000, &err);
    assert_mock_handle_unsupported(err);
    err = NULL;
    TEST_ASSERT_NULL(topics);

    kafka_consumer_OffsetAndMetadata_t *oam = kafka_consumer_OffsetAndMetadata_new(3, &err);
    assert_ok(err, "OffsetAndMetadata_new");
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, (void *)tp, oam);
    /* The commits work on a mock handle, as Java's MockConsumer commits
     * inside a callback (see test_mock_handle_commits_commit_into_the_mock);
     * the timed forms ignore the timeout, as Java's mock does. */
    kafka_consumer_ConsumerHandle_commit_sync_with_offsets(h, offsets, &err);
    assert_ok(err, "ConsumerHandle_commit_sync_with_offsets");
    kafka_consumer_ConsumerHandle_commit_sync_with_offsets_timeout(h, offsets, 1000, &err);
    assert_ok(err, "ConsumerHandle_commit_sync_with_offsets_timeout");
    kafka_consumer_ConsumerHandle_commit_sync_with_timeout(h, 1000, &err);
    assert_ok(err, "ConsumerHandle_commit_sync_with_timeout");
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);

    /* The `_cb` twin delivers the same rejection through its completion,
     * queued on the owning consumer's callbacks vector. */
    completion_t completion;
    completion_init(&completion);
    kafka_consumer_ConsumerHandle_seek_to_beginning_cb(h, list, on_void_op, &completion);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    assert_mock_handle_unsupported(completion.error);

    tp_list_destroy(list);
    kafka_consumer_ConsumerHandle_destroy(h);
    fixture_destroy(&f);
}

/* The mock's committed offset of `topic`-0, or -1 when there is none. */
static int64_t mock_committed_offset(fixture_t *f, const char *topic) {
    kafka_common_Error_t *err = NULL;
    kafka_List_t *list = tp_list(topic, 1);
    kafka_Map_t *committed = NULL;
    committed = kafka_consumer_Consumer_committed(f->consumer, list, &err);
    assert_ok(err, "Consumer_committed");
    int64_t offset = kafka_Map_size(committed) == 0
                         ? -1
                         : kafka_consumer_OffsetAndMetadata_offset(
                               (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_value(committed, 0));
    kafka_Map_destroy(committed);
    tp_list_destroy(list);
    return offset;
}

// The handle's commits naming no offsets commit the mock's consumed positions
// (Java's `MockConsumer.commitSync()` / `commitAsync()` /
// `commitAsync(callback)`, all `subscriptions.allConsumed()`), the callback
// running inline; a callback error propagates out of the commit.
static void test_mock_handle_commits_commit_into_the_mock(void) {
    kafka_common_Error_t *err = NULL;
    fixture_t f;
    make_consumed_mock(&f, "test"); /* consumed position 1 */
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);

    TEST_ASSERT_EQUAL_INT64(-1, mock_committed_offset(&f, "test"));
    kafka_consumer_ConsumerHandle_commit_sync(h, &err);
    assert_ok(err, "ConsumerHandle_commit_sync");
    TEST_ASSERT_EQUAL_INT64(1, mock_committed_offset(&f, "test"));

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    kafka_consumer_Consumer_seek_with_offset(f.consumer, tp, 7, &err);
    assert_ok(err, "Consumer_seek_with_offset");
    kafka_consumer_ConsumerHandle_commit_async(h, &err);
    assert_ok(err, "ConsumerHandle_commit_async");
    TEST_ASSERT_EQUAL_INT64(7, mock_committed_offset(&f, "test"));

    kafka_consumer_Consumer_seek_with_offset(f.consumer, tp, 8, &err);
    assert_ok(err, "Consumer_seek_with_offset");
    commit_probe_t probe;
    commit_probe_init(&probe, f.consumer, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    kafka_consumer_ConsumerHandle_commit_async_with_callback(h, cb, &err);
    assert_ok(err, "ConsumerHandle_commit_async_with_callback");
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.calls));
    TEST_ASSERT_TRUE(probe.error_was_null);
    TEST_ASSERT_EQUAL_INT64(8, probe.first_offset);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.thread));
    TEST_ASSERT_EQUAL_INT64(8, mock_committed_offset(&f, "test"));

    probe.raise = "handle callback failed";
    kafka_consumer_ConsumerHandle_commit_async_with_callback(h, cb, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("handle callback failed", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.calls));

    kafka_consumer_OffsetCommitCallback_destroy(cb);
    kafka_common_TopicPartition_destroy(tp);
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

    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerRecords_t *records = kafka_consumer_Consumer_poll(f.consumer, 10, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);
    err = NULL;

    records = kafka_consumer_Consumer_poll(f.consumer, 10, &err);
    assert_ok(err, "Consumer_poll");
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

    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_commit_sync(f.consumer, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_concurrent_modification_error(err));
    kafka_common_Error_destroy(err);
    err = NULL;

    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);
    TEST_ASSERT_NOT_NULL(h);
    kafka_consumer_ConsumerHandle_wakeup(h);
    kafka_List_t *asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_NOT_NULL(asg);
    kafka_List_destroy(asg);
    /* Not a CM rejection: the handle commits into the mock. */
    kafka_consumer_ConsumerHandle_commit_sync(h, &err);
    assert_ok(err, "ConsumerHandle_commit_sync");
    kafka_consumer_ConsumerHandle_destroy(h);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &probe.assigned, 1));
    probe_release(&probe);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_NULL(completion.error);
    tp_list_destroy(list);
    fixture_destroy(&f);
}

/* A KafkaConsumer pointed at an unreachable bootstrap: no broker is needed
 * for the metadata-free calls below. The caller owns the returned class
 * handle. */
static kafka_consumer_KafkaConsumer_t *create_real_consumer(void) {
    kafka_common_Error_t *err = NULL;
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"localhost:9092");
    kafka_Map_put(props, (void *)"group.id", (void *)"handle-test-group");
    kafka_Map_put(props, (void *)"group.protocol", (void *)"consumer");
    kafka_consumer_ConsumerConfig_t *config = kafka_consumer_ConsumerConfig_new(props, &err);
    assert_ok(err, "ConsumerConfig_new");
    kafka_Map_destroy(props);
    kafka_consumer_KafkaConsumer_t *kc = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &err);
    assert_ok(err, "KafkaConsumer_new");
    kafka_consumer_ConsumerConfig_destroy(config);
    TEST_ASSERT_NOT_NULL(kc);
    return kc;
}

// On a real consumer the handle reads the shared SubscriptionState, and once
// the consumer is destroyed its operations fail with "consumer destroyed".
static void test_consumer_handle_shares_state_with_real_consumer(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_real_consumer();
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
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

    kafka_consumer_KafkaConsumer_destroy(kc);

    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerHandle_commit_sync(h, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("consumer destroyed", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_consumer_ConsumerHandle_wakeup(h); /* harmless */
    kafka_consumer_ConsumerHandle_destroy(h);
}

static void assert_handle_outside_callback(kafka_common_Error_t *err) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_concurrent_modification_error(err));
    TEST_ASSERT_EQUAL_STRING("KafkaConsumer is not safe for multi-threaded access. "
                             "ConsumerHandle operations are allowed only from inside a callback the consumer "
                             "is running (ConsumerRebalanceListener, OffsetCommitCallback); from another thread "
                             "call wakeup().",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
}

// On a real consumer the handle's async operations are admitted only while
// the consumer runs a callback (ConsumerRebalanceListener, OffsetCommitCallback);
// outside one they fail with Java's ConcurrentModificationException message
// and their failure value, before anything is submitted. The `_cb` twin
// delivers the same rejection through its completion, and the sync reads and
// `wakeup` are not gated. (A mock's handle is not gated: see
// test_consumer_handle_async_ops_unsupported_on_mock.)
static void test_consumer_handle_outside_a_callback_is_concurrent_modification(void) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_KafkaConsumer_t *kc = create_real_consumer();
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
    callback_pump_t pump;
    consumer_callback_pump_install(&pump, consumer);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(consumer);
    TEST_ASSERT_NULL(assign_one(consumer, "handle-topic", 0));
    kafka_List_t *list = tp_list("handle-topic", 1);
    const kafka_common_TopicPartition_t *tp = (const kafka_common_TopicPartition_t *)kafka_List_get(list, 0);

    kafka_consumer_ConsumerHandle_commit_sync(h, &err);
    assert_handle_outside_callback(err);
    err = NULL;
    kafka_consumer_ConsumerHandle_commit_async(h, &err);
    assert_handle_outside_callback(err);
    err = NULL;
    kafka_consumer_ConsumerHandle_pause(h, list, &err);
    assert_handle_outside_callback(err);
    err = NULL;
    kafka_consumer_ConsumerHandle_seek_with_offset(h, tp, 5, &err);
    assert_handle_outside_callback(err);
    err = NULL;
    int64_t position = kafka_consumer_ConsumerHandle_position(h, tp, &err);
    assert_handle_outside_callback(err);
    err = NULL;
    TEST_ASSERT_EQUAL_INT64(-1, position); /* the failure value */
    kafka_Map_t *committed = kafka_consumer_ConsumerHandle_committed(h, list, &err);
    assert_handle_outside_callback(err);
    err = NULL;
    TEST_ASSERT_NULL(committed);
    /* current_lag waits for the background task (Java's CurrentLagEvent), so
     * it is gated like the other async operations. */
    int64_t lag = 7;
    int8_t lag_present = kafka_consumer_ConsumerHandle_current_lag(h, tp, &lag, &err);
    assert_handle_outside_callback(err);
    err = NULL;
    TEST_ASSERT_EQUAL_INT8(0, lag_present); /* the failure value */
    TEST_ASSERT_EQUAL_INT64(7, lag);        /* left unwritten */

    /* Not gated: the state reads and the cross-thread wakeup. */
    kafka_List_t *asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    kafka_List_destroy(asg);
    kafka_List_t *paused = kafka_consumer_ConsumerHandle_paused(h);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(paused));
    kafka_List_destroy(paused);
    kafka_consumer_ConsumerHandle_wakeup(h);

    /* The `_cb` twin queues the rejection on the consumer's callbacks vector. */
    completion_t completion;
    completion_init(&completion);
    kafka_consumer_ConsumerHandle_pause_cb(h, list, on_void_op, &completion);
    TEST_ASSERT_TRUE(callback_pump_until(&pump, &completion.fired, 1));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    assert_handle_outside_callback(completion.error);
    lag_completion_t lag_done;
    lag_completion_init(&lag_done);
    kafka_consumer_ConsumerHandle_current_lag_cb(h, tp, on_lag, &lag_done);
    TEST_ASSERT_TRUE(callback_pump_until(&pump, &lag_done.fired, 1));
    TEST_ASSERT_EQUAL_INT8(0, lag_done.present);
    TEST_ASSERT_EQUAL_INT64(0, lag_done.value);
    assert_handle_outside_callback(lag_done.error);

    tp_list_destroy(list);
    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_KafkaConsumer_destroy(kc);
    callback_pump_destroy(&pump);
}

// ===========================================================================
// ConsumerRebalanceListener
// ===========================================================================

// MockConsumer.rebalance: onPartitionsRevoked(removed) only when something
// was removed, then onPartitionsAssigned(added) always, with sorted lists.
static void test_rebalance_listener_assigned_then_revoked(void) {
    kafka_common_Error_t *err = NULL;
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
    kafka_List_t *asg = consumer_assignment(f.consumer);
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
    kafka_consumer_MockConsumer_rebalance(f.mock, empty, &err);
    assert_ok(err, "MockConsumer_rebalance");
    kafka_List_destroy(empty);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_STRING("t-0", probe.last_revoked_partitions);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.lost));
    asg = consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(asg));
    kafka_List_destroy(asg);
    fixture_destroy(&f);
}

// The blocking `rebalance` invokes the listener directly on the calling
// thread, the methods reporting synchronously; a NULL `on_partitions_lost`
// falls back to `on_partitions_revoked` (Java's default method).
static void test_rebalance_listener_runs_on_calling_thread(void) {
    kafka_common_Error_t *err = NULL;
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
    kafka_consumer_MockConsumer_rebalance(f.mock, empty, &err);
    assert_ok(err, "MockConsumer_rebalance");
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
    kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(defaulted, list, &err);
    assert_ok(err, "ConsumerRebalanceListener_on_partitions_lost");
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.lost));
    TEST_ASSERT_EQUAL_STRING("t-0,t-1", probe.last_revoked_partitions);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));
    kafka_consumer_ConsumerRebalanceListener_destroy(defaulted);

    /* With a lost method of its own, that one runs. */
    kafka_consumer_ConsumerRebalanceListener_t *overridden = probe_listener(&probe, 1);
    kafka_consumer_ConsumerRebalanceListener_on_partitions_lost(overridden, list, &err);
    assert_ok(err, "ConsumerRebalanceListener_on_partitions_lost");
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.lost));
    /* The other two standalone invokers. */
    kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked(overridden, list, &err);
    assert_ok(err, "ConsumerRebalanceListener_on_partitions_revoked");
    TEST_ASSERT_EQUAL_INT(3, atomic_load(&probe.revoked));
    kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned(overridden, list, &err);
    assert_ok(err, "ConsumerRebalanceListener_on_partitions_assigned");
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
        kafka_consumer_Consumer__set_callback_result(r->probe->consumer, id, NULL, NULL);
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

    kafka_List_t *asg = consumer_assignment(f.consumer);
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

// ---------------------------------------------------------------------------
// MockConsumer_lose_partitions: Java's `MockConsumer.losePartitions`
// (KAFKA-20575)
// ---------------------------------------------------------------------------

/* `losePartitions(singleton(topic-partition))`, blocking, freeing the list. */
static kafka_common_Error_t *lose_one(fixture_t *f, const char *topic, int32_t partition) {
    kafka_List_t *list = tp_list_one(topic, partition);
    kafka_common_Error_t *err = NULL;
    kafka_consumer_MockConsumer_lose_partitions(f->mock, list, &err);
    tp_list_destroy(list);
    return err;
}

// Losing a partition fires `on_partitions_lost`, not `on_partitions_revoked`,
// on the calling thread, and removes it from the assignment (Java's
// `testLosePartitionsCallsOnPartitionsLost` / `...RemovesFromAssignment`).
static void test_lose_partitions_fires_lost_and_shrinks_the_assignment(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    subscribe_probe_listener(&f, "test", &probe, 1);
    TEST_ASSERT_NULL(rebalance_to(&f, "test", 2));

    TEST_ASSERT_NULL(lose_one(&f, "test", 0));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.lost));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_STRING("test-0", probe.last_partitions);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));

    kafka_List_t *asg = consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_TopicPartition_partition(
                                   (const kafka_common_TopicPartition_t *)kafka_List_get(asg, 0)));
    kafka_List_destroy(asg);
    fixture_destroy(&f);
}

// With a NULL `lost` method the listener falls back to `on_partitions_revoked`
// with the lost partitions, as Java's default `onPartitionsLost` does.
static void test_lose_partitions_without_lost_callback_delegates_to_revoked(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    subscribe_probe(&f, "test", &probe);
    TEST_ASSERT_NULL(rebalance_to(&f, "test", 2));

    TEST_ASSERT_NULL(lose_one(&f, "test", 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.revoked));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.lost));
    TEST_ASSERT_EQUAL_STRING("test-1", probe.last_revoked_partitions);
    fixture_destroy(&f);
}

// A partition that is not assigned is rejected with Java's
// `IllegalStateException` text, the listener is not called and the assignment
// does not change (`testLosePartitionsThrowsIfNotAssigned`).
static void test_lose_partitions_rejects_an_unassigned_partition(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    subscribe_probe_listener(&f, "test", &probe, 1);
    TEST_ASSERT_NULL(rebalance_to(&f, "test", 1));

    kafka_common_Error_t *err = lose_one(&f, "test", 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("Cannot lose partitions that are not currently assigned: [test-1]",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.lost));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.revoked));

    kafka_List_t *asg = consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    kafka_List_destroy(asg);
    fixture_destroy(&f);
}

// `lose_partitions_cb` queues the listener for the pump, as `rebalance_cb`
// does: `on_partitions_lost` runs on the pumping thread, the loss does not
// complete before it reports (§31), duplicates in the list are dropped, and
// the completion fires exactly once. Losing the same partition again then
// fails through the completion, without a listener call (KAFKA-20575).
static void test_lose_partitions_cb_queues_the_listener_for_the_pump(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    subscribe_probe_listener(&f, "test", &probe, 1);
    TEST_ASSERT_NULL(rebalance_to(&f, "test", 2));

    completion_t completion;
    completion_init(&completion);
    kafka_List_t *list = tp_list_one("test", 0);
    kafka_List_add(list, kafka_common_TopicPartition_new("test", 0));
    kafka_consumer_MockConsumer_lose_partitions_cb(f.mock, list, on_void_op, &completion);
    tp_list_destroy(list);

    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    sleep_ms(50);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.lost)); /* announced, not run */
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired));
    TEST_ASSERT_EQUAL_INT32(1, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.lost));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), probe.last_thread));
    TEST_ASSERT_EQUAL_STRING("test-0", probe.last_partitions);

    /* The probe reported inline, so the completion is queued next. */
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    sleep_ms(20); /* room for a duplicate completion to show up */
    callback_pump_execute(&f.pump);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&completion.fired));
    TEST_ASSERT_NULL(completion.error);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));

    kafka_List_t *asg = consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_TopicPartition_partition(
                                   (const kafka_common_TopicPartition_t *)kafka_List_get(asg, 0)));
    kafka_List_destroy(asg);

    completion_t again;
    completion_init(&again);
    list = tp_list_one("test", 0);
    kafka_consumer_MockConsumer_lose_partitions_cb(f.mock, list, on_void_op, &again);
    tp_list_destroy(list);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &again.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&again.fired));
    TEST_ASSERT_NOT_NULL(again.error);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(again.error));
    TEST_ASSERT_EQUAL_STRING("Cannot lose partitions that are not currently assigned: [test-0]",
                             kafka_common_Error_message(again.error));
    kafka_common_Error_destroy(again.error);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.lost));
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
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_subscribe_with_topics_listener(f.consumer, topics, listener, &err);
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
    /* Java's MockConsumer replaces the assignment before onPartitionsAssigned,
     * and the mock handle reads and commits the mock's own state. */
    TEST_ASSERT_EQUAL_INT(1, probe.handle_assignment_size);
    TEST_ASSERT_TRUE(probe.handle_commit_succeeded);
    TEST_ASSERT_TRUE(probe.consumer_call_concurrent_modification);
    kafka_consumer_ConsumerHandle_destroy(probe.handle);

    /* The in-callback `ConsumerHandle_wakeup` armed the next poll. */
    update_beginning_offset(f.mock, "t", 0);
    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerRecords_t *records = kafka_consumer_Consumer_poll(f.consumer, 10, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records); /* the failure value */
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
    kafka_List_t *asg = consumer_assignment(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(asg));
    kafka_List_destroy(asg);

    /* The consumer is free again. */
    TEST_ASSERT_NULL(rebalance_to(&f, "t", 1));
    fixture_destroy(&f);
}

// A binding's foreign callback error comes back with its opaque pointer
// unchanged: the mock, like Java's `MockConsumer.rebalance`, does not wrap the
// listener's error, so it is the rebalance's result itself. A NULL release
// function leaves the pointer unowned.
static void test_listener_local_callback_error_keeps_its_opaque(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    int marker = 7;
    probe.assigned_error = kafka_common_Error_local_callback("ValueError: bad", &marker, NULL);
    subscribe_probe(&f, "t", &probe);

    kafka_common_Error_t *err = rebalance_to(&f, "t", 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_callback_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_kafka_error(err));
    TEST_ASSERT_EQUAL_INT(kafka_common_Error_LOCAL_CALLBACK, kafka_common_Error__enum(err));
    TEST_ASSERT_EQUAL_STRING("ValueError: bad", kafka_common_Error_message(err));
    const kafka_common_LocalCallbackError_t *view = kafka_common_Error_local_callback_error(err);
    TEST_ASSERT_NOT_NULL(view);
    TEST_ASSERT_EQUAL_PTR(&marker, kafka_common_LocalCallbackError_opaque(view));
    TEST_ASSERT_EQUAL_STRING("ValueError: bad", kafka_common_LocalCallbackError_message(view));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_EQUAL_INT(7, marker);
    fixture_destroy(&f);
}

// What `count_release` counts: how many times it ran, and on which thread.
typedef struct {
    atomic_int calls;
    pthread_t thread;
} release_probe_t;

static void count_release(void *opaque) {
    release_probe_t *p = (release_probe_t *)opaque;
    p->thread = pthread_self();
    atomic_fetch_add(&p->calls, 1);
}

// Given a release function, the error owns its opaque pointer: Rust releases
// it exactly once, when the last copy of the error is destroyed; with NULL it
// never does.
static void test_local_callback_error_releases_its_opaque_once(void) {
    release_probe_t owned;
    atomic_init(&owned.calls, 0);
    kafka_common_Error_t *err = kafka_common_Error_local_callback("ValueError: bad", &owned, count_release);
    const kafka_common_LocalCallbackError_t *view = kafka_common_Error_local_callback_error(err);
    TEST_ASSERT_EQUAL_PTR(&owned, kafka_common_LocalCallbackError_opaque(view));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&owned.calls));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&owned.calls));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), owned.thread));

    release_probe_t unowned;
    atomic_init(&unowned.calls, 0);
    kafka_common_Error_destroy(kafka_common_Error_local_callback("boom", &unowned, NULL));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&unowned.calls));
}

// A listener's owned foreign callback error travels through the client and
// comes back as the rebalance's result, still holding the pointer; the
// release runs once, after the last copy is gone (the result destroyed and
// the consumer with it), never while the result is alive.
static void test_listener_local_callback_error_releases_its_opaque_once(void) {
    fixture_t f;
    fixture_init(&f);
    listener_probe_t probe;
    probe_init(&probe, f.consumer, 1);
    release_probe_t released;
    atomic_init(&released.calls, 0);
    probe.assigned_error = kafka_common_Error_local_callback("ValueError: bad", &released, count_release);
    subscribe_probe(&f, "t", &probe);

    kafka_common_Error_t *err = rebalance_to(&f, "t", 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_callback_error(err));
    const kafka_common_LocalCallbackError_t *view = kafka_common_Error_local_callback_error(err);
    TEST_ASSERT_NOT_NULL(view);
    TEST_ASSERT_EQUAL_PTR(&released, kafka_common_LocalCallbackError_opaque(view));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&released.calls));
    kafka_common_Error_destroy(err);
    fixture_destroy(&f);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&released.calls));
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

    kafka_List_t *subs = consumer_subscription(f.consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(subs));
    TEST_ASSERT_EQUAL_STRING("t", (const char *)kafka_List_get(subs, 0));
    kafka_List_destroy(subs);

    TEST_ASSERT_NULL(rebalance_to(&f, "t", 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.assigned));
    fixture_destroy(&f);
}

// On a KafkaConsumer, `subscribe([])` is `unsubscribe()`: it keeps the
// listener registered by the earlier `subscribe`, which `close()` may still
// call, and a listener given with the empty list is not registered. So the
// caller frees each `self_` at its documented release point: the new
// listener's right away, the kept one's only once the consumer is destroyed.
// A freed `self_` is poisoned first, so a call through one would read
// garbage. Without a broker the group never assigns, so no method runs; the
// Python broker test (test_listener_release_broker.py) drives the close-time
// call itself.
static void test_empty_subscribe_keeps_the_listener_until_destroy(void) {
    kafka_common_Error_t *err = NULL;
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"127.0.0.1:1");
    kafka_Map_put(props, (void *)"group.id", (void *)"listener-release-test-group");
    kafka_Map_put(props, (void *)"group.protocol", (void *)"consumer");
    kafka_consumer_ConsumerConfig_t *config = kafka_consumer_ConsumerConfig_new(props, &err);
    assert_ok(err, "ConsumerConfig_new");
    kafka_Map_destroy(props);
    kafka_consumer_KafkaConsumer_t *kc = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &err);
    assert_ok(err, "KafkaConsumer_new");
    kafka_consumer_ConsumerConfig_destroy(config);
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);

    listener_probe_t *kept = malloc(sizeof(*kept));
    listener_probe_t *ignored = malloc(sizeof(*ignored));
    TEST_ASSERT_NOT_NULL(kept);
    TEST_ASSERT_NOT_NULL(ignored);
    probe_init(kept, consumer, 1);
    probe_init(ignored, consumer, 1);

    kafka_consumer_ConsumerRebalanceListener_t *listener = probe_listener(kept, 1);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"t");
    kafka_consumer_Consumer_subscribe_with_topics_listener(consumer, topics, listener, &err);
    assert_ok(err, "Consumer_subscribe_with_topics_listener");
    kafka_List_destroy(topics);
    kafka_consumer_ConsumerRebalanceListener_destroy(listener);

    // `subscribe([], ignored)`: replaces nothing, so `ignored` may go now.
    listener = probe_listener(ignored, 1);
    kafka_List_t *empty = kafka_List_new();
    kafka_consumer_Consumer_subscribe_with_topics_listener(consumer, empty, listener, &err);
    assert_ok(err, "Consumer_subscribe_with_topics_listener([])");
    kafka_consumer_ConsumerRebalanceListener_destroy(listener);
    memset(ignored, 0xAB, sizeof(*ignored));
    free(ignored);

    // A listenerless `subscribe([])` keeps it too.
    kafka_consumer_Consumer_subscribe_with_topics(consumer, empty, &err);
    assert_ok(err, "Consumer_subscribe_with_topics([])");
    kafka_List_destroy(empty);
    kafka_List_t *subs = consumer_subscription(consumer);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(subs));
    kafka_List_destroy(subs);

    kafka_consumer_CloseOptions_t *options = kafka_consumer_CloseOptions_new_with_timeout(0);
    kafka_consumer_Consumer_close_with_options(consumer, options, &err);
    assert_ok(err, "Consumer_close_with_options");
    kafka_consumer_CloseOptions_destroy(options);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&kept->revoked) + atomic_load(&kept->assigned));
    kafka_consumer_KafkaConsumer_destroy(kc);

    // The consumer is destroyed: the kept listener's release point.
    memset(kept, 0xAB, sizeof(*kept));
    free(kept);
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

// ---------------------------------------------------------------------------
// Commits through the handle from inside an `onComplete`
//
// A Java callback calls `consumer.commitAsync(offsets, callback)` on the
// consumer it captured; a C callback calls the handle. On the mock, Java's
// `commitAsync` puts the offsets and runs the callback inline, so the nested
// callback runs before the outer one returns, on the outer one's thread.
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_Consumer_t *consumer;
    kafka_consumer_ConsumerHandle_t *handle;
    commit_probe_t *inner;        /* committed from inside, through the handle */
    int cb_form;                  /* use the handle's `_cb` twin */
    completion_t *inner_completion; /* the `_cb` twin's completion */
    atomic_int calls;
    pthread_t thread;
    kafka_common_Error_t *nested_error; /* the blocking nested commit's result, owned */
    int consumer_call_concurrent_modification;
    int sequence_end;             /* after the nested commit returned */
} nesting_probe_t;

static kafka_Map_t *one_offset(kafka_common_TopicPartition_t *tp, kafka_consumer_OffsetAndMetadata_t **oam,
                               int64_t offset) {
    kafka_common_Error_t *err = NULL;
    *oam = kafka_consumer_OffsetAndMetadata_new(offset, &err);
    assert_ok(err, "OffsetAndMetadata_new");
    kafka_Map_t *map = kafka_Map_new();
    kafka_Map_put(map, tp, *oam);
    return map;
}

static void nesting_on_complete(void *self_, const kafka_Map_t *offsets, const kafka_common_Error_t *error,
                                int64_t callback_id) {
    (void)offsets;
    (void)error;
    nesting_probe_t *p = (nesting_probe_t *)self_;
    p->thread = pthread_self();
    atomic_fetch_add(&p->calls, 1);
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    kafka_Map_t *nested = one_offset(tp, &oam, 9);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(p->inner);
    if (p->cb_form) {
        kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb(p->handle, nested, cb, on_void_op,
                                                                           p->inner_completion);
    } else {
        kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback(p->handle, nested, cb, &p->nested_error);
    }
    kafka_Map_destroy(nested);
    kafka_consumer_OffsetAndMetadata_destroy(oam);
    /* The consumer itself is busy with the commit that invoked us. */
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_commit_async_with_callback(p->consumer, cb, &err);
    p->consumer_call_concurrent_modification =
        err != NULL && kafka_common_Error_is_local_concurrent_modification_error(err);
    kafka_common_Error_destroy(err);
    kafka_consumer_OffsetCommitCallback_destroy(cb); /* the registrations are copied */
    kafka_common_TopicPartition_destroy(tp);
    p->sequence_end = atomic_fetch_add(&g_sequence, 1);
    kafka_consumer_Consumer__set_callback_result(p->consumer, callback_id, NULL, NULL);
}

static int64_t committed_offset(kafka_consumer_Consumer_t *consumer, kafka_common_TopicPartition_t *tp) {
    kafka_common_Error_t *err = NULL;
    kafka_List_t *list = kafka_List_new();
    kafka_List_add(list, tp);
    kafka_Map_t *committed = NULL;
    committed = kafka_consumer_Consumer_committed(consumer, list, &err);
    assert_ok(err, "Consumer_committed");
    const kafka_consumer_OffsetAndMetadata_t *got =
        (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_get(committed, tp);
    int64_t offset = got == NULL ? -1 : kafka_consumer_OffsetAndMetadata_offset(got);
    kafka_Map_destroy(committed);
    kafka_List_destroy(list);
    return offset;
}

// Blocking: the nested commit's callback runs inside it, on the outer
// callback's thread (the calling one), before the outer callback returns; the
// nested offsets land after the outer ones.
static void test_handle_commit_with_callback_inside_callback_nests(void) {
    kafka_common_Error_t *err = NULL;
    fixture_t f;
    make_consumed_mock(&f, "test");
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);

    commit_probe_t inner;
    commit_probe_init(&inner, f.consumer, 1);
    nesting_probe_t outer;
    memset(&outer, 0, sizeof(outer));
    outer.consumer = f.consumer;
    outer.handle = h;
    outer.inner = &inner;
    atomic_init(&outer.calls, 0);
    kafka_consumer_OffsetCommitCallback_t *cb = kafka_consumer_OffsetCommitCallback_new(&outer, nesting_on_complete);

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    kafka_Map_t *offsets = one_offset(tp, &oam, 3);
    kafka_consumer_Consumer_commit_async_with_offsets_callback(f.consumer, offsets, cb, &err);
    assert_ok(err, "Consumer_commit_async_with_offsets_callback");
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);
    kafka_consumer_OffsetCommitCallback_destroy(cb);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&outer.calls));
    TEST_ASSERT_NULL(outer.nested_error);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&inner.calls));
    TEST_ASSERT_TRUE(inner.error_was_null);
    TEST_ASSERT_EQUAL_INT64(9, inner.first_offset);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), outer.thread));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), inner.thread)); /* not a runtime worker */
    TEST_ASSERT_TRUE(inner.sequence < outer.sequence_end);         /* inside the outer callback */
    TEST_ASSERT_TRUE(outer.consumer_call_concurrent_modification);
    TEST_ASSERT_EQUAL_INT64(9, committed_offset(f.consumer, tp));

    kafka_common_TopicPartition_destroy(tp);
    kafka_consumer_ConsumerHandle_destroy(h);
    fixture_destroy(&f);
}

// `_cb`: the nested commit's callback and its completion are queued for the
// pump, which runs them on the pumping thread, the callback first.
static void test_handle_commit_with_callback_cb_inside_callback_queues(void) {
    fixture_t f;
    make_consumed_mock(&f, "test");
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);

    commit_probe_t inner;
    commit_probe_init(&inner, f.consumer, 1);
    completion_t inner_completion;
    completion_init(&inner_completion);
    nesting_probe_t outer;
    memset(&outer, 0, sizeof(outer));
    outer.consumer = f.consumer;
    outer.handle = h;
    outer.inner = &inner;
    outer.cb_form = 1;
    outer.inner_completion = &inner_completion;
    atomic_init(&outer.calls, 0);
    kafka_consumer_OffsetCommitCallback_t *cb = kafka_consumer_OffsetCommitCallback_new(&outer, nesting_on_complete);

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    kafka_Map_t *offsets = one_offset(tp, &oam, 3);
    completion_t completion;
    completion_init(&completion);
    kafka_consumer_Consumer_commit_async_with_offsets_callback_cb(f.consumer, offsets, cb, on_void_op, &completion);
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);

    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &completion.fired, 1));
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &inner_completion.fired, 1));
    kafka_consumer_OffsetCommitCallback_destroy(cb);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&outer.calls));
    TEST_ASSERT_NULL(completion.error);
    TEST_ASSERT_NULL(inner_completion.error);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&inner.calls));
    TEST_ASSERT_EQUAL_INT64(9, inner.first_offset);
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), inner.thread));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), inner_completion.thread));
    TEST_ASSERT_TRUE(inner.sequence < inner_completion.sequence); /* callback, then completion */
    TEST_ASSERT_EQUAL_INT64(9, committed_offset(f.consumer, tp));

    kafka_common_TopicPartition_destroy(tp);
    kafka_consumer_ConsumerHandle_destroy(h);
    fixture_destroy(&f);
}

// Once the consumer is destroyed the handle's commits fail with
// LocalIllegalState; the `_cb` twin fires its completion inline.
static void test_handle_commit_with_callback_after_destroy(void) {
    fixture_t f;
    fixture_init(&f);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(f.consumer);
    fixture_destroy(&f);

    commit_probe_t probe;
    commit_probe_init(&probe, NULL, 1);
    kafka_consumer_OffsetCommitCallback_t *cb = commit_probe_callback(&probe);
    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerHandle_commit_async_with_callback(h, cb, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_EQUAL_STRING("consumer destroyed", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("test", 0);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    kafka_Map_t *offsets = one_offset(tp, &oam, 1);
    err = NULL;
    kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback(h, offsets, cb, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    kafka_common_Error_destroy(err);

    completion_t completion;
    completion_init(&completion);
    kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb(h, offsets, cb, on_void_op, &completion);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&completion.fired));
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(completion.error));
    kafka_common_Error_destroy(completion.error);
    completion_init(&completion);
    kafka_consumer_ConsumerHandle_commit_async_with_callback_cb(h, cb, on_void_op, &completion);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&completion.fired));
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(completion.error));
    kafka_common_Error_destroy(completion.error);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.calls));

    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);
    kafka_common_TopicPartition_destroy(tp);
    kafka_consumer_OffsetCommitCallback_destroy(cb);
    kafka_consumer_ConsumerHandle_destroy(h);
}

int main(void) {
    atomic_init(&g_sequence, 0);
    UNITY_BEGIN();
    RUN_TEST(test_commit_async_with_callback_fires_with_offsets_null_error);
    RUN_TEST(test_commit_async_with_offsets_callback_echoes_offsets);
    RUN_TEST(test_commit_async_with_callback_cb_queues_the_callback);
    RUN_TEST(test_commit_returns_only_after_callback_reports);
    RUN_TEST(test_commit_callback_error_propagates_from_commit);
    RUN_TEST(test_commit_callback_error_propagates_from_commit_cb);
    RUN_TEST(test_offset_commit_callback_standalone_invoker);
    RUN_TEST(test_consumer_handle_new_destroy);
    RUN_TEST(test_consumer_handle_sync_getters_read_the_mock);
    RUN_TEST(test_mock_handle_commits_commit_into_the_mock);
    RUN_TEST(test_consumer_handle_async_ops_unsupported_on_mock);
    RUN_TEST(test_handle_commit_with_callback_inside_callback_nests);
    RUN_TEST(test_handle_commit_with_callback_cb_inside_callback_queues);
    RUN_TEST(test_handle_commit_with_callback_after_destroy);
    RUN_TEST(test_consumer_handle_wakeup);
    RUN_TEST(test_consumer_handle_usable_while_op_in_flight);
    RUN_TEST(test_consumer_handle_shares_state_with_real_consumer);
    RUN_TEST(test_consumer_handle_outside_a_callback_is_concurrent_modification);
    RUN_TEST(test_rebalance_listener_assigned_then_revoked);
    RUN_TEST(test_rebalance_listener_runs_on_calling_thread);
    RUN_TEST(test_rebalance_cb_queues_the_listener_for_the_pump);
    RUN_TEST(test_rebalance_blocks_until_listener_reports);
    RUN_TEST(test_rebalance_requires_a_subscription);
    RUN_TEST(test_lose_partitions_fires_lost_and_shrinks_the_assignment);
    RUN_TEST(test_lose_partitions_without_lost_callback_delegates_to_revoked);
    RUN_TEST(test_lose_partitions_rejects_an_unassigned_partition);
    RUN_TEST(test_lose_partitions_cb_queues_the_listener_for_the_pump);
    RUN_TEST(test_subscribe_with_listener_on_manually_assigned_consumer_fails);
    RUN_TEST(test_listener_calls_consumer_handle_no_deadlock);
    RUN_TEST(test_listener_error_propagates);
    RUN_TEST(test_listener_local_callback_error_keeps_its_opaque);
    RUN_TEST(test_local_callback_error_releases_its_opaque_once);
    RUN_TEST(test_listener_local_callback_error_releases_its_opaque_once);
    RUN_TEST(test_subscribe_with_topics_listener_cb);
    RUN_TEST(test_empty_subscribe_keeps_the_listener_until_destroy);
    RUN_TEST(test_destroy_runs_pending_rebalance_cb_listener_and_completion_once);
    return UNITY_END();
}
