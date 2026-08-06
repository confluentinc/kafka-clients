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

// User-callback bindings of the consumer C FFI: the `OffsetCommitCallback`
// equivalent (`kafka_consumer_Consumer_commit_async_with_callback` and
// `..._commit_async_offsets_with_callback`), the `ConsumerRebalanceListener`
// equivalent (`kafka_consumer_ConsumerRebalanceListener_t` +
// `kafka_consumer_Consumer_subscribe_with_listener`), and the reentrancy handle
// (`kafka_consumer_ConsumerHandle_t`) user callbacks use to call back into the
// consumer.
//
// All tests are MockConsumer-backed. The mock's core `commit_async_impl` awaits
// `on_complete` inline (mirroring Java's `MockConsumer.commitAsync`, which calls
// `callback.onComplete` synchronously), so a commit through these entry points
// exercises the full adapter round trip: app thread -> dispatcher thread (C
// callback) -> back to the app thread. `kafka_consumer_MockConsumer_rebalance`
// (Java's `MockConsumer.rebalance`) drives the rebalance callbacks the same way.
//
// Not covered here: `on_partitions_lost` with a NULL `lost` callback delegating
// to `on_partitions_revoked` (Java's default method). The mock never fires
// `on_partitions_lost` — neither does Java's — so the delegation is asserted in
// the Rust unit tests of `src/ffi/consumer.rs`
// (`on_partitions_lost_delegates_to_revoked_when_no_lost_callback`), which invoke
// the adapter directly.

#include <confluent_kafka.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <time.h>
#include <pthread.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/* Captures what a commit callback observed. */
typedef struct {
    atomic_int fired;
    int had_error;
    int32_t error_code;
    /* Snapshot of the delivered offset map (first `entry_count` entries). */
    int32_t entry_count;
    char topics[4][64];
    int32_t partitions[4];
    int64_t offsets[4];
    char metadata[4][64];
    int has_leader_epoch[4];
    int32_t leader_epochs[4];
    pthread_t thread_id;
    /* Set as the callback's LAST action; see the "returns only after" test. */
    atomic_int done;
    /* Non-zero: sleep ~200ms before setting `done`. */
    int slow;
} commit_result_t;

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

/* Commit callback: records the delivered map/error, then destroys both. */
static void on_commit(kafka_consumer_OffsetMap_t *offsets,
                      kafka_common_KafkaError_t *error,
                      void *user_data) {
    commit_result_t *r = (commit_result_t *)user_data;
    r->thread_id = pthread_self();

    if (offsets != NULL) {
        int32_t count = kafka_consumer_OffsetMap_count(offsets);
        r->entry_count = count;
        for (int32_t i = 0; i < count && i < 4; i++) {
            const kafka_consumer_TopicPartition_t *key =
                kafka_consumer_OffsetMap_get_key(offsets, i);
            const kafka_consumer_OffsetAndMetadata_t *value =
                kafka_consumer_OffsetMap_get_value(offsets, i);
            snprintf(r->topics[i], sizeof(r->topics[i]), "%s",
                     kafka_consumer_TopicPartition_topic(key));
            r->partitions[i] = kafka_consumer_TopicPartition_partition(key);
            r->offsets[i] = kafka_consumer_OffsetAndMetadata_offset(value);
            snprintf(r->metadata[i], sizeof(r->metadata[i]), "%s",
                     kafka_consumer_OffsetAndMetadata_metadata(value));
            int32_t epoch = 0;
            r->has_leader_epoch[i] =
                kafka_consumer_OffsetAndMetadata_leader_epoch(value, &epoch) ? 1 : 0;
            r->leader_epochs[i] = epoch;
        }
        /* The callee owns the delivered map. */
        kafka_consumer_OffsetMap_destroy(offsets);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_KafkaError_code(error);
        /* The callee owns the delivered error. */
        kafka_common_KafkaError_destroy(error);
    }

    atomic_fetch_add(&r->fired, 1);
    if (r->slow) {
        struct timespec ts = {0, 200000000}; /* 200ms */
        nanosleep(&ts, NULL);
    }
    /* LAST action: proves the FFI call waited for the callback to return. */
    atomic_store(&r->done, 1);
}

static void commit_result_init(commit_result_t *r) {
    memset(r, 0, sizeof(*r));
    atomic_init(&r->fired, 0);
    atomic_init(&r->done, 0);
}

/* Assigns a single (topic, partition) to the consumer. */
static kafka_common_KafkaError_t *assign_one(kafka_consumer_Consumer_t *c,
                                            const char *topic,
                                            int32_t partition) {
    const char *topics[1] = {topic};
    int32_t partitions[1] = {partition};
    return kafka_consumer_Consumer_assign(c, topics, partitions, 1);
}

/* A one-(topic,partition) assigned, earliest-positioned mock consumer. */
static kafka_consumer_Consumer_t *make_assigned_mock(const char *topic, int32_t partition) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(c);
    TEST_ASSERT_NULL(assign_one(c, topic, partition));
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_update_beginning_offsets(c, topic, partition, 0));
    return c;
}

/* A heap counter used as `user_data`, so a use-after-free would be detectable. */
typedef struct {
    atomic_int destroy_calls;
    atomic_int callback_calls;
} destroy_counter_t;

static void on_user_data_destroy(void *user_data) {
    destroy_counter_t *counter = (destroy_counter_t *)user_data;
    atomic_fetch_add(&counter->destroy_calls, 1);
}

/* Commit callback paired with `destroy_counter_t` user_data: it only counts and
 * frees the delivered handles (it must NOT reinterpret `user_data` as a
 * `commit_result_t`). */
static void on_commit_counting(kafka_consumer_OffsetMap_t *offsets,
                               kafka_common_KafkaError_t *error,
                               void *user_data) {
    destroy_counter_t *counter = (destroy_counter_t *)user_data;
    kafka_consumer_OffsetMap_destroy(offsets);
    kafka_common_KafkaError_destroy(error);
    atomic_fetch_add(&counter->callback_calls, 1);
}

// ---------------------------------------------------------------------------
// commit_async_with_callback: callback gets the consumed offsets, error NULL
// ---------------------------------------------------------------------------

static void test_commit_async_with_callback_fires_with_offsets_null_error(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    const uint8_t value[] = {0x76, 0x61, 0x6c}; /* "val" */
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_add_record(c, "test", 0, 0,
                                                            NULL, -1,
                                                            value, (int32_t)sizeof(value)));
    kafka_common_KafkaError_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_Consumer_poll(c, 100, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));
    kafka_consumer_ConsumerRecords_destroy(records);

    commit_result_t result;
    commit_result_init(&result);

    kafka_common_KafkaError_t *err =
        kafka_consumer_Consumer_commit_async_with_callback(c, on_commit, &result, NULL);
    TEST_ASSERT_NULL(err);

    /* The mock invokes the callback inline, so it has already fired. */
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_FALSE(result.had_error);
    /* The consumed position after the offset-0 record is 1. */
    TEST_ASSERT_EQUAL_INT32(1, result.entry_count);
    TEST_ASSERT_EQUAL_STRING("test", result.topics[0]);
    TEST_ASSERT_EQUAL_INT32(0, result.partitions[0]);
    TEST_ASSERT_EQUAL_INT64(1, result.offsets[0]);

    /* The committed offsets are observable through `committed()`. */
    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    kafka_consumer_OffsetMap_t *committed = NULL;
    err = kafka_consumer_Consumer_committed(c, topics, partitions, 1, &committed);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(committed);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_OffsetMap_count(committed));
    TEST_ASSERT_EQUAL_INT64(
        1, kafka_consumer_OffsetAndMetadata_offset(kafka_consumer_OffsetMap_get_value(committed, 0)));
    kafka_consumer_OffsetMap_destroy(committed);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// commit_async_offsets_with_callback echoes the explicit offsets back
// ---------------------------------------------------------------------------

static void test_commit_async_offsets_with_callback_echoes_offsets(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    int64_t offsets[1] = {42};
    int32_t leader_epochs[1] = {7};
    const char *metadata[1] = {"meta-42"};

    commit_result_t result;
    commit_result_init(&result);

    kafka_common_KafkaError_t *err =
        kafka_consumer_Consumer_commit_async_offsets_with_callback(
            c, topics, partitions, offsets, leader_epochs, metadata, 1,
            on_commit, &result, NULL);
    TEST_ASSERT_NULL(err);

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_EQUAL_INT32(1, result.entry_count);
    TEST_ASSERT_EQUAL_STRING("test", result.topics[0]);
    TEST_ASSERT_EQUAL_INT32(0, result.partitions[0]);
    TEST_ASSERT_EQUAL_INT64(42, result.offsets[0]);
    TEST_ASSERT_EQUAL_STRING("meta-42", result.metadata[0]);
    TEST_ASSERT_TRUE(result.has_leader_epoch[0]);
    TEST_ASSERT_EQUAL_INT32(7, result.leader_epochs[0]);

    /* A null metadata array and a negative epoch mean "none". */
    commit_result_t bare;
    commit_result_init(&bare);
    offsets[0] = 43;
    leader_epochs[0] = -1;
    err = kafka_consumer_Consumer_commit_async_offsets_with_callback(
        c, topics, partitions, offsets, leader_epochs, NULL, 1,
        on_commit, &bare, NULL);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&bare.fired));
    TEST_ASSERT_EQUAL_INT64(43, bare.offsets[0]);
    TEST_ASSERT_EQUAL_STRING("", bare.metadata[0]);
    TEST_ASSERT_FALSE(bare.has_leader_epoch[0]);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// The callback runs on the dispatcher thread, not the calling thread
// ---------------------------------------------------------------------------

static void test_commit_callback_runs_on_dispatcher_thread(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    commit_result_t result;
    commit_result_init(&result);

    kafka_common_KafkaError_t *err =
        kafka_consumer_Consumer_commit_async_with_callback(c, on_commit, &result, NULL);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_TRUE(wait_for(&result.fired, 1));

    /* User code never runs on the caller's thread (nor on a tokio worker). */
    TEST_ASSERT_NOT_EQUAL(pthread_self(), result.thread_id);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// user_data_destroy fires exactly once — on success and on marshaling failure
// ---------------------------------------------------------------------------

static void test_commit_callback_user_data_destroy_fires_exactly_once(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    /* Success path: the callback fires, then the registration is released. */
    destroy_counter_t *counter = (destroy_counter_t *)malloc(sizeof(destroy_counter_t));
    TEST_ASSERT_NOT_NULL(counter);
    atomic_init(&counter->destroy_calls, 0);
    atomic_init(&counter->callback_calls, 0);

    kafka_common_KafkaError_t *err = kafka_consumer_Consumer_commit_async_with_callback(
        c, on_commit_counting, counter, on_user_data_destroy);
    TEST_ASSERT_NULL(err);
    /* The mock drops the registration inside the commit, so by the time the
     * call returns the callback and the hook have each run exactly once. */
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&counter->callback_calls));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&counter->destroy_calls));
    free(counter);

    /* Marshaling failure: a negative offset is rejected before the callback can
     * be registered, so the callback never fires — but the `user_data` transfer
     * is unconditional, so the destroy hook still runs exactly once. */
    destroy_counter_t *failed = (destroy_counter_t *)malloc(sizeof(destroy_counter_t));
    TEST_ASSERT_NOT_NULL(failed);
    atomic_init(&failed->destroy_calls, 0);
    atomic_init(&failed->callback_calls, 0);

    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    int64_t offsets[1] = {-1}; /* invalid: "Invalid negative offset" */
    int32_t leader_epochs[1] = {-1};

    err = kafka_consumer_Consumer_commit_async_offsets_with_callback(
        c, topics, partitions, offsets, leader_epochs, NULL, 1,
        on_commit_counting, failed, on_user_data_destroy);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&failed->callback_calls));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&failed->destroy_calls));
    free(failed);

    /* A null destroy hook is accepted (C side keeps ownership). */
    commit_result_t result;
    commit_result_init(&result);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_async_with_callback(c, on_commit, &result, NULL));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// The commit call returns only after the callback has returned
//
// The adapter dispatches the callback to the dispatcher thread and awaits it
// (Java runs `onComplete` on the thread inside `poll()`/`commitSync()`), so a
// callback that sleeps 200ms and sets `done` as its very last action must have
// set it by the time the FFI call returns. Fire-and-forget dispatch would fail
// this.
// ---------------------------------------------------------------------------

static void test_commit_returns_only_after_callback_returns(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    commit_result_t result;
    commit_result_init(&result);
    result.slow = 1;

    kafka_common_KafkaError_t *err =
        kafka_consumer_Consumer_commit_async_with_callback(c, on_commit, &result, NULL);
    TEST_ASSERT_NULL(err);

    /* No waiting here: both must already be true. */
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.done));

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// kafka_consumer_ConsumerHandle_t — the reentrancy handle
// ---------------------------------------------------------------------------

/* ConcurrentModificationError maps to the UnknownServerError numeric code (-1),
 * since it carries no embedded Kafka `Errors` value (see `KafkaError::error()`).
 * A handle op must NEVER produce it. */
#define CONCURRENT_MODIFICATION_CODE (-1)

/* Substring of the core's `unsupported_version` message for a handle obtained
 * from a MockConsumer (`ConsumerHandle::async_state`). */
#define MOCK_HANDLE_UNSUPPORTED "not supported on a MockConsumer handle"

/* Asserts that `err` is a non-null "unsupported on a mock handle" error, then
 * frees it. */
static void assert_unsupported_on_mock(kafka_common_KafkaError_t *err) {
    TEST_ASSERT_NOT_NULL(err);
    const char *message = kafka_common_KafkaError_message(err);
    TEST_ASSERT_NOT_NULL(message);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(message, MOCK_HANDLE_UNSUPPORTED), message);
    kafka_common_KafkaError_destroy(err);
}

static void test_consumer_handle_new_destroy(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h);

    /* Handles are independent: a second one can be taken while the first is
     * alive, and destroying one leaves the other (and the consumer) usable. */
    kafka_consumer_ConsumerHandle_t *h2 = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h2);
    TEST_ASSERT_NOT_EQUAL(h, h2);
    kafka_consumer_ConsumerHandle_destroy(h2);

    kafka_consumer_TopicPartitionList_t *asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_NOT_NULL(asg);
    kafka_consumer_TopicPartitionList_destroy(asg);

    /* NULL is a no-op. */
    kafka_consumer_ConsumerHandle_destroy(NULL);
    kafka_consumer_ConsumerHandle_wakeup(NULL);

    /* Destroy the handle before the consumer (documented contract). */
    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// Sync getters never return NULL (no guard) but are empty on a mock handle,
// even when the owning consumer has an assignment — the mock's ConsumerHandle
// carries only the shared wakeup flag, not the SubscriptionState (core
// behavior, see `ConsumerHandleInner::Mock`).
// ---------------------------------------------------------------------------

static void test_consumer_handle_sync_getters_empty_on_mock(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h);

    /* The consumer itself does see the assignment... */
    kafka_consumer_TopicPartitionList_t *owner_asg = kafka_consumer_Consumer_assignment(c);
    TEST_ASSERT_NOT_NULL(owner_asg);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_TopicPartitionList_count(owner_asg));
    kafka_consumer_TopicPartitionList_destroy(owner_asg);

    /* ...while the mock-derived handle reports empty sets. */
    kafka_consumer_TopicPartitionList_t *asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_NOT_NULL(asg);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_TopicPartitionList_count(asg));
    kafka_consumer_TopicPartitionList_destroy(asg);

    kafka_consumer_StringList_t *sub = kafka_consumer_ConsumerHandle_subscription(h);
    TEST_ASSERT_NOT_NULL(sub);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_StringList_count(sub));
    kafka_consumer_StringList_destroy(sub);

    kafka_consumer_TopicPartitionList_t *paused = kafka_consumer_ConsumerHandle_paused(h);
    TEST_ASSERT_NOT_NULL(paused);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_TopicPartitionList_count(paused));
    kafka_consumer_TopicPartitionList_destroy(paused);

    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// Every async op on a mock-derived handle fails with the core's
// `unsupported_version` error — it never hangs and never panics.
// ---------------------------------------------------------------------------

static void test_consumer_handle_async_ops_unsupported_on_mock(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h);

    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    int64_t offsets[1] = {1};
    int32_t leader_epochs[1] = {-1};
    int64_t timestamps[1] = {0};

    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_commit_sync(h));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_commit_async(h));
    assert_unsupported_on_mock(
        kafka_consumer_ConsumerHandle_commit_sync_offsets(h, topics, partitions, offsets, leader_epochs, NULL, 1));
    assert_unsupported_on_mock(
        kafka_consumer_ConsumerHandle_commit_async_offsets(h, topics, partitions, offsets, leader_epochs, NULL, 1));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_assign(h, topics, partitions, 1));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_seek(h, "test", 0, 5));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_seek_with_metadata(h, "test", 0, 5, -1, NULL));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_seek_to_beginning(h, topics, partitions, 1));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_seek_to_end(h, topics, partitions, 1));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_pause(h, topics, partitions, 1));
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_resume(h, topics, partitions, 1));

    /* Value-returning ops leave their out-params untouched on failure. */
    int64_t position = -7;
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_position(h, "test", 0, &position));
    TEST_ASSERT_EQUAL_INT64(-7, position);
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_position_timeout(h, "test", 0, 100, &position));
    TEST_ASSERT_EQUAL_INT64(-7, position);

    kafka_consumer_OffsetMap_t *committed = NULL;
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_committed(h, topics, partitions, 1, &committed));
    TEST_ASSERT_NULL(committed);

    kafka_consumer_LongOffsetMap_t *long_map = NULL;
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_beginning_offsets(h, topics, partitions, 1, &long_map));
    TEST_ASSERT_NULL(long_map);
    assert_unsupported_on_mock(kafka_consumer_ConsumerHandle_end_offsets(h, topics, partitions, 1, &long_map));
    TEST_ASSERT_NULL(long_map);

    kafka_consumer_OffsetAndTimestampMap_t *ts_map = NULL;
    assert_unsupported_on_mock(
        kafka_consumer_ConsumerHandle_offsets_for_times(h, topics, partitions, timestamps, 1, &ts_map));
    TEST_ASSERT_NULL(ts_map);

    /* A marshaling failure is reported before the op is driven, so it yields
     * the validation error rather than the unsupported-on-mock one. */
    int64_t bad_offsets[1] = {-1};
    kafka_common_KafkaError_t *err =
        kafka_consumer_ConsumerHandle_commit_sync_offsets(h, topics, partitions, bad_offsets, leader_epochs, NULL, 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_KafkaError_message(err), "negative offset"));
    kafka_common_KafkaError_destroy(err);

    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// wakeup() through the handle arms the same flag the next poll observes
// (deterministic: nothing else is in flight, so nothing can consume the flag).
// ---------------------------------------------------------------------------

static void test_consumer_handle_wakeup(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h);

    kafka_consumer_ConsumerHandle_wakeup(h);

    kafka_common_KafkaError_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records = kafka_consumer_Consumer_poll(c, 10, &poll_err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_NOT_NULL(poll_err);
    kafka_common_KafkaError_destroy(poll_err);

    /* The flag was consumed, so the next poll succeeds. */
    poll_err = NULL;
    records = kafka_consumer_Consumer_poll(c, 10, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    TEST_ASSERT_NOT_NULL(records);
    kafka_consumer_ConsumerRecords_destroy(records);

    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// The handle bypasses the access guard
//
// A commit callback runs on the dispatcher thread *while the app thread that
// called commit still holds the owner guard* (the mock invokes `on_complete`
// inline inside the guarded commit). From there:
//   - a plain `kafka_consumer_Consumer_*` op is rejected with
//     ConcurrentModification (proving the guard really is held), while
//   - `kafka_consumer_ConsumerHandle_*` ops are NOT rejected — the getter
//     returns a list and the async op returns the mock's unsupported error.
// This is exactly the reentrancy the rebalance listener (Phase 5) needs.
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_Consumer_t *consumer;
    kafka_consumer_ConsumerHandle_t *handle;
    atomic_int fired;
    int handle_assignment_non_null;
    int32_t handle_assignment_count;
    int handle_commit_error_code;
    char handle_commit_message[192];
    int owner_commit_error_code;
} guard_probe_t;

static void on_commit_probing_guard(kafka_consumer_OffsetMap_t *offsets,
                                   kafka_common_KafkaError_t *error,
                                   void *user_data) {
    guard_probe_t *p = (guard_probe_t *)user_data;
    kafka_consumer_OffsetMap_destroy(offsets);
    kafka_common_KafkaError_destroy(error);

    /* The guard IS held right now: a plain consumer op is rejected. */
    kafka_common_KafkaError_t *owner_err = kafka_consumer_Consumer_commit_sync(p->consumer);
    p->owner_commit_error_code = owner_err != NULL ? kafka_common_KafkaError_code(owner_err) : 0;
    kafka_common_KafkaError_destroy(owner_err);

    /* The handle bypasses it: the getter succeeds... */
    kafka_consumer_TopicPartitionList_t *asg = kafka_consumer_ConsumerHandle_assignment(p->handle);
    p->handle_assignment_non_null = asg != NULL;
    p->handle_assignment_count = asg != NULL ? kafka_consumer_TopicPartitionList_count(asg) : -1;
    kafka_consumer_TopicPartitionList_destroy(asg);

    /* ...and the async op reaches the core (mock => unsupported, NOT the
     * ConcurrentModification the guarded path would have produced). */
    kafka_common_KafkaError_t *handle_err = kafka_consumer_ConsumerHandle_commit_sync(p->handle);
    if (handle_err != NULL) {
        p->handle_commit_error_code = kafka_common_KafkaError_code(handle_err);
        snprintf(p->handle_commit_message, sizeof(p->handle_commit_message), "%s",
                 kafka_common_KafkaError_message(handle_err));
        kafka_common_KafkaError_destroy(handle_err);
    }

    atomic_fetch_add(&p->fired, 1);
}

static void test_consumer_handle_usable_while_op_in_flight(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h);

    guard_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    atomic_init(&probe.fired, 0);
    probe.consumer = c;
    probe.handle = h;
    probe.handle_commit_error_code = INT32_MAX; /* sentinel: callback ran */

    kafka_common_KafkaError_t *err =
        kafka_consumer_Consumer_commit_async_with_callback(c, on_commit_probing_guard, &probe, NULL);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_TRUE(wait_for(&probe.fired, 1));

    /* Control: the owner guard was held, so the plain consumer op was rejected. */
    TEST_ASSERT_EQUAL_INT(CONCURRENT_MODIFICATION_CODE, probe.owner_commit_error_code);

    /* The handle getter was not rejected (it returned a list, not NULL). */
    TEST_ASSERT_TRUE(probe.handle_assignment_non_null);
    TEST_ASSERT_EQUAL_INT32(0, probe.handle_assignment_count);

    /* The handle commit reached the core: the mock's unsupported error, never
     * ConcurrentModification. */
    TEST_ASSERT_NOT_EQUAL(INT32_MAX, probe.handle_commit_error_code);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(probe.handle_commit_message, MOCK_HANDLE_UNSUPPORTED),
                                 probe.handle_commit_message);

    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// The Async (real KafkaConsumer) arm of the handle
//
// No broker is needed: the handle shares the consumer's `SubscriptionState`, so
// `subscription()` reflects a `subscribe()` made on the consumer, and
// `position()` on a partition that is not assigned fails fast with an
// IllegalState error (never `unsupported`, which is the mock-only outcome).
// ---------------------------------------------------------------------------

static void test_consumer_handle_shares_state_with_real_consumer(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        "group.id",          "handle-test-group",
        "group.protocol",    "consumer",
        NULL
    };
    kafka_consumer_ConsumerProperties_t *props = kafka_consumer_ConsumerProperties_from_configs(configs);
    TEST_ASSERT_NOT_NULL(props);
    kafka_common_KafkaError_t *err = NULL;
    kafka_consumer_Consumer_t *c = kafka_consumer_KafkaConsumer_new(props, &err);
    kafka_consumer_ConsumerProperties_destroy(props);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(c);

    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h);

    const char *topics[1] = {"handle-topic"};
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe(c, topics, 1));

    /* The handle reads the consumer's shared SubscriptionState. */
    kafka_consumer_StringList_t *sub = kafka_consumer_ConsumerHandle_subscription(h);
    TEST_ASSERT_NOT_NULL(sub);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_StringList_count(sub));
    TEST_ASSERT_EQUAL_STRING("handle-topic", kafka_consumer_StringList_get(sub, 0));
    kafka_consumer_StringList_destroy(sub);

    /* Nothing assigned yet (no broker), so these are empty but non-null. */
    kafka_consumer_TopicPartitionList_t *asg = kafka_consumer_ConsumerHandle_assignment(h);
    TEST_ASSERT_NOT_NULL(asg);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_TopicPartitionList_count(asg));
    kafka_consumer_TopicPartitionList_destroy(asg);

    kafka_consumer_TopicPartitionList_t *paused = kafka_consumer_ConsumerHandle_paused(h);
    TEST_ASSERT_NOT_NULL(paused);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_TopicPartitionList_count(paused));
    kafka_consumer_TopicPartitionList_destroy(paused);

    /* An async op reaches the real implementation: `position` on a partition
     * that is not assigned fails immediately (no broker round trip). */
    int64_t position = -7;
    kafka_common_KafkaError_t *pos_err =
        kafka_consumer_ConsumerHandle_position_timeout(h, "handle-topic", 0, 100, &position);
    TEST_ASSERT_NOT_NULL(pos_err);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(kafka_common_KafkaError_message(pos_err), "partitions assigned"),
                                 kafka_common_KafkaError_message(pos_err));
    TEST_ASSERT_EQUAL_INT64(-7, position);
    kafka_common_KafkaError_destroy(pos_err);

    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// kafka_consumer_ConsumerRebalanceListener_t — the rebalance listener
// ---------------------------------------------------------------------------

/* Snapshot of a partition list delivered to a listener callback. */
typedef struct {
    int32_t count;
    char topics[4][64];
    int32_t partitions[4];
} tp_snapshot_t;

/* Snapshots `list` and then destroys it — the callee owns the delivered list. */
static void take_and_destroy_list(kafka_consumer_TopicPartitionList_t *list, tp_snapshot_t *out) {
    out->count = list != NULL ? kafka_consumer_TopicPartitionList_count(list) : -1;
    for (int32_t i = 0; i < out->count && i < 4; i++) {
        const kafka_consumer_TopicPartition_t *tp = kafka_consumer_TopicPartitionList_get(list, i);
        snprintf(out->topics[i], sizeof(out->topics[i]), "%s", kafka_consumer_TopicPartition_topic(tp));
        out->partitions[i] = kafka_consumer_TopicPartition_partition(tp);
    }
    kafka_consumer_TopicPartitionList_destroy(list);
}

/* What the plain revoked/assigned listener callbacks observed. */
typedef struct {
    atomic_int revoked_calls;
    atomic_int assigned_calls;
    tp_snapshot_t revoked;
    tp_snapshot_t assigned;
    pthread_t revoked_thread;
    pthread_t assigned_thread;
    /* Non-zero: the assigned callback returns an error handle. */
    int assigned_fails;
} listener_result_t;

static void listener_result_init(listener_result_t *r) {
    memset(r, 0, sizeof(*r));
    atomic_init(&r->revoked_calls, 0);
    atomic_init(&r->assigned_calls, 0);
    r->revoked.count = -1;
    r->assigned.count = -1;
}

static kafka_common_KafkaError_t *on_revoked(kafka_consumer_TopicPartitionList_t *partitions, void *user_data) {
    listener_result_t *r = (listener_result_t *)user_data;
    r->revoked_thread = pthread_self();
    take_and_destroy_list(partitions, &r->revoked);
    atomic_fetch_add(&r->revoked_calls, 1);
    return NULL;
}

#define LISTENER_ERROR_MESSAGE "listener refused the assignment"

static kafka_common_KafkaError_t *on_assigned(kafka_consumer_TopicPartitionList_t *partitions, void *user_data) {
    listener_result_t *r = (listener_result_t *)user_data;
    r->assigned_thread = pthread_self();
    take_and_destroy_list(partitions, &r->assigned);
    atomic_fetch_add(&r->assigned_calls, 1);
    if (r->assigned_fails) {
        /* The C equivalent of the Java listener throwing: ownership of the
         * handle transfers to the client, which turns it back into an `Err`. */
        return kafka_common_KafkaError_new(-1, LISTENER_ERROR_MESSAGE);
    }
    return NULL;
}

/* A mock consumer subscribed to `topic` with a listener over `result`. */
static kafka_consumer_Consumer_t *make_subscribed_mock(const char *topic, listener_result_t *result) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(c);
    kafka_consumer_ConsumerRebalanceListener_t *listener =
        kafka_consumer_ConsumerRebalanceListener_new(on_revoked, on_assigned, NULL, result, NULL);
    TEST_ASSERT_NOT_NULL(listener);
    const char *topics[1] = {topic};
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_listener(c, topics, 1, listener));
    return c;
}

/* Drives `MockConsumer.rebalance` with a (topic, partition) list. */
static kafka_common_KafkaError_t *rebalance_to(kafka_consumer_Consumer_t *c,
                                              const char *const *topics,
                                              const int32_t *partitions,
                                              int32_t count) {
    return kafka_consumer_MockConsumer_rebalance(c, topics, partitions, count);
}

// ---------------------------------------------------------------------------
// assigned gets the ADDED partitions, revoked gets the REMOVED ones
//
// Mirrors Java's `MockConsumer.rebalance`: `onPartitionsRevoked` fires only when
// something was removed, `onPartitionsAssigned` fires with the newly added
// partitions (and fires even when nothing was added).
// ---------------------------------------------------------------------------

static void test_rebalance_listener_assigned_then_revoked(void) {
    listener_result_t result;
    listener_result_init(&result);
    kafka_consumer_Consumer_t *c = make_subscribed_mock("test", &result);

    /* First rebalance: nothing assigned before, so both partitions are added. */
    const char *topics2[2] = {"test", "test"};
    int32_t partitions2[2] = {0, 1};
    TEST_ASSERT_NULL(rebalance_to(c, topics2, partitions2, 2));

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.assigned_calls));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.revoked_calls)); /* nothing removed */
    TEST_ASSERT_EQUAL_INT32(2, result.assigned.count);
    TEST_ASSERT_EQUAL_STRING("test", result.assigned.topics[0]);
    TEST_ASSERT_EQUAL_INT32(0, result.assigned.partitions[0]);
    TEST_ASSERT_EQUAL_STRING("test", result.assigned.topics[1]);
    TEST_ASSERT_EQUAL_INT32(1, result.assigned.partitions[1]);

    /* The assignment really was applied. */
    kafka_consumer_TopicPartitionList_t *asg = kafka_consumer_Consumer_assignment(c);
    TEST_ASSERT_EQUAL_INT32(2, kafka_consumer_TopicPartitionList_count(asg));
    kafka_consumer_TopicPartitionList_destroy(asg);

    /* Second rebalance down to {test-1}: test-0 is revoked, nothing is added —
     * but `onPartitionsAssigned` still fires, with an empty list. */
    const char *topics1[1] = {"test"};
    int32_t partitions1[1] = {1};
    TEST_ASSERT_NULL(rebalance_to(c, topics1, partitions1, 1));

    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.revoked_calls));
    TEST_ASSERT_EQUAL_INT32(1, result.revoked.count);
    TEST_ASSERT_EQUAL_STRING("test", result.revoked.topics[0]);
    TEST_ASSERT_EQUAL_INT32(0, result.revoked.partitions[0]);

    TEST_ASSERT_EQUAL_INT(2, atomic_load(&result.assigned_calls));
    TEST_ASSERT_EQUAL_INT32(0, result.assigned.count);

    asg = kafka_consumer_Consumer_assignment(c);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_TopicPartitionList_count(asg));
    kafka_consumer_TopicPartitionList_destroy(asg);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// A rebalance on a manually-assigned consumer is rejected, as in Java
// (`IllegalArgumentException` from `assignFromSubscribed`), and the mock driver
// is mock-only.
// ---------------------------------------------------------------------------

static void test_rebalance_requires_a_subscription_and_a_mock(void) {
    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};

    /* Manual assignment: no dynamic assignment allowed. */
    kafka_consumer_Consumer_t *manual = make_assigned_mock("test", 0);
    kafka_common_KafkaError_t *err = rebalance_to(manual, topics, partitions, 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(kafka_common_KafkaError_message(err), "manual assignment in use"),
                                 kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);
    kafka_consumer_Consumer_destroy(manual);

    /* A real consumer has no rebalance driver. */
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        "group.id",          "rebalance-test-group",
        "group.protocol",    "consumer",
        NULL
    };
    kafka_consumer_ConsumerProperties_t *props = kafka_consumer_ConsumerProperties_from_configs(configs);
    TEST_ASSERT_NOT_NULL(props);
    kafka_common_KafkaError_t *new_err = NULL;
    kafka_consumer_Consumer_t *real = kafka_consumer_KafkaConsumer_new(props, &new_err);
    kafka_consumer_ConsumerProperties_destroy(props);
    TEST_ASSERT_NULL(new_err);
    TEST_ASSERT_NOT_NULL(real);

    err = rebalance_to(real, topics, partitions, 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(kafka_common_KafkaError_message(err), "only supported on a MockConsumer"),
                                 kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);
    kafka_consumer_Consumer_destroy(real);
}

// ---------------------------------------------------------------------------
// Listener callbacks run on the dispatcher thread, never on the caller's
// ---------------------------------------------------------------------------

static void test_rebalance_listener_runs_on_dispatcher_thread(void) {
    listener_result_t result;
    listener_result_init(&result);
    kafka_consumer_Consumer_t *c = make_subscribed_mock("test", &result);

    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    TEST_ASSERT_NULL(rebalance_to(c, topics, partitions, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.assigned_calls));
    TEST_ASSERT_NOT_EQUAL(pthread_self(), result.assigned_thread);

    int32_t none[1] = {0};
    TEST_ASSERT_NULL(rebalance_to(c, topics, none, 0));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.revoked_calls));
    TEST_ASSERT_NOT_EQUAL(pthread_self(), result.revoked_thread);
    /* Both callbacks of one consumer share the single dispatcher thread. */
    TEST_ASSERT_EQUAL(result.assigned_thread, result.revoked_thread);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// The rebalance does not complete until the listener returns
// (`consumer-threading.md` §31, regression test #2)
//
// The listener parks on a condvar. The main thread observes that the
// `MockConsumer_rebalance` call (driven from a helper thread) has NOT returned
// while the listener is parked, releases it, and then checks the ordering: the
// listener returned strictly before the rebalance call did.
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_Consumer_t *consumer;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int release;             /* guarded by `mutex` */
    atomic_int in_callback;
    /* Monotonic ticket numbers proving the ordering. */
    atomic_int seq;
    int listener_return_seq;
    int rebalance_return_seq;
    atomic_int rebalance_returned;
    kafka_common_KafkaError_t *rebalance_error;
} parking_listener_t;

static kafka_common_KafkaError_t *on_assigned_parking(kafka_consumer_TopicPartitionList_t *partitions,
                                                     void *user_data) {
    parking_listener_t *p = (parking_listener_t *)user_data;
    kafka_consumer_TopicPartitionList_destroy(partitions);

    atomic_store(&p->in_callback, 1);
    pthread_mutex_lock(&p->mutex);
    while (!p->release) {
        pthread_cond_wait(&p->cond, &p->mutex);
    }
    pthread_mutex_unlock(&p->mutex);

    p->listener_return_seq = atomic_fetch_add(&p->seq, 1);
    return NULL;
}

static kafka_common_KafkaError_t *on_revoked_parking(kafka_consumer_TopicPartitionList_t *partitions,
                                                    void *user_data) {
    (void)user_data;
    kafka_consumer_TopicPartitionList_destroy(partitions);
    return NULL;
}

static void *rebalance_thread_main(void *arg) {
    parking_listener_t *p = (parking_listener_t *)arg;
    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    p->rebalance_error = kafka_consumer_MockConsumer_rebalance(p->consumer, topics, partitions, 1);
    p->rebalance_return_seq = atomic_fetch_add(&p->seq, 1);
    atomic_store(&p->rebalance_returned, 1);
    return NULL;
}

static void test_rebalance_blocks_until_listener_returns(void) {
    parking_listener_t probe;
    memset(&probe, 0, sizeof(probe));
    pthread_mutex_init(&probe.mutex, NULL);
    pthread_cond_init(&probe.cond, NULL);
    atomic_init(&probe.in_callback, 0);
    atomic_init(&probe.seq, 1);
    atomic_init(&probe.rebalance_returned, 0);

    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(c);
    probe.consumer = c;

    kafka_consumer_ConsumerRebalanceListener_t *listener = kafka_consumer_ConsumerRebalanceListener_new(
        on_revoked_parking, on_assigned_parking, NULL, &probe, NULL);
    const char *topics[1] = {"test"};
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_listener(c, topics, 1, listener));

    pthread_t thread;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, NULL, rebalance_thread_main, &probe));

    /* Wait until the listener is parked inside the callback. */
    TEST_ASSERT_TRUE(wait_for(&probe.in_callback, 1));

    /* The listener is still inside the callback, so the rebalance cannot have
     * returned. Give it a real window to (incorrectly) return. */
    struct timespec ts = {0, 100000000}; /* 100ms */
    nanosleep(&ts, NULL);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.rebalance_returned));

    /* Release the listener. */
    pthread_mutex_lock(&probe.mutex);
    probe.release = 1;
    pthread_cond_signal(&probe.cond);
    pthread_mutex_unlock(&probe.mutex);

    TEST_ASSERT_EQUAL_INT(0, pthread_join(thread, NULL));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.rebalance_returned));
    TEST_ASSERT_NULL(probe.rebalance_error);
    /* The listener returned strictly before the rebalance call did. */
    TEST_ASSERT_TRUE(probe.listener_return_seq < probe.rebalance_return_seq);

    kafka_consumer_Consumer_destroy(c);
    pthread_cond_destroy(&probe.cond);
    pthread_mutex_destroy(&probe.mutex);
}

// ---------------------------------------------------------------------------
// A listener may call back into the consumer through `ConsumerHandle_t` without
// deadlocking (`consumer-threading.md` §31, regression test #1, adapted)
//
// Java gets this for free because the callback runs on the polling thread. Here
// the callback runs on the dispatcher thread — a plain OS thread — while the
// consumer task that triggered the rebalance awaits it, so a nested blocking
// handle op is legal and completes. On a mock-derived handle the op itself is
// unsupported by the core, which is still the proof that matters: the call
// *returns* instead of hanging, and it is not rejected by the access guard.
//
// The same test also documents the guard: the plain `kafka_consumer_Consumer_*`
// API is rejected with ConcurrentModification from inside the callback.
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_Consumer_t *consumer;
    kafka_consumer_ConsumerHandle_t *handle;
    atomic_int fired;
    int handle_assignment_non_null;
    int handle_commit_error_code;
    char handle_commit_message[192];
    int owner_commit_error_code;
} listener_reentrancy_t;

static kafka_common_KafkaError_t *on_assigned_reentrant(kafka_consumer_TopicPartitionList_t *partitions,
                                                       void *user_data) {
    listener_reentrancy_t *p = (listener_reentrancy_t *)user_data;
    kafka_consumer_TopicPartitionList_destroy(partitions);

    /* The sanctioned reentrancy path: a blocking handle op from inside the
     * listener returns (it does not deadlock and is not guard-rejected). */
    kafka_common_KafkaError_t *handle_err = kafka_consumer_ConsumerHandle_commit_sync(p->handle);
    if (handle_err != NULL) {
        p->handle_commit_error_code = kafka_common_KafkaError_code(handle_err);
        snprintf(p->handle_commit_message, sizeof(p->handle_commit_message), "%s",
                 kafka_common_KafkaError_message(handle_err));
        kafka_common_KafkaError_destroy(handle_err);
    }

    kafka_consumer_TopicPartitionList_t *asg = kafka_consumer_ConsumerHandle_assignment(p->handle);
    p->handle_assignment_non_null = asg != NULL;
    kafka_consumer_TopicPartitionList_destroy(asg);

    /* The plain API is not: the app thread driving the rebalance holds the
     * access guard. */
    kafka_common_KafkaError_t *owner_err = kafka_consumer_Consumer_commit_sync(p->consumer);
    p->owner_commit_error_code = owner_err != NULL ? kafka_common_KafkaError_code(owner_err) : 0;
    kafka_common_KafkaError_destroy(owner_err);

    atomic_fetch_add(&p->fired, 1);
    return NULL;
}

static kafka_common_KafkaError_t *on_revoked_noop(kafka_consumer_TopicPartitionList_t *partitions, void *user_data) {
    (void)user_data;
    kafka_consumer_TopicPartitionList_destroy(partitions);
    return NULL;
}

static void test_listener_calls_consumer_handle_no_deadlock(void) {
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(c);
    kafka_consumer_ConsumerHandle_t *h = kafka_consumer_Consumer_handle(c);
    TEST_ASSERT_NOT_NULL(h);

    listener_reentrancy_t probe;
    memset(&probe, 0, sizeof(probe));
    atomic_init(&probe.fired, 0);
    probe.consumer = c;
    probe.handle = h;
    probe.handle_commit_error_code = INT32_MAX; /* sentinel: callback ran */

    kafka_consumer_ConsumerRebalanceListener_t *listener =
        kafka_consumer_ConsumerRebalanceListener_new(on_revoked_noop, on_assigned_reentrant, NULL, &probe, NULL);
    const char *topics[1] = {"test"};
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_listener(c, topics, 1, listener));

    /* If the nested handle op deadlocked, this call would never return. */
    int32_t partitions[1] = {0};
    TEST_ASSERT_NULL(rebalance_to(c, topics, partitions, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.fired));

    /* The handle op reached the core: the mock's unsupported error, never
     * ConcurrentModification. */
    TEST_ASSERT_NOT_EQUAL(INT32_MAX, probe.handle_commit_error_code);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(probe.handle_commit_message, MOCK_HANDLE_UNSUPPORTED),
                                 probe.handle_commit_message);
    TEST_ASSERT_TRUE(probe.handle_assignment_non_null);

    /* Control: the plain API is guard-rejected from inside the callback. */
    TEST_ASSERT_EQUAL_INT(CONCURRENT_MODIFICATION_CODE, probe.owner_commit_error_code);

    kafka_consumer_ConsumerHandle_destroy(h);
    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// An error handle returned by a listener propagates out of the rebalance
// (the C equivalent of the Java listener throwing)
// ---------------------------------------------------------------------------

static void test_listener_error_propagates(void) {
    listener_result_t result;
    listener_result_init(&result);
    result.assigned_fails = 1;
    kafka_consumer_Consumer_t *c = make_subscribed_mock("test", &result);

    const char *topics[1] = {"test"};
    int32_t partitions[1] = {0};
    kafka_common_KafkaError_t *err = rebalance_to(c, topics, partitions, 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.assigned_calls));
    /* The core propagates the listener's error with `?`, so the message is the
     * one the C callback supplied, verbatim. */
    TEST_ASSERT_EQUAL_STRING(LISTENER_ERROR_MESSAGE, kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    kafka_consumer_Consumer_destroy(c);
}

// ---------------------------------------------------------------------------
// user_data_destroy fires exactly once when the registration is released
//
// A registered listener is released when a subsequent `subscribe` replaces it
// (matching Java: `SubscriptionState.unsubscribe()` clears the subscription but
// keeps the listener), or when the consumer is destroyed.
// ---------------------------------------------------------------------------

/* Listener callbacks paired with `destroy_counter_t` user_data — they must NOT
 * reinterpret it as a `listener_result_t`. */
static kafka_common_KafkaError_t *on_revoked_counting(kafka_consumer_TopicPartitionList_t *partitions,
                                                     void *user_data) {
    destroy_counter_t *counter = (destroy_counter_t *)user_data;
    kafka_consumer_TopicPartitionList_destroy(partitions);
    atomic_fetch_add(&counter->callback_calls, 1);
    return NULL;
}

static kafka_common_KafkaError_t *on_assigned_counting(kafka_consumer_TopicPartitionList_t *partitions,
                                                      void *user_data) {
    destroy_counter_t *counter = (destroy_counter_t *)user_data;
    kafka_consumer_TopicPartitionList_destroy(partitions);
    atomic_fetch_add(&counter->callback_calls, 1);
    return NULL;
}

static destroy_counter_t *new_destroy_counter(void) {
    destroy_counter_t *counter = (destroy_counter_t *)malloc(sizeof(destroy_counter_t));
    TEST_ASSERT_NOT_NULL(counter);
    atomic_init(&counter->destroy_calls, 0);
    atomic_init(&counter->callback_calls, 0);
    return counter;
}

static void test_listener_user_data_destroy_fires_exactly_once(void) {
    /* (1) Never subscribed: `_destroy` releases it. */
    destroy_counter_t *never = new_destroy_counter();
    kafka_consumer_ConsumerRebalanceListener_t *unused = kafka_consumer_ConsumerRebalanceListener_new(
        on_revoked_counting, on_assigned_counting, NULL, never, on_user_data_destroy);
    TEST_ASSERT_NOT_NULL(unused);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&never->destroy_calls));
    kafka_consumer_ConsumerRebalanceListener_destroy(unused);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&never->destroy_calls));
    free(never);

    /* (2) Registered, used, then replaced by a plain `subscribe`. */
    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(c);
    destroy_counter_t *counter = new_destroy_counter();
    kafka_consumer_ConsumerRebalanceListener_t *listener = kafka_consumer_ConsumerRebalanceListener_new(
        on_revoked_counting, on_assigned_counting, NULL, counter, on_user_data_destroy);
    const char *topics[1] = {"test"};
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_listener(c, topics, 1, listener));

    int32_t partitions[1] = {0};
    TEST_ASSERT_NULL(rebalance_to(c, topics, partitions, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&counter->callback_calls));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&counter->destroy_calls));

    /* Replacing the registration releases the old listener. The Arc may be
     * dropped on a consumer task, so allow for a brief window. */
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe(c, topics, 1));
    TEST_ASSERT_TRUE(wait_for(&counter->destroy_calls, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&counter->destroy_calls));

    kafka_consumer_Consumer_destroy(c);
    /* Still exactly once after the consumer is gone. */
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&counter->destroy_calls));
    free(counter);

    /* (3) Registered and released by destroying the consumer. */
    kafka_consumer_Consumer_t *c2 = kafka_consumer_MockConsumer_new("earliest");
    destroy_counter_t *counter2 = new_destroy_counter();
    kafka_consumer_ConsumerRebalanceListener_t *listener2 = kafka_consumer_ConsumerRebalanceListener_new(
        on_revoked_counting, on_assigned_counting, NULL, counter2, on_user_data_destroy);
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_listener(c2, topics, 1, listener2));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&counter2->destroy_calls));
    kafka_consumer_Consumer_destroy(c2);
    TEST_ASSERT_TRUE(wait_for(&counter2->destroy_calls, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&counter2->destroy_calls));
    free(counter2);
}

// ---------------------------------------------------------------------------
// A failing subscribe still consumes the listener
//
// `subscribe_with_listener` takes ownership unconditionally, so the destroy hook
// fires even when the call never registers anything. Driven deterministically by
// calling it from inside a commit callback, where the app thread still holds the
// access guard (see `test_consumer_handle_usable_while_op_in_flight`).
// ---------------------------------------------------------------------------

typedef struct {
    kafka_consumer_Consumer_t *consumer;
    destroy_counter_t *counter;
    atomic_int fired;
    int subscribe_error_code;
} subscribe_reject_t;

static void on_commit_subscribing(kafka_consumer_OffsetMap_t *offsets,
                                  kafka_common_KafkaError_t *error,
                                  void *user_data) {
    subscribe_reject_t *p = (subscribe_reject_t *)user_data;
    kafka_consumer_OffsetMap_destroy(offsets);
    kafka_common_KafkaError_destroy(error);

    kafka_consumer_ConsumerRebalanceListener_t *listener = kafka_consumer_ConsumerRebalanceListener_new(
        on_revoked_counting, on_assigned_counting, NULL, p->counter, on_user_data_destroy);
    const char *topics[1] = {"test"};
    kafka_common_KafkaError_t *err =
        kafka_consumer_Consumer_subscribe_with_listener(p->consumer, topics, 1, listener);
    p->subscribe_error_code = err != NULL ? kafka_common_KafkaError_code(err) : 0;
    kafka_common_KafkaError_destroy(err);

    atomic_fetch_add(&p->fired, 1);
}

static void test_failing_subscribe_with_listener_still_releases_the_listener(void) {
    kafka_consumer_Consumer_t *c = make_assigned_mock("test", 0);

    subscribe_reject_t probe;
    memset(&probe, 0, sizeof(probe));
    atomic_init(&probe.fired, 0);
    probe.consumer = c;
    probe.counter = new_destroy_counter();
    probe.subscribe_error_code = INT32_MAX; /* sentinel: callback ran */

    TEST_ASSERT_NULL(kafka_consumer_Consumer_commit_async_with_callback(c, on_commit_subscribing, &probe, NULL));
    TEST_ASSERT_TRUE(wait_for(&probe.fired, 1));

    /* The guard was held, so the subscribe was rejected... */
    TEST_ASSERT_EQUAL_INT(CONCURRENT_MODIFICATION_CODE, probe.subscribe_error_code);
    /* ...and the listener was released anyway, exactly once, with no callback. */
    TEST_ASSERT_TRUE(wait_for(&probe.counter->destroy_calls, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.counter->destroy_calls));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&probe.counter->callback_calls));

    kafka_consumer_Consumer_destroy(c);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&probe.counter->destroy_calls));
    free(probe.counter);
}

// ---------------------------------------------------------------------------
// The async subscribe variant registers the listener the same way
// ---------------------------------------------------------------------------

/* Completion of a void-returning async consumer op. */
typedef struct {
    atomic_int fired;
    int had_error;
    int32_t error_code;
} op_result_t;

static void on_subscribe_done(kafka_common_KafkaError_t *error, void *user_data) {
    op_result_t *r = (op_result_t *)user_data;
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_KafkaError_code(error);
        kafka_common_KafkaError_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_subscribe_with_listener_async(void) {
    listener_result_t result;
    listener_result_init(&result);

    kafka_consumer_Consumer_t *c = kafka_consumer_MockConsumer_new("earliest");
    TEST_ASSERT_NOT_NULL(c);

    kafka_consumer_ConsumerRebalanceListener_t *listener =
        kafka_consumer_ConsumerRebalanceListener_new(on_revoked, on_assigned, NULL, &result, NULL);
    op_result_t op;
    memset(&op, 0, sizeof(op));
    atomic_init(&op.fired, 0);

    const char *topics[1] = {"test"};
    kafka_consumer_Consumer_subscribe_with_listener_async(c, topics, 1, listener, on_subscribe_done, &op);
    TEST_ASSERT_TRUE(wait_for(&op.fired, 1));
    TEST_ASSERT_FALSE(op.had_error);

    int32_t partitions[1] = {0};
    TEST_ASSERT_NULL(rebalance_to(c, topics, partitions, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.assigned_calls));
    TEST_ASSERT_EQUAL_INT32(1, result.assigned.count);
    TEST_ASSERT_EQUAL_STRING("test", result.assigned.topics[0]);
    TEST_ASSERT_EQUAL_INT32(0, result.assigned.partitions[0]);

    kafka_consumer_Consumer_destroy(c);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_commit_async_with_callback_fires_with_offsets_null_error);
    RUN_TEST(test_commit_async_offsets_with_callback_echoes_offsets);
    RUN_TEST(test_commit_callback_runs_on_dispatcher_thread);
    RUN_TEST(test_commit_callback_user_data_destroy_fires_exactly_once);
    RUN_TEST(test_commit_returns_only_after_callback_returns);
    RUN_TEST(test_consumer_handle_new_destroy);
    RUN_TEST(test_consumer_handle_sync_getters_empty_on_mock);
    RUN_TEST(test_consumer_handle_async_ops_unsupported_on_mock);
    RUN_TEST(test_consumer_handle_wakeup);
    RUN_TEST(test_consumer_handle_usable_while_op_in_flight);
    RUN_TEST(test_consumer_handle_shares_state_with_real_consumer);
    RUN_TEST(test_rebalance_listener_assigned_then_revoked);
    RUN_TEST(test_rebalance_requires_a_subscription_and_a_mock);
    RUN_TEST(test_rebalance_listener_runs_on_dispatcher_thread);
    RUN_TEST(test_rebalance_blocks_until_listener_returns);
    RUN_TEST(test_listener_calls_consumer_handle_no_deadlock);
    RUN_TEST(test_listener_error_propagates);
    RUN_TEST(test_listener_user_data_destroy_fires_exactly_once);
    RUN_TEST(test_failing_subscribe_with_listener_still_releases_the_listener);
    RUN_TEST(test_subscribe_with_listener_async);
    return UNITY_END();
}
