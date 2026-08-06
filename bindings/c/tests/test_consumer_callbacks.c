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
// `..._commit_async_offsets_with_callback`).
//
// All tests are MockConsumer-backed. The mock's core `commit_async_impl` awaits
// `on_complete` inline (mirroring Java's `MockConsumer.commitAsync`, which calls
// `callback.onComplete` synchronously), so a commit through these entry points
// exercises the full adapter round trip: app thread -> dispatcher thread (C
// callback) -> back to the app thread.

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

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_commit_async_with_callback_fires_with_offsets_null_error);
    RUN_TEST(test_commit_async_offsets_with_callback_echoes_offsets);
    RUN_TEST(test_commit_callback_runs_on_dispatcher_thread);
    RUN_TEST(test_commit_callback_user_data_destroy_fires_exactly_once);
    RUN_TEST(test_commit_returns_only_after_callback_returns);
    return UNITY_END();
}
