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
#include <stdatomic.h>
#include <time.h>
#include <pthread.h>
#include "unity.h"
#include "test_support.h"

void setUp(void) {}
void tearDown(void) {}

/* Helper: create a KafkaProducer with a single bootstrap.servers config. */
static kafka_producer_Producer_t *create_producer(const char *bootstrap,
                                                   kafka_common_Error_t **out_err) {
    const char *configs[] = {
        "bootstrap.servers", bootstrap,
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, out_err);
    kafka_producer_ProducerProperties_destroy(props);
    return producer;
}

// ---------------------------------------------------------------------------
// ProducerProperties tests
// ---------------------------------------------------------------------------

void test_properties_new_and_put(void) {
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_new();
    TEST_ASSERT_NOT_NULL(props);

    kafka_producer_ProducerProperties_put(props, "bootstrap.servers", "localhost:9092");

    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

void test_properties_from_configs(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        "client.id",         "test-client",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    TEST_ASSERT_NOT_NULL(props);

    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

void test_properties_from_configs_null(void) {
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(NULL);
    TEST_ASSERT_NULL(props);
}

void test_properties_from_configs_odd(void) {
    /* Missing value for second key — odd number of entries. */
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        "client.id",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    TEST_ASSERT_NULL(props);
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

void test_create_close_destroy(void) {
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer = create_producer("localhost:9092", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    kafka_producer_Producer_destroy(producer);
}

void test_create_destroy_without_close(void) {
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer = create_producer("localhost:9092", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    /* Destroy without close -- Drop impl handles cleanup */
    kafka_producer_Producer_destroy(producer);
}

void test_create_null_props(void) {
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(NULL, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(producer);
    kafka_common_Error_destroy(err);
}

void test_create_null_out_error(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    /* NULL out_error: caller doesn't want error details, should still work */
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, NULL);
    TEST_ASSERT_NOT_NULL(producer);
    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, NULL);
    kafka_producer_Producer_destroy(producer);
}

void test_create_invalid_config_value(void) {
    const char *configs[] = {
        "batch.size", "not-a-number",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(producer);
    kafka_common_Error_destroy(err);
    kafka_producer_ProducerProperties_destroy(props);
}

// ---------------------------------------------------------------------------
// Mock-specific operations should be no-ops for KafkaProducer
// ---------------------------------------------------------------------------

void test_mock_ops_noop(void) {
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer = create_producer("localhost:9092", &err);
    TEST_ASSERT_NULL(err);

    TEST_ASSERT_FALSE(kafka_producer_MockProducer_complete_next(producer));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_error_next(producer, 2, NULL));
    TEST_ASSERT_EQUAL_INT32(0, kafka_producer_MockProducer_history_count(producer));
    kafka_producer_MockProducer_clear(producer); /* no-op */

    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Multiple config entries
// ---------------------------------------------------------------------------

void test_create_multiple_config(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092,localhost:9093",
        "client.id",         "my-producer",
        "batch.size",        "32768",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Transactions
//
// The transaction lifecycle itself is covered broker-free in
// test_mock_producer.c. What needs the real KafkaProducer is the
// transaction-control concurrency guard, because it needs one control call to be
// slow enough for a second one to overlap it — and `init_transactions` against an
// unreachable broker blocks for exactly `max.block.ms`, which no mock call does.
//
// Java's KafkaProducer.send() is safe to call concurrently but its five
// transaction-control methods are not safe to call concurrently with each other.
// The FFI enforces the latter with a flag scoped to those five functions only.
// See design/history/Milestone-11/producer-transactions-ffi-plan.md.
// ---------------------------------------------------------------------------

/* Signature shared by the four no-argument transaction-control functions. */
typedef kafka_common_Error_t *(*txn_control_fn)(kafka_producer_Producer_t *);

/* Set once the helper thread has completed an init_transactions call that the
 * guard did *not* reject, i.e. once the overlap window has closed. */
static atomic_int txn_init_returned = 0;
/* Set if the helper never managed to win the guard (see txn_init_thread). */
static atomic_int txn_init_gave_up = 0;

/* True if `err` is the transaction-control guard rejection. The guard reuses
 * KafkaError::concurrent_modification, which shares IllegalState's error code,
 * so the message is what distinguishes it from an ordinary state error. */
static int is_txn_guard_error(kafka_common_Error_t *err) {
    if (err == NULL) {
        return 0;
    }
    const char *msg = kafka_common_Error_message(err);
    return msg != NULL && strstr(msg, "not safe for concurrent access") != NULL;
}

/* Attempt ceiling for txn_init_thread's retry loop: with the 1ms sleep between
 * attempts, a run of pure rejections gives up after roughly a second rather than
 * spinning forever if this thread somehow never wins the guard. */
#define TXN_INIT_MAX_ATTEMPTS 1000

/* Holds the transaction-control guard for ~max.block.ms so the main thread has a
 * window to be rejected in.
 *
 * init_transactions blocks for max.block.ms because the broker is unreachable, so
 * InitProducerId never completes; the timeout error it ends with is expected and
 * irrelevant. The retry loop is what makes the test deterministic: this thread
 * competes for the same guard as the main thread, so its own call can be the one
 * that gets rejected — and a rejected call returns instantly, opening no window
 * at all. A rejection never touches the producer, so retrying is a fresh attempt
 * that still blocks for the full max.block.ms once it does win. */
static void *txn_init_thread(void *arg) {
    kafka_producer_Producer_t *producer = (kafka_producer_Producer_t *)arg;
    for (int attempt = 0; attempt < TXN_INIT_MAX_ATTEMPTS; attempt++) {
        kafka_common_Error_t *err = kafka_producer_Producer_init_transactions(producer);
        /* Classify with a local strstr, then publish the closed window *before*
         * freeing `err`. The Rust guard is released the instant the call above
         * returns, so anything done ahead of the store leaves the guard free
         * while the flag still reads 0 — and a main-thread poll landing in that
         * window would see an ordinary state error rather than the guard
         * rejection, and report a spurious failure. The store cannot be made
         * simultaneous with the guard release (that happens inside Rust), so the
         * point is to leave nothing but a few instructions in between. */
        int rejected = is_txn_guard_error(err);
        if (!rejected) {
            atomic_store(&txn_init_returned, 1);
        }
        if (err != NULL) {
            kafka_common_Error_destroy(err);
        }
        if (!rejected) {
            return NULL;
        }
        struct timespec ts = {0, 1000000}; /* 1ms, matching the main-thread loop */
        nanosleep(&ts, NULL);
    }
    atomic_store(&txn_init_gave_up, 1);
    atomic_store(&txn_init_returned, 1);
    return NULL;
}

/* Delivery callback for the unguarded-send assertion. It owns whichever handle
 * it is given; the outcome is irrelevant (there is no broker), so it only frees
 * them. */
static atomic_int txn_guard_records_fired = 0;

static void txn_guard_on_record(kafka_producer_RecordMetadata_t *metadata,
                                kafka_common_Error_t *error,
                                void *user_data) {
    (void)user_data;
    atomic_fetch_add(&txn_guard_records_fired, 1);
    if (metadata != NULL) {
        kafka_producer_RecordMetadata_destroy(metadata);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
}

/* Calls `fn` until the guard rejects it, giving up once the helper thread's
 * init_transactions has returned (the window is then closed). Returns 1 if the
 * guard fired. */
static int rejected_while_init_runs(kafka_producer_Producer_t *producer,
                                    txn_control_fn fn) {
    while (!atomic_load(&txn_init_returned)) {
        kafka_common_Error_t *err = fn(producer);
        int rejected = is_txn_guard_error(err);
        if (err != NULL) {
            kafka_common_Error_destroy(err);
        }
        if (rejected) {
            return 1;
        }
        struct timespec ts = {0, 1000000}; /* 1ms */
        nanosleep(&ts, NULL);
    }
    return 0;
}

/* Async control-call probe: records whether the delivered error was the guard
 * rejection. The async entry points CAS-fail *synchronously* (firing the callback
 * inline before returning), so a rejection is observed on the calling thread just
 * like the sync probe; a non-rejected call won the flag and fires its callback
 * later from the dispatcher thread. */
typedef struct {
    atomic_int fired;
    int was_guard_error;
} txn_async_probe_t;

static void txn_async_on_operation(kafka_common_Error_t *error, void *user_data) {
    txn_async_probe_t *p = (txn_async_probe_t *)user_data;
    p->was_guard_error = is_txn_guard_error(error);
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&p->fired, 1);
}

/* Async analog of rejected_while_init_runs, proving the async entry points share the
 * same guard as the sync ones: calls commit_transaction_async until its callback
 * reports the guard rejection, giving up once the helper's init has returned. A
 * rejected async call fires its callback synchronously (the CAS fails before the
 * spawn); a non-rejected one won the flag and ran a commit against the
 * never-successfully-initialized producer, which fails fast (a state error, no
 * network wait) and releases the flag, so retrying is safe and does not stall the
 * window. Returns 1 if the guard fired. */
static int async_commit_rejected_while_init_runs(kafka_producer_Producer_t *producer) {
    while (!atomic_load(&txn_init_returned)) {
        txn_async_probe_t probe;
        atomic_init(&probe.fired, 0);
        probe.was_guard_error = 0;
        kafka_producer_Producer_commit_transaction_async(producer, txn_async_on_operation, &probe);
        /* Wait for the callback: synchronous on the rejection path, prompt (fast
         * state error) on the win path, so this returns quickly either way and keeps
         * `probe` alive until the callback has run. */
        wait_for(&probe.fired, 1);
        if (probe.was_guard_error) {
            return 1;
        }
        struct timespec ts = {0, 1000000}; /* 1ms */
        nanosleep(&ts, NULL);
    }
    return 0;
}

void test_transaction_control_guard_rejects_concurrent_calls(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:1",   /* unreachable on purpose */
        "transactional.id",  "c-ffi-txn-guard",
        /* The overlap window only has to outlast four sub-millisecond probe
         * calls, and init_transactions blocks for exactly this long, so keep it
         * short — this is dead wait time on every CI build. */
        "max.block.ms",      "1000",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    kafka_producer_ProducerProperties_destroy(props);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    atomic_store(&txn_init_returned, 0);
    atomic_store(&txn_init_gave_up, 0);
    atomic_store(&txn_guard_records_fired, 0);
    pthread_t init_task;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&init_task, NULL, txn_init_thread, producer));

    /* Everything below records into locals and asserts only after the helper
     * thread is joined and the producer destroyed. Unity's TEST_ASSERT_* longjmp
     * out of the test on failure, so asserting while the helper is still running
     * would leave a live thread making FFI calls into a producer that never gets
     * destroyed, for the remainder of the binary. Same shape as
     * test_kafka_consumer.c's wakeup-thread test. */

    /* commit and abort issued from this thread while init_transactions holds the
     * guard must fail fast, and with the guard error rather than a state error —
     * i.e. rejected before the producer was touched at all. */
    int commit_rejected = rejected_while_init_runs(
        producer, kafka_producer_Producer_commit_transaction);
    int abort_rejected = rejected_while_init_runs(
        producer, kafka_producer_Producer_abort_transaction);
    int begin_rejected = rejected_while_init_runs(
        producer, kafka_producer_Producer_begin_transaction);
    /* The async commit variant shares the same guard: a call issued while init holds
     * it must have its callback report the guard error too. */
    int async_commit_rejected = async_commit_rejected_while_init_runs(producer);
    int gave_up = atomic_load(&txn_init_gave_up);

    /* send is deliberately outside the guard: the non-blocking send path must
     * return immediately even while a control call holds it, and must not be
     * rejected. (The delivery callback resolves later, after the metadata
     * timeout; only the submit call is measured here.) */
    /* static: send_async borrows the buffer until the callback fires, which is
     * after this frame would be gone. */
    static const uint8_t value[] = "v";
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    err = NULL;
    kafka_producer_Producer_send_async(producer, "guard-topic", -1, -1, NULL, -1,
                                      value, (int32_t)sizeof(value) - 1,
                                      txn_guard_on_record, NULL, &err);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    int send_accepted = (err == NULL);
    if (err != NULL) {
        kafka_common_Error_destroy(err);
    }
    double send_ms = (double)(t1.tv_sec - t0.tv_sec) * 1000.0
                   + (double)(t1.tv_nsec - t0.tv_nsec) / 1000000.0;

    int join_rc = pthread_join(init_task, NULL);

    /* The guard is released on return, so a control call is accepted again. It
     * still fails — init timed out — but not with the guard error. Transaction
     * control now drains the submission channel before it runs (async sends
     * inside a transaction are supported and drained into the operation), so this
     * commit hands the send_async above to the producer before failing on the
     * timed-out init. */
    err = kafka_producer_Producer_commit_transaction(producer);
    int after_join_was_guard_error = is_txn_guard_error(err);
    if (err != NULL) {
        kafka_common_Error_destroy(err);
    }
    /* Wait, don't sample: the FFI only *enqueues* the completion, so on a loaded
     * box a bare read races the dispatcher thread and reports a phantom failure.
     * The wait deterministically observes the send_async callback for the count
     * assertion below; destroy() also joins the submission task, so nothing is
     * left in flight at teardown. */
    int send_callback_fired = wait_for(&txn_guard_records_fired, 1);

    kafka_producer_Producer_destroy(producer);

    TEST_ASSERT_EQUAL_INT(0, join_rc);
    /* Assert this first: if the helper never won the guard there was no overlap
     * window at all, and the three probe assertions below would misreport that as
     * "the guard failed to reject". */
    TEST_ASSERT_FALSE(gave_up);
    TEST_ASSERT_TRUE(commit_rejected);
    TEST_ASSERT_TRUE(abort_rejected);
    TEST_ASSERT_TRUE(begin_rejected);
    TEST_ASSERT_TRUE(async_commit_rejected);
    TEST_ASSERT_TRUE(send_accepted);
    /* 50ms, not the configured max.block.ms: a threshold equal to max.block.ms
     * bounds the very delay this is meant to detect, so it could never fail. The
     * measured value is ~0.01ms. */
    TEST_ASSERT_TRUE(send_ms < 50.0);
    TEST_ASSERT_FALSE(after_join_was_guard_error);
    TEST_ASSERT_TRUE(send_callback_fired);
}

void test_transaction_methods_on_non_transactional_producer(void) {
    /* No transactional.id: all five control methods must fail, none may hang.
     * max.block.ms is configured down because init_transactions blocks for it and
     * the 60s default would dominate the suite. */
    const char *configs[] = {
        "bootstrap.servers", "localhost:1",
        "max.block.ms",      "300",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_common_Error_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    kafka_producer_ProducerProperties_destroy(props);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    /* Every failure below must be the non-transactional-producer error, not the
     * guard rejection — they share error code -1, so only the message separates
     * them, and a leaked transaction-control flag would otherwise pass unnoticed. */
    err = kafka_producer_Producer_init_transactions(producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "non-transactional"));
    kafka_common_Error_destroy(err);

    err = kafka_producer_Producer_begin_transaction(producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_transaction_abortable_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "non-transactional"));
    kafka_common_Error_destroy(err);

    /* The real-producer arm of send_offsets_to_transaction: marshaling, the
     * group-metadata clone and the blocking call all run here. Nothing else in the
     * suite reaches it — the mock tests take the other arm. */
    kafka_consumer_Consumer_t *consumer = kafka_consumer_MockConsumer_new("earliest");
    kafka_consumer_ConsumerGroupMetadata_t *group_metadata =
        kafka_consumer_Consumer_group_metadata(consumer);
    TEST_ASSERT_NOT_NULL(group_metadata);
    const char *topics[] = { "input-topic" };
    const int32_t partitions[] = { 0 };
    const int64_t offsets[] = { 99 };
    const int32_t leader_epochs[] = { 3 };
    const char *metadata[] = { "meta" };
    err = kafka_producer_Producer_send_offsets_to_transaction(
        producer, topics, partitions, offsets, leader_epochs, metadata, 1,
        group_metadata);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "non-transactional"));
    kafka_common_Error_destroy(err);

    /* A null group_metadata is rejected before the guard is even taken, with a
     * message naming the parameter rather than the generic invalid-request text. */
    err = kafka_producer_Producer_send_offsets_to_transaction(
        producer, topics, partitions, offsets, leader_epochs, metadata, 1, NULL);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "group_metadata"));
    kafka_common_Error_destroy(err);

    err = kafka_producer_Producer_commit_transaction(producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "non-transactional"));
    kafka_common_Error_destroy(err);

    err = kafka_producer_Producer_abort_transaction(producer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "non-transactional"));
    kafka_common_Error_destroy(err);

    /* Mock-only driver hooks are no-ops on a real producer. */
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_set_commit_transaction_error(
        producer, false, 120, NULL));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_sent_offsets(producer));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_committed_offset(
        producer, "g", "t", 0, NULL, NULL, NULL, 0));

    kafka_consumer_ConsumerGroupMetadata_destroy(group_metadata);
    kafka_consumer_Consumer_destroy(consumer);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(void) {
    UNITY_BEGIN();

    /* ProducerProperties */
    RUN_TEST(test_properties_new_and_put);
    RUN_TEST(test_properties_from_configs);
    RUN_TEST(test_properties_from_configs_null);
    RUN_TEST(test_properties_from_configs_odd);

    /* Lifecycle */
    RUN_TEST(test_create_close_destroy);
    RUN_TEST(test_create_destroy_without_close);
    RUN_TEST(test_create_null_props);
    RUN_TEST(test_create_null_out_error);
    RUN_TEST(test_create_invalid_config_value);

    /* Mock ops */
    RUN_TEST(test_mock_ops_noop);

    /* Multiple config */
    RUN_TEST(test_create_multiple_config);

    /* Transactions */
    RUN_TEST(test_transaction_methods_on_non_transactional_producer);
    RUN_TEST(test_transaction_control_guard_rejects_concurrent_calls);

    return UNITY_END();
}
