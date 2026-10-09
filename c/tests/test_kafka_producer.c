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

// ---------------------------------------------------------------------------
// Fixtures
//
// `ProducerConfig` is built from a `kafka_Map_t` of C strings (the Java
// `Properties`); a map built in C borrows its keys and values, so the string
// literals below outlive it trivially. The configuration is validated at
// `kafka_producer_ProducerConfig_new`, so `kafka_producer_KafkaProducer_new`
// only ever sees a valid one, and the `Producer` interface is reached through
// the borrowed `__as_Producer` view, valid until the class handle is
// destroyed (CLAUDE.md §4, "Traits").
// ---------------------------------------------------------------------------

/* A map from NULL-terminated `key, value, key, value, ...` pairs. */
static kafka_Map_t *config_map(const char *const *pairs) {
    kafka_Map_t *map = kafka_Map_new();
    for (int i = 0; pairs[i] != NULL; i += 2) {
        kafka_Map_put(map, (void *)pairs[i], (void *)pairs[i + 1]);
    }
    return map;
}

/* `new ProducerConfig(props)` from pairs, asserting it validates. */
static kafka_producer_ProducerConfig_t *make_config(const char *const *pairs) {
    kafka_Map_t *props = config_map(pairs);
    kafka_producer_ProducerConfig_t *config = NULL;
    kafka_common_Error_t *err = kafka_producer_ProducerConfig_new(props, &config);
    kafka_Map_destroy(props);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(config);
    return config;
}

/* A byte-passthrough producer (NULL serializers) from `pairs`. */
static kafka_producer_KafkaProducer_t *make_producer(const char *const *pairs) {
    kafka_producer_ProducerConfig_t *config = make_config(pairs);
    kafka_producer_KafkaProducer_t *producer = NULL;
    kafka_common_Error_t *err = kafka_producer_KafkaProducer_new(config, NULL, NULL, &producer);
    kafka_producer_ProducerConfig_destroy(config);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);
    return producer;
}

/* `close()` through the interface view, then the class destroy. */
static void close_and_destroy(kafka_producer_KafkaProducer_t *producer) {
    const kafka_producer_Producer_t *view = kafka_producer_KafkaProducer__as_Producer(producer);
    kafka_common_Error_t *err = kafka_producer_Producer_close(view);
    TEST_ASSERT_NULL(err);
    kafka_producer_KafkaProducer_destroy(producer);
}

static const char *const BOOTSTRAP_ONLY[] = { "bootstrap.servers", "localhost:9092", NULL };

// ---------------------------------------------------------------------------
// ProducerConfig tests
// ---------------------------------------------------------------------------

void test_config_new_and_producer(void) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"localhost:9092");
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(props));

    kafka_producer_ProducerConfig_t *config = NULL;
    TEST_ASSERT_NULL(kafka_producer_ProducerConfig_new(props, &config));
    TEST_ASSERT_NOT_NULL(config);
    /* The map stays the caller's and is no longer needed by the config. */
    kafka_Map_destroy(props);

    kafka_producer_KafkaProducer_t *producer = NULL;
    TEST_ASSERT_NULL(kafka_producer_KafkaProducer_new(config, NULL, NULL, &producer));
    TEST_ASSERT_NOT_NULL(producer);
    kafka_producer_ProducerConfig_destroy(config);

    close_and_destroy(producer);
}

void test_config_builds_several_producers(void) {
    /* Like Java's `Properties`, one configuration can build several
     * producers: the handle keeps the validated properties. */
    const char *const pairs[] = {
        "bootstrap.servers", "localhost:9092",
        "client.id",         "test-client",
        NULL
    };
    kafka_producer_ProducerConfig_t *config = make_config(pairs);

    kafka_producer_KafkaProducer_t *first = NULL;
    kafka_producer_KafkaProducer_t *second = NULL;
    TEST_ASSERT_NULL(kafka_producer_KafkaProducer_new(config, NULL, NULL, &first));
    TEST_ASSERT_NULL(kafka_producer_KafkaProducer_new(config, NULL, NULL, &second));
    TEST_ASSERT_NOT_NULL(first);
    TEST_ASSERT_NOT_NULL(second);
    TEST_ASSERT_TRUE(first != second);
    kafka_producer_ProducerConfig_destroy(config);

    close_and_destroy(first);
    close_and_destroy(second);
}

void test_config_invalid_value(void) {
    /* Validation happens when the configuration is built, so an invalid
     * value never reaches `KafkaProducer_new`. The message is Java's
     * `ConfigException` text and names the property. */
    const char *const pairs[] = {
        "bootstrap.servers", "localhost:9092",
        "batch.size",        "not-a-number",
        NULL
    };
    kafka_Map_t *props = config_map(pairs);
    kafka_producer_ProducerConfig_t *config = NULL;
    kafka_common_Error_t *err = kafka_producer_ProducerConfig_new(props, &config);
    kafka_Map_destroy(props);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(config);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_CONFIG, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_config_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_error(err));
    const char *msg = kafka_common_Error_message(err);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_NOT_NULL(strstr(msg, "batch.size"));
    TEST_ASSERT_NOT_NULL(strstr(msg, "not-a-number"));
    kafka_common_Error_destroy(err);

    /* An invalid enumerated value as well. */
    const char *const acks[] = { "bootstrap.servers", "localhost:9092", "acks", "many", NULL };
    props = config_map(acks);
    err = kafka_producer_ProducerConfig_new(props, &config);
    kafka_Map_destroy(props);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(config);
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "acks"));
    kafka_common_Error_destroy(err);
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

void test_create_close_destroy(void) {
    kafka_producer_KafkaProducer_t *producer = make_producer(BOOTSTRAP_ONLY);
    close_and_destroy(producer);
}

void test_create_destroy_without_close(void) {
    kafka_producer_KafkaProducer_t *producer = make_producer(BOOTSTRAP_ONLY);
    /* Destroy without close: the Drop impl handles cleanup. */
    kafka_producer_KafkaProducer_destroy(producer);
}

void test_destroy_null(void) {
    kafka_producer_KafkaProducer_destroy(NULL);
    kafka_producer_ProducerConfig_destroy(NULL);
}

void test_create_with_string_serializers(void) {
    /* `new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())`:
     * the `__as_Serializer` views share the class handles, which the caller
     * keeps alive until the producer is destroyed (as with a partitioner
     * view). The producer's key and value are then C strings, not
     * `kafka_Bytes_t`. */
    kafka_producer_ProducerConfig_t *config = make_config(BOOTSTRAP_ONLY);
    kafka_common_serialization_StringSerializer_t *key_ser = kafka_common_serialization_StringSerializer_new();
    kafka_common_serialization_StringSerializer_t *value_ser = kafka_common_serialization_StringSerializer_new();
    TEST_ASSERT_NOT_NULL(key_ser);
    TEST_ASSERT_NOT_NULL(value_ser);

    kafka_producer_KafkaProducer_t *producer = NULL;
    kafka_common_Error_t *err = kafka_producer_KafkaProducer_new(
        config,
        (kafka_common_serialization_Serializer_t *)kafka_common_serialization_StringSerializer__as_Serializer(key_ser),
        (kafka_common_serialization_Serializer_t *)kafka_common_serialization_StringSerializer__as_Serializer(value_ser),
        &producer);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);
    kafka_producer_ProducerConfig_destroy(config);

    /* The shared implementation is still usable through the view while the
     * producer lives. */
    kafka_Bytes_t out = { NULL, -1 };
    err = kafka_common_serialization_Serializer_serialize(
        kafka_common_serialization_StringSerializer__as_Serializer(value_ser), "topic", "hello", &out);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(5, out.len);
    TEST_ASSERT_EQUAL_MEMORY("hello", out.data, 5);
    /* Borrowed from the handle until its next serialize call: nothing to free. */

    close_and_destroy(producer);
    kafka_common_serialization_StringSerializer_destroy(key_ser);
    kafka_common_serialization_StringSerializer_destroy(value_ser);
}

void test_create_multiple_config(void) {
    const char *const pairs[] = {
        "bootstrap.servers", "localhost:9092,localhost:9093",
        "client.id",         "my-producer",
        "batch.size",        "32768",
        NULL
    };
    kafka_producer_KafkaProducer_t *producer = make_producer(pairs);
    close_and_destroy(producer);
}

// ---------------------------------------------------------------------------
// Send against an unreachable broker: the real `send_cb` path
// ---------------------------------------------------------------------------

/* What a `send_cb` completion delivered. */
typedef struct {
    atomic_int fired;
    kafka_common_KafkaFuture_t *future;
    int had_error;
    pthread_t thread_id;
} send_result_t;

static void on_send(kafka_common_KafkaFuture_t *value, kafka_common_Error_t *error, void *opaque) {
    send_result_t *r = (send_result_t *)opaque;
    r->future = value;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    r->thread_id = pthread_self();
    atomic_fetch_add(&r->fired, 1);
}

void test_send_cb_unreachable_broker_fails_future_with_timeout(void) {
    /* Java's `send` catches an `ApiException` from the metadata wait, fires
     * the callback and returns a failed future (`FutureFailure`) rather than
     * throwing. So `send_cb` delivers a future, not an error, and the future
     * fails with the metadata `TimeoutException`. The completion runs on the
     * pumping thread, never on a Rust one. */
    const char *const pairs[] = {
        "bootstrap.servers", "localhost:1",   /* unreachable on purpose */
        "max.block.ms",      "300",
        NULL
    };
    kafka_producer_KafkaProducer_t *producer = make_producer(pairs);
    const kafka_producer_Producer_t *view = kafka_producer_KafkaProducer__as_Producer(producer);
    callback_pump_t pump;
    callback_pump_install(&pump, view);

    static const uint8_t value_bytes[] = "v";
    static const kafka_Bytes_t value = { value_bytes, 1 };
    kafka_producer_ProducerRecord_t *record = kafka_producer_ProducerRecord_new("unreachable-topic", &value);
    send_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_producer_Producer_send_cb(view, record, on_send, &result);
    kafka_producer_ProducerRecord_destroy(record);

    TEST_ASSERT_TRUE(callback_pump_until(&pump, &result.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), result.thread_id));
    TEST_ASSERT_FALSE(result.had_error);
    TEST_ASSERT_NOT_NULL(result.future);
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(result.future));

    void *metadata = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(result.future, &metadata);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(metadata);
    TEST_ASSERT_TRUE(kafka_common_Error_is_timeout_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(err));
    TEST_ASSERT_EQUAL_STRING("Topic unreachable-topic not present in metadata after 300 ms.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(result.future);

    close_and_destroy(producer);
    callback_pump_destroy(&pump);
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
typedef kafka_common_Error_t *(*txn_control_fn)(const kafka_producer_Producer_t *);

/* Set once the helper thread has completed an init_transactions call that the
 * guard did *not* reject, i.e. once the overlap window has closed. */
static atomic_int txn_init_returned = 0;
/* Set if the helper never managed to win the guard (see txn_init_thread). */
static atomic_int txn_init_gave_up = 0;

/* True if `err` is the transaction-control guard rejection: a
 * `LocalConcurrentModification`, a class of its own, so both the code and the
 * message identify it. */
static int is_txn_guard_error(const kafka_common_Error_t *err) {
    if (err == NULL) {
        return 0;
    }
    const char *msg = kafka_common_Error_message(err);
    return kafka_common_Error_code(err) == kafka_common_ErrorCode_LOCAL_CONCURRENT_MODIFICATION
        || (msg != NULL && strstr(msg, "not safe for concurrent access") != NULL);
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
    const kafka_producer_Producer_t *producer = (const kafka_producer_Producer_t *)arg;
    for (int attempt = 0; attempt < TXN_INIT_MAX_ATTEMPTS; attempt++) {
        kafka_common_Error_t *err = kafka_producer_Producer_init_transactions(producer);
        /* Classify with a local check, then publish the closed window *before*
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

/* Calls `fn` until the guard rejects it, giving up once the helper thread's
 * init_transactions has returned (the window is then closed). Returns 1 if the
 * guard fired. */
static int rejected_while_init_runs(const kafka_producer_Producer_t *producer, txn_control_fn fn) {
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

/* Queued control-call probe: records whether the delivered error was the guard
 * rejection. Both outcomes are delivered through the callback vector, so the
 * probe pumps it. */
typedef struct {
    atomic_int fired;
    int was_guard_error;
} txn_cb_probe_t;

static void txn_cb_on_operation(kafka_common_Error_t *error, void *opaque) {
    txn_cb_probe_t *p = (txn_cb_probe_t *)opaque;
    p->was_guard_error = is_txn_guard_error(error);
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&p->fired, 1);
}

/* Queued analog of rejected_while_init_runs, proving the `_cb` entry points
 * share the same guard as the blocking ones: calls commit_transaction_cb until
 * its completion reports the guard rejection, giving up once the helper's init
 * has returned. A rejected call queues its completion at once (the flag CAS
 * fails before anything is spawned); a non-rejected one won the flag and ran a
 * commit against the never-successfully-initialized producer, which fails fast
 * (a state error, no network wait) and releases the flag, so retrying is safe
 * and does not stall the window. Returns 1 if the guard fired. */
static int cb_commit_rejected_while_init_runs(const kafka_producer_Producer_t *producer,
                                              callback_pump_t *pump) {
    while (!atomic_load(&txn_init_returned)) {
        txn_cb_probe_t probe;
        atomic_init(&probe.fired, 0);
        probe.was_guard_error = 0;
        kafka_producer_Producer_commit_transaction_cb(producer, txn_cb_on_operation, &probe);
        /* Pump until the completion arrives: prompt either way, and `probe`
         * stays alive until the callback has run. */
        callback_pump_until(pump, &probe.fired, 1);
        if (probe.was_guard_error) {
            return 1;
        }
        struct timespec ts = {0, 1000000}; /* 1ms */
        nanosleep(&ts, NULL);
    }
    return 0;
}

/* The unguarded-send completion: the outcome is irrelevant (there is no
 * broker), it only frees what it is given and counts. */
static atomic_int txn_guard_sends_fired = 0;

static void txn_guard_on_send(kafka_common_KafkaFuture_t *value, kafka_common_Error_t *error, void *opaque) {
    (void)opaque;
    if (value != NULL) {
        kafka_common_KafkaFuture_destroy(value);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&txn_guard_sends_fired, 1);
}

void test_transaction_control_guard_rejects_concurrent_calls(void) {
    const char *const pairs[] = {
        "bootstrap.servers", "localhost:1",   /* unreachable on purpose */
        "transactional.id",  "c-ffi-txn-guard",
        /* The overlap window only has to outlast four sub-millisecond probe
         * calls, and init_transactions blocks for exactly this long, so keep it
         * short — this is dead wait time on every CI build. */
        "max.block.ms",      "1000",
        NULL
    };
    kafka_producer_KafkaProducer_t *producer = make_producer(pairs);
    const kafka_producer_Producer_t *view = kafka_producer_KafkaProducer__as_Producer(producer);
    callback_pump_t pump;
    callback_pump_install(&pump, view);

    atomic_store(&txn_init_returned, 0);
    atomic_store(&txn_init_gave_up, 0);
    atomic_store(&txn_guard_sends_fired, 0);
    pthread_t init_task;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&init_task, NULL, txn_init_thread, (void *)view));

    /* Everything below records into locals and asserts only after the helper
     * thread is joined and the producer destroyed. Unity's TEST_ASSERT_* longjmp
     * out of the test on failure, so asserting while the helper is still running
     * would leave a live thread making FFI calls into a producer that never gets
     * destroyed, for the remainder of the binary. Same shape as
     * test_kafka_consumer.c's wakeup-thread test. */

    /* commit and abort issued from this thread while init_transactions holds the
     * guard must fail fast, and with the guard error rather than a state error —
     * i.e. rejected before the producer was touched at all. */
    int commit_rejected = rejected_while_init_runs(view, kafka_producer_Producer_commit_transaction);
    int abort_rejected = rejected_while_init_runs(view, kafka_producer_Producer_abort_transaction);
    int begin_rejected = rejected_while_init_runs(view, kafka_producer_Producer_begin_transaction);
    /* The queued commit variant shares the same guard: a call issued while init
     * holds it must have its completion report the guard error too. */
    int cb_commit_rejected = cb_commit_rejected_while_init_runs(view, &pump);
    int gave_up = atomic_load(&txn_init_gave_up);

    /* send is deliberately outside the guard: the non-blocking send path must
     * return immediately even while a control call holds it, and must not be
     * rejected. (The completion resolves later, after the metadata timeout;
     * only the submit call is measured here.) The bytes are static because
     * the record is copied but the key/value `void *`s are the caller's until
     * the completion fires, after this frame would be gone. */
    static const uint8_t value_bytes[] = "v";
    static const kafka_Bytes_t value = { value_bytes, 1 };
    kafka_producer_ProducerRecord_t *record = kafka_producer_ProducerRecord_new("guard-topic", &value);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    kafka_producer_Producer_send_cb(view, record, txn_guard_on_send, NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    kafka_producer_ProducerRecord_destroy(record);
    double send_ms = (double)(t1.tv_sec - t0.tv_sec) * 1000.0
                   + (double)(t1.tv_nsec - t0.tv_nsec) / 1000000.0;

    int join_rc = pthread_join(init_task, NULL);

    /* The guard is released on return, so a control call is accepted again. It
     * still fails — init timed out — but not with the guard error. Transaction
     * control drains the submission channel before it runs (queued sends inside
     * a transaction are supported and drained into the operation), so this
     * commit hands the send_cb above to the producer before failing on the
     * timed-out init. */
    kafka_common_Error_t *err = kafka_producer_Producer_commit_transaction(view);
    int after_join_was_guard_error = is_txn_guard_error(err);
    if (err != NULL) {
        kafka_common_Error_destroy(err);
    }
    /* Pump, don't sample: the completion is only *queued*, so it is observed
     * by running the vector on this thread. destroy() also joins the submission
     * task and runs anything still pending, so nothing is left in flight at
     * teardown. */
    int send_completion_fired = callback_pump_until(&pump, &txn_guard_sends_fired, 1);

    /* Bounded close, not `close()`: after a timed-out init_transactions and a
     * send that failed on the metadata wait, the unbounded graceful close of
     * this transactional producer never returns on the current Rust side
     * (`Sender::run`'s "wait until these are completed" loop keeps spinning;
     * `close_with_timeout(0)` or any bound force-closes fine, and a producer
     * without the send closes at once). The bound keeps the suite from hanging
     * and is what Java's `close(Duration)` does once the timeout elapses. */
    kafka_common_Error_t *close_err = kafka_producer_Producer_close_with_timeout(view, 2000);
    kafka_producer_KafkaProducer_destroy(producer);
    callback_pump_destroy(&pump);

    TEST_ASSERT_NULL(close_err);

    TEST_ASSERT_EQUAL_INT(0, join_rc);
    /* Assert this first: if the helper never won the guard there was no overlap
     * window at all, and the probe assertions below would misreport that as
     * "the guard failed to reject". */
    TEST_ASSERT_FALSE(gave_up);
    TEST_ASSERT_TRUE(commit_rejected);
    TEST_ASSERT_TRUE(abort_rejected);
    TEST_ASSERT_TRUE(begin_rejected);
    TEST_ASSERT_TRUE(cb_commit_rejected);
    /* 50ms, not the configured max.block.ms: a threshold equal to max.block.ms
     * bounds the very delay this is meant to detect, so it could never fail. The
     * measured value is ~0.01ms. */
    TEST_ASSERT_TRUE(send_ms < 50.0);
    TEST_ASSERT_FALSE(after_join_was_guard_error);
    TEST_ASSERT_TRUE(send_completion_fired);
}

void test_transaction_methods_on_non_transactional_producer(void) {
    /* No transactional.id: all five control methods must fail, none may hang.
     * max.block.ms is configured down because init_transactions blocks for it and
     * the 60s default would dominate the suite. */
    const char *const pairs[] = {
        "bootstrap.servers", "localhost:1",
        "max.block.ms",      "300",
        NULL
    };
    kafka_producer_KafkaProducer_t *producer = make_producer(pairs);
    const kafka_producer_Producer_t *view = kafka_producer_KafkaProducer__as_Producer(producer);

    /* Every failure below must be the non-transactional-producer error, not the
     * guard rejection: a leaked transaction-control flag would otherwise pass
     * unnoticed. `enable.idempotence` defaults to true, so this producer has a
     * `TransactionManager` and passes Java's `throwIfNoTransactionManager()`;
     * it is the manager's own `ensureTransactional()` that rejects it, with
     * "Transactional method invoked on a non-transactional producer." */
    static const char FRAGMENT[] = "non-transactional producer";

    kafka_common_Error_t *err = kafka_producer_Producer_init_transactions(view);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), FRAGMENT));
    kafka_common_Error_destroy(err);

    err = kafka_producer_Producer_begin_transaction(view);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_FALSE(kafka_common_Error_is_transaction_abortable_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), FRAGMENT));
    kafka_common_Error_destroy(err);

    /* The real-producer arm of send_offsets_to_transaction: the map copy, the
     * group-metadata read and the blocking call all run here. Nothing else in
     * the suite reaches it — the mock tests take the other arm. */
    kafka_consumer_MockConsumer_t *mock_consumer = NULL;
    TEST_ASSERT_NULL(kafka_consumer_MockConsumer_new("earliest", &mock_consumer));
    kafka_consumer_Consumer_t *consumer = kafka_consumer_MockConsumer__as_Consumer(mock_consumer);
    kafka_consumer_ConsumerGroupMetadata_t *group_metadata = kafka_consumer_Consumer_group_metadata(consumer);
    TEST_ASSERT_NOT_NULL(group_metadata);
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new("input-topic", 0);
    kafka_consumer_OffsetAndMetadata_t *oam = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(99, 3, "meta", &oam));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, tp, oam);
    err = kafka_producer_Producer_send_offsets_to_transaction(view, offsets, group_metadata);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), FRAGMENT));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(oam);
    kafka_common_TopicPartition_destroy(tp);

    err = kafka_producer_Producer_commit_transaction(view);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), FRAGMENT));
    kafka_common_Error_destroy(err);

    err = kafka_producer_Producer_abort_transaction(view);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_FALSE(is_txn_guard_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), FRAGMENT));
    kafka_common_Error_destroy(err);

    kafka_consumer_ConsumerGroupMetadata_destroy(group_metadata);
    kafka_consumer_MockConsumer_destroy(mock_consumer); /* the view dies with the mock */
    close_and_destroy(producer);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(void) {
    UNITY_BEGIN();

    /* ProducerConfig */
    RUN_TEST(test_config_new_and_producer);
    RUN_TEST(test_config_builds_several_producers);
    RUN_TEST(test_config_invalid_value);

    /* Lifecycle */
    RUN_TEST(test_create_close_destroy);
    RUN_TEST(test_create_destroy_without_close);
    RUN_TEST(test_destroy_null);
    RUN_TEST(test_create_with_string_serializers);
    RUN_TEST(test_create_multiple_config);

    /* Send */
    RUN_TEST(test_send_cb_unreachable_broker_fails_future_with_timeout);

    /* Transactions */
    RUN_TEST(test_transaction_methods_on_non_transactional_producer);
    RUN_TEST(test_transaction_control_guard_rejects_concurrent_calls);

    return UNITY_END();
}
