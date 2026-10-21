/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Helpers shared between the C binding test binaries.
 *
 * No Rust thread ever runs a C callback (CLAUDE.md §4, "Async variants of
 * blocking methods"): the `_cb` completions, the delivery callbacks and the
 * interface methods a `_cb` operation triggers (a consumer's rebalance
 * listener, say) are queued on the client's callback vector, and
 * `<Client>__execute_callbacks` runs them on the thread that calls it. A test
 * therefore pumps the vector itself; `callback_pump_t` below does that, woken
 * by the notify hook the client fires once each time the vector goes from
 * empty to non-empty. The pump is client-agnostic: `callback_pump_install`
 * binds it to a producer, `consumer_callback_pump_install` to a consumer.
 */

#ifndef CONFLUENT_KAFKA_TEST_SUPPORT_H
#define CONFLUENT_KAFKA_TEST_SUPPORT_H

#include <confluent_kafka.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include "unity.h"

/* Spins up to ~5s for `*flag` to reach `expected`. Returns 1 on success. */
static inline int wait_for(atomic_int *flag, int expected) {
    for (int i = 0; i < 5000; i++) {
        if (atomic_load(flag) >= expected) {
            return 1;
        }
        struct timespec ts = {0, 1000000}; /* 1ms */
        nanosleep(&ts, NULL);
    }
    return atomic_load(flag) >= expected;
}

/* The bound every wait below uses, in milliseconds. */
#define CALLBACK_PUMP_TIMEOUT_MS 5000

/* A client's callback pump: the notify hook installed by
 * `callback_pump_install` / `consumer_callback_pump_install` only signals the
 * condition variable (the hook may schedule, never run callbacks);
 * `callback_pump_until` waits on it and drains the vector with the client's
 * `__execute_callbacks` on the calling thread. */
typedef struct {
    const void *client;                        /* the producer or consumer handle */
    int32_t (*execute)(const void *client);    /* its `__execute_callbacks` */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int signalled;        /* notify firings not yet consumed by a pump */
    atomic_int notified;  /* total notify firings, for assertions */
    atomic_int executed;  /* sum of what execute_callbacks returned */
} callback_pump_t;

/* The `<Client>_callbacks_notify_fn_t` of a pump (both clients share the
 * `void (*)(void *opaque)` shape). */
static inline void callback_pump_notify(void *opaque) {
    callback_pump_t *pump = (callback_pump_t *)opaque;
    pthread_mutex_lock(&pump->mutex);
    pump->signalled++;
    atomic_fetch_add(&pump->notified, 1);
    pthread_cond_signal(&pump->cond);
    pthread_mutex_unlock(&pump->mutex);
}

static inline int32_t callback_pump_execute_producer(const void *client) {
    return kafka_producer_Producer__execute_callbacks((const kafka_producer_Producer_t *)client);
}

static inline int32_t callback_pump_execute_consumer(const void *client) {
    return kafka_consumer_Consumer__execute_callbacks((const kafka_consumer_Consumer_t *)client);
}

static inline void callback_pump_init(callback_pump_t *pump, const void *client,
                                      int32_t (*execute)(const void *client)) {
    pump->client = client;
    pump->execute = execute;
    pthread_mutex_init(&pump->mutex, NULL);
    pthread_cond_init(&pump->cond, NULL);
    pump->signalled = 0;
    atomic_init(&pump->notified, 0);
    atomic_init(&pump->executed, 0);
}

/* Initializes `pump` and installs its notify hook on `producer`. `pump` must
 * outlive the producer handle (or be uninstalled with a different hook). */
static inline void callback_pump_install(callback_pump_t *pump,
                                         const kafka_producer_Producer_t *producer) {
    callback_pump_init(pump, producer, callback_pump_execute_producer);
    kafka_producer_Producer__set_callbacks_notify(producer, callback_pump_notify, pump);
}

/* The consumer twin of `callback_pump_install`: `consumer` is any
 * `kafka_consumer_Consumer_t` (a KafkaConsumer's or a mock's `__as_Consumer`
 * view). */
static inline void consumer_callback_pump_install(callback_pump_t *pump,
                                                  const kafka_consumer_Consumer_t *consumer) {
    callback_pump_init(pump, consumer, callback_pump_execute_consumer);
    kafka_consumer_Consumer__set_callbacks_notify(consumer, callback_pump_notify, pump);
}

static inline void callback_pump_destroy(callback_pump_t *pump) {
    pthread_cond_destroy(&pump->cond);
    pthread_mutex_destroy(&pump->mutex);
}

/* Waits (at most CALLBACK_PUMP_TIMEOUT_MS in total) for the notify hook to
 * fire, without running anything. Returns 1 if it fired, 0 on timeout. The
 * consumed signal is accounted for, so a following `callback_pump_until` does
 * not need a second notify to drain what was announced. */
static inline int callback_pump_wait_notify(callback_pump_t *pump) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += CALLBACK_PUMP_TIMEOUT_MS / 1000;
    int fired = 0;
    pthread_mutex_lock(&pump->mutex);
    while (pump->signalled == 0) {
        if (pthread_cond_timedwait(&pump->cond, &pump->mutex, &deadline) != 0) {
            break;
        }
    }
    if (pump->signalled > 0) {
        fired = 1;
    }
    pthread_mutex_unlock(&pump->mutex);
    return fired;
}

/* Runs `execute_callbacks` once, accumulating the returned count. */
static inline int32_t callback_pump_execute(callback_pump_t *pump) {
    int32_t executed = pump->execute(pump->client);
    atomic_fetch_add(&pump->executed, executed);
    pthread_mutex_lock(&pump->mutex);
    pump->signalled = 0;
    pthread_mutex_unlock(&pump->mutex);
    return executed;
}

/* Pumps until `*flag` reaches `expected`: waits for the notify hook (bounded
 * by CALLBACK_PUMP_TIMEOUT_MS overall), drains the vector on this thread,
 * re-checks. Returns 1 on success, 0 on timeout. */
static inline int callback_pump_until(callback_pump_t *pump, atomic_int *flag, int expected) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += CALLBACK_PUMP_TIMEOUT_MS / 1000;
    while (atomic_load(flag) < expected) {
        pthread_mutex_lock(&pump->mutex);
        int timed_out = 0;
        while (pump->signalled == 0 && !timed_out) {
            if (pthread_cond_timedwait(&pump->cond, &pump->mutex, &deadline) != 0) {
                timed_out = 1;
            }
        }
        pthread_mutex_unlock(&pump->mutex);
        callback_pump_execute(pump);
        if (timed_out && atomic_load(flag) < expected) {
            return 0;
        }
    }
    return 1;
}

/* Fails the test with the error's message when a fallible call stored an
 * error in `err`, which it frees; `what` names the call. A fallible function
 * takes a trailing `kafka_common_Error_t **out_err` that must point at NULL
 * on entry and is written only on failure, so a test declares
 * `kafka_common_Error_t *err = NULL;`, passes `&err` and checks it here. */
static inline void assert_ok(kafka_common_Error_t *err, const char *what) {
    if (err != NULL) {
        char msg[256];
        snprintf(msg, sizeof msg, "%s failed: %s", what, kafka_common_Error_message(err));
        kafka_common_Error_destroy(err);
        TEST_FAIL_MESSAGE(msg);
    }
}

/* The consumer's state reads (`assignment`, `subscription`, `paused`,
 * `current_lag`, `group_metadata`) fail with LOCAL_CONCURRENT_MODIFICATION
 * only while another call holds the consumer, as Java's `acquire()` throws
 * `ConcurrentModificationException`. These wrappers are for a test reading
 * with nothing else in flight: any error fails the test. */
static inline kafka_List_t *consumer_assignment(kafka_consumer_Consumer_t *consumer) {
    kafka_common_Error_t *err = NULL;
    kafka_List_t *out = kafka_consumer_Consumer_assignment(consumer, &err);
    assert_ok(err, "assignment");
    return out;
}

static inline kafka_List_t *consumer_subscription(kafka_consumer_Consumer_t *consumer) {
    kafka_common_Error_t *err = NULL;
    kafka_List_t *out = kafka_consumer_Consumer_subscription(consumer, &err);
    assert_ok(err, "subscription");
    return out;
}

static inline kafka_List_t *consumer_paused(kafka_consumer_Consumer_t *consumer) {
    kafka_common_Error_t *err = NULL;
    kafka_List_t *out = kafka_consumer_Consumer_paused(consumer, &err);
    assert_ok(err, "paused");
    return out;
}

static inline int64_t consumer_current_lag(kafka_consumer_Consumer_t *consumer,
                                           const kafka_common_TopicPartition_t *tp) {
    kafka_common_Error_t *err = NULL;
    int64_t out = kafka_consumer_Consumer_current_lag(consumer, tp, &err);
    assert_ok(err, "current_lag");
    return out;
}

static inline kafka_consumer_ConsumerGroupMetadata_t *consumer_group_metadata(
    kafka_consumer_Consumer_t *consumer) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerGroupMetadata_t *out = kafka_consumer_Consumer_group_metadata(consumer, &err);
    assert_ok(err, "group_metadata");
    return out;
}

/* What a `current_lag_cb` completion observed: the lag (-1 for Java's empty
 * `OptionalLong`, or beside an error) and the owned error, if any, kept for
 * the test to classify and free. Both twins' completions
 * (`kafka_consumer_Consumer_current_lag_cb_t`,
 * `kafka_consumer_ConsumerHandle_current_lag_cb_t`) share this shape. */
typedef struct {
    atomic_int fired;
    int64_t value;
    kafka_common_Error_t *error;
} lag_completion_t;

static inline void lag_completion_init(lag_completion_t *c) {
    c->value = 0;
    c->error = NULL;
    atomic_init(&c->fired, 0);
}

static inline void on_lag(int64_t value, kafka_common_Error_t *error, void *opaque) {
    lag_completion_t *c = (lag_completion_t *)opaque;
    c->value = value;
    c->error = error;
    atomic_fetch_add(&c->fired, 1);
}

#endif /* CONFLUENT_KAFKA_TEST_SUPPORT_H */
