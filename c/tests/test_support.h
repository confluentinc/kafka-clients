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
 * blocking methods"): the `_cb` completions and the delivery callbacks are
 * queued on the client's callback vector, and `<Client>_execute_callbacks`
 * runs them on the thread that calls it. A test therefore pumps the vector
 * itself; `callback_pump_t` below does that, woken by the notify hook the
 * client fires once each time the vector goes from empty to non-empty.
 */

#ifndef CONFLUENT_KAFKA_TEST_SUPPORT_H
#define CONFLUENT_KAFKA_TEST_SUPPORT_H

#include <confluent_kafka.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <time.h>

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

/* A producer's callback pump: the notify hook installed by
 * `callback_pump_install` only signals the condition variable (the hook may
 * schedule, never run callbacks); `callback_pump_until` waits on it and
 * drains the vector with `kafka_producer_Producer_execute_callbacks` on the
 * calling thread. */
typedef struct {
    const kafka_producer_Producer_t *producer;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int signalled;        /* notify firings not yet consumed by a pump */
    atomic_int notified;  /* total notify firings, for assertions */
    atomic_int executed;  /* sum of what execute_callbacks returned */
} callback_pump_t;

/* The `kafka_producer_Producer_callbacks_notify_fn_t` of a pump. */
static inline void callback_pump_notify(void *opaque) {
    callback_pump_t *pump = (callback_pump_t *)opaque;
    pthread_mutex_lock(&pump->mutex);
    pump->signalled++;
    atomic_fetch_add(&pump->notified, 1);
    pthread_cond_signal(&pump->cond);
    pthread_mutex_unlock(&pump->mutex);
}

/* Initializes `pump` and installs its notify hook on `producer`. `pump` must
 * outlive the producer handle (or be uninstalled with a different hook). */
static inline void callback_pump_install(callback_pump_t *pump,
                                         const kafka_producer_Producer_t *producer) {
    pump->producer = producer;
    pthread_mutex_init(&pump->mutex, NULL);
    pthread_cond_init(&pump->cond, NULL);
    pump->signalled = 0;
    atomic_init(&pump->notified, 0);
    atomic_init(&pump->executed, 0);
    kafka_producer_Producer_set_callbacks_notify(producer, callback_pump_notify, pump);
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
    int32_t executed = kafka_producer_Producer_execute_callbacks(pump->producer);
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

#endif /* CONFLUENT_KAFKA_TEST_SUPPORT_H */
