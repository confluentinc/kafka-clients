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
 * Temporary C11 <threads.h> shim for Apple, whose SDK does not ship the header.
 * Stopgap until the vendored tinycthread shim (zlib) lands.
 */

#ifndef CONFLUENT_KAFKA_THREADS_SHIM_H
#define CONFLUENT_KAFKA_THREADS_SHIM_H

#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

typedef pthread_t       thrd_t;
typedef pthread_mutex_t mtx_t;
typedef pthread_cond_t  cnd_t;
typedef int (*thrd_start_t)(void *);

#define mtx_plain    0
#define thrd_success 0

#define mtx_init(m, type)      pthread_mutex_init((m), NULL)
#define mtx_lock(m)            pthread_mutex_lock(m)
#define mtx_unlock(m)          pthread_mutex_unlock(m)
#define mtx_destroy(m)         pthread_mutex_destroy(m)
#define cnd_init(c)            pthread_cond_init((c), NULL)
#define cnd_wait(c, m)         pthread_cond_wait((c), (m))
#define cnd_timedwait(c, m, t) pthread_cond_timedwait((c), (m), (t))
#define cnd_signal(c)          pthread_cond_signal(c)
#define cnd_destroy(c)         pthread_cond_destroy(c)
#define thrd_join(t, res)      pthread_join((t), NULL)
#define thrd_sleep(dur, rem)   nanosleep((dur), (rem))

/* C11's entry point returns int; pthread_create wants void *(*)(void *). */
struct cf_thrd_arg { thrd_start_t func; void *arg; };

static inline void *cf_thrd_run(void *p) {
    struct cf_thrd_arg a = *(struct cf_thrd_arg *)p;
    free(p);
    return (void *)(intptr_t)a.func(a.arg);
}

static inline int thrd_create(thrd_t *thr, thrd_start_t func, void *arg) {
    struct cf_thrd_arg *a = malloc(sizeof(*a));
    if (a == NULL) {
        return 1;
    }
    a->func = func;
    a->arg = arg;
    if (pthread_create(thr, NULL, cf_thrd_run, a) != 0) {
        free(a);
        return 1;
    }
    return thrd_success;
}

#endif /* CONFLUENT_KAFKA_THREADS_SHIM_H */
