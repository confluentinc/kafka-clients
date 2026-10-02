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

// Portability shim for the C11 <threads.h> API used by _confluentkafka.c.
//
// glibc (Linux / CI) ships <threads.h>, so there we include it unchanged.
// Apple's SDK has never shipped <threads.h> (verified absent through the
// current 26.x SDKs), so on platforms lacking it we provide exactly the
// subset the batching producer uses, implemented over POSIX threads:
//
//   thrd_t, thrd_create, thrd_join, thrd_sleep, thrd_success
//   mtx_t, mtx_plain, mtx_init, mtx_lock, mtx_unlock, mtx_destroy
//   cnd_t, cnd_init, cnd_wait, cnd_timedwait, cnd_signal, cnd_destroy
//   timespec_get / TIME_UTC
//
// This is original portability code, not an OpenJDK translation, hence the
// Apache 2.0 header above (Confluent Inc.).

#ifndef C11THREADS_COMPAT_H
#define C11THREADS_COMPAT_H

#if defined(__has_include) && __has_include(<threads.h>)

// Native C11 threads available (glibc, musl, ...). Use it as-is so the
// Linux/CI code path is unchanged.
#include <threads.h>

#else  // ---- POSIX fallback (macOS) --------------------------------------

#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>

// C11 return codes (§7.26.1). Only thrd_success / thrd_timedout / thrd_nomem
// / thrd_error are meaningful for the API subset below.
enum {
    thrd_success = 0,
    thrd_nomem = 1,
    thrd_timedout = 2,
    thrd_busy = 3,
    thrd_error = 4
};

// C11 mutex types (§7.26.4.1). Only mtx_plain is used by _confluentkafka.c;
// the others are provided for completeness of the enum.
enum {
    mtx_plain = 0,
    mtx_recursive = 1,
    mtx_timed = 2
};

typedef pthread_t thrd_t;
typedef pthread_mutex_t mtx_t;
typedef pthread_cond_t cnd_t;

// C11 thread-start signature: `int (*)(void*)`. pthread wants
// `void* (*)(void*)`, so thrd_create wraps the C11 entry point in a tiny
// heap-allocated trampoline that adapts the signatures. Casting the function
// pointer directly and calling through the wrong prototype would be undefined
// behaviour (differing return types), so the trampoline is used instead. The
// one malloc per thread is irrelevant: thread creation happens twice per
// producer (send + poll-futures tasks), never on a hot path.
typedef int (*thrd_start_t)(void*);

struct c11threads_trampoline_arg {
    thrd_start_t func;
    void* arg;
};

static inline void* c11threads_trampoline(void* p) {
    struct c11threads_trampoline_arg local = *(struct c11threads_trampoline_arg*)p;
    free(p);
    return (void*)(intptr_t)local.func(local.arg);
}

static inline int thrd_create(thrd_t* thr, thrd_start_t func, void* arg) {
    struct c11threads_trampoline_arg* wrapped =
        (struct c11threads_trampoline_arg*)malloc(sizeof(*wrapped));
    if (wrapped == NULL) {
        return thrd_nomem;
    }
    wrapped->func = func;
    wrapped->arg = arg;
    if (pthread_create(thr, NULL, c11threads_trampoline, wrapped) != 0) {
        free(wrapped);
        return thrd_error;
    }
    return thrd_success;
}

static inline int thrd_join(thrd_t thr, int* res) {
    void* ret = NULL;
    if (pthread_join(thr, &ret) != 0) {
        return thrd_error;
    }
    if (res != NULL) {
        *res = (int)(intptr_t)ret;
    }
    return thrd_success;
}

// C11 thrd_sleep: sleeps for `duration`; on interrupt fills `remaining` and
// returns -1; other errors return a negative value other than -1. The caller
// in _confluentkafka.c ignores the return value, but the semantics are kept
// faithful.
static inline int thrd_sleep(const struct timespec* duration,
                             struct timespec* remaining) {
    if (nanosleep(duration, remaining) == 0) {
        return 0;
    }
    return errno == EINTR ? -1 : -2;
}

static inline int mtx_init(mtx_t* mtx, int type) {
    (void)type;  // only mtx_plain is used; a plain pthread mutex matches it.
    return pthread_mutex_init(mtx, NULL) == 0 ? thrd_success : thrd_error;
}

static inline int mtx_lock(mtx_t* mtx) {
    return pthread_mutex_lock(mtx) == 0 ? thrd_success : thrd_error;
}

static inline int mtx_unlock(mtx_t* mtx) {
    return pthread_mutex_unlock(mtx) == 0 ? thrd_success : thrd_error;
}

static inline void mtx_destroy(mtx_t* mtx) {
    pthread_mutex_destroy(mtx);
}

static inline int cnd_init(cnd_t* cond) {
    return pthread_cond_init(cond, NULL) == 0 ? thrd_success : thrd_error;
}

static inline int cnd_signal(cnd_t* cond) {
    return pthread_cond_signal(cond) == 0 ? thrd_success : thrd_error;
}

static inline int cnd_wait(cnd_t* cond, mtx_t* mtx) {
    return pthread_cond_wait(cond, mtx) == 0 ? thrd_success : thrd_error;
}

// C11 cnd_timedwait takes an ABSOLUTE timeout expressed against TIME_UTC (the
// realtime clock). pthread_cond_timedwait also takes an absolute timeout on
// CLOCK_REALTIME by default, so the timespec passes straight through. The one
// call site pairs this with timespec_get(TIME_UTC) below, keeping the clock
// domains consistent.
static inline int cnd_timedwait(cnd_t* cond, mtx_t* mtx,
                                const struct timespec* ts) {
    int rc = pthread_cond_timedwait(cond, mtx, ts);
    if (rc == 0) {
        return thrd_success;
    }
    if (rc == ETIMEDOUT) {
        return thrd_timedout;
    }
    return thrd_error;
}

static inline void cnd_destroy(cnd_t* cond) {
    pthread_cond_destroy(cond);
}

// timespec_get / TIME_UTC live in C11 <time.h>, not <threads.h>, but some SDKs
// gate them behind __STDC_VERSION__ >= 201112L and _confluentkafka.c compiles
// with -std=c99. Provide them only if the platform <time.h> did not (guard on
// TIME_UTC so we never collide with a system definition).
#ifndef TIME_UTC
#define TIME_UTC 1
static inline int timespec_get(struct timespec* ts, int base) {
    if (base != TIME_UTC) {
        return 0;
    }
    return clock_gettime(CLOCK_REALTIME, ts) == 0 ? base : 0;
}
#endif  // TIME_UTC

#endif  // native <threads.h> vs POSIX fallback

#endif  // C11THREADS_COMPAT_H
