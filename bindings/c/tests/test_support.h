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
 * Async FFI callbacks fire on the producer's dispatcher thread, so a test must
 * synchronize on a flag rather than assume the callback ran inline.
 */

#ifndef CONFLUENT_KAFKA_TEST_SUPPORT_H
#define CONFLUENT_KAFKA_TEST_SUPPORT_H

#include <stdatomic.h>
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

#endif /* CONFLUENT_KAFKA_TEST_SUPPORT_H */
