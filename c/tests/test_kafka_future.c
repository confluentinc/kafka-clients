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

// `org.apache.kafka.common.KafkaFuture` through the C FFI (CLAUDE.md §4):
// the one generic `kafka_common_KafkaFuture_t` whose value is a `void *`,
// built here from `completed_future`, combined with `all_of` and mapped with
// `then_apply` through a C `KafkaFuture.BaseFunction` — Java's functional
// interface, which C implements as a rule 3 interface since it has no
// closures.

#include <confluent_kafka.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __linux__
#include <dirent.h>
#endif

#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// ---------------------------------------------------------------------------
// A C `BaseFunction`: doubles the `int64_t` behind `a` into a slot `self`
// owns, counting its calls.
// ---------------------------------------------------------------------------

typedef struct {
    int64_t output;
    int calls;
} doubler_t;

static kafka_common_Error_t *double_apply(void *self, void *a, void **out_apply) {
    doubler_t *doubler = self;
    doubler->calls++;
    doubler->output = *(const int64_t *)a * 2;
    *out_apply = &doubler->output;
    return NULL;
}

static kafka_common_Error_t *failing_apply(void *self, void *a, void **out_apply) {
    (void)self;
    (void)a;
    (void)out_apply;
    return kafka_common_Error_local_illegal_argument("apply failed");
}

static void test_completed_future_delivers_the_value_or_the_error(void) {
    int32_t value = 5;
    kafka_common_KafkaFuture_t *future = kafka_common_KafkaFuture_completed_future(&value, NULL);
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_KafkaFuture_is_done(future));

    void *out = NULL;
    kafka_common_Error_t *error = kafka_common_KafkaFuture_get(future, &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&value, out);
    error = kafka_common_KafkaFuture_get_with_timeout(future, 100, &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&value, out);
    kafka_common_KafkaFuture_destroy(future);
    TEST_ASSERT_EQUAL_INT32(5, value);

    // The error is consumed by the future and comes back owned from `get`.
    future = kafka_common_KafkaFuture_completed_future(NULL, kafka_common_Error_timeout("boom"));
    error = kafka_common_KafkaFuture_get(future, &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("boom", kafka_common_Error_message(error));
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_Error_is_timeout_error(error));
    kafka_common_Error_destroy(error);
    kafka_common_KafkaFuture_destroy(future);
    kafka_common_KafkaFuture_destroy(NULL);
}

static void test_all_of_resolves_to_null_and_reports_the_first_failure(void) {
    int32_t value = 1;
    kafka_common_KafkaFuture_t *ok = kafka_common_KafkaFuture_completed_future(&value, NULL);
    kafka_common_KafkaFuture_t *failed =
        kafka_common_KafkaFuture_completed_future(NULL, kafka_common_Error_local_illegal_state("bad"));

    kafka_List_t *futures = kafka_List_new();
    kafka_List_add(futures, ok);
    kafka_common_KafkaFuture_t *all = kafka_common_KafkaFuture_all_of(futures);
    void *out = &value;
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get(all, &out));
    TEST_ASSERT_NULL(out);
    kafka_common_KafkaFuture_destroy(all);

    kafka_List_add(futures, failed);
    all = kafka_common_KafkaFuture_all_of(futures);
    kafka_common_Error_t *error = kafka_common_KafkaFuture_get(all, &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("bad", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    // The inputs are borrowed: they may go before the combined future.
    kafka_List_destroy(futures);
    kafka_common_KafkaFuture_destroy(ok);
    kafka_common_KafkaFuture_destroy(failed);
    kafka_common_KafkaFuture_destroy(all);
}

static void test_base_function_is_reached_through_the_invoker(void) {
    doubler_t doubler = {0, 0};
    kafka_common_KafkaFuture_BaseFunction_t *function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    int64_t a = 21;
    void *out = NULL;
    kafka_common_Error_t *error = kafka_common_KafkaFuture_BaseFunction_apply(function, &a, &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    TEST_ASSERT_EQUAL_INT64(42, *(int64_t *)out);
    TEST_ASSERT_EQUAL_INT(1, doubler.calls);

    kafka_common_KafkaFuture_BaseFunction_t *failing = kafka_common_KafkaFuture_BaseFunction_new(NULL, failing_apply);
    error = kafka_common_KafkaFuture_BaseFunction_apply(failing, &a, &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("apply failed", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_common_KafkaFuture_BaseFunction_destroy(function);
    kafka_common_KafkaFuture_BaseFunction_destroy(failing);
    kafka_common_KafkaFuture_BaseFunction_destroy(NULL);
}

static void test_then_apply_runs_the_function_once_and_borrows_its_result(void) {
    int64_t value = 21;
    kafka_common_KafkaFuture_t *source = kafka_common_KafkaFuture_completed_future(&value, NULL);
    doubler_t doubler = {0, 0};
    kafka_common_KafkaFuture_BaseFunction_t *function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);

    kafka_common_KafkaFuture_t *derived = kafka_common_KafkaFuture_then_apply(source, function);
    // The derived future copied the registration; `self` must outlive it.
    kafka_common_KafkaFuture_BaseFunction_destroy(function);
    TEST_ASSERT_EQUAL_INT(0, doubler.calls);

    void *out = NULL;
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get(derived, &out));
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    TEST_ASSERT_EQUAL_INT64(42, *(int64_t *)out);
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get(derived, &out));
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    TEST_ASSERT_EQUAL_INT(1, doubler.calls);

    // Chained off the derived future it doubles again.
    function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    kafka_common_KafkaFuture_t *chained = kafka_common_KafkaFuture_then_apply(derived, function);
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get(chained, &out));
    TEST_ASSERT_EQUAL_INT64(84, *(int64_t *)out);
    TEST_ASSERT_EQUAL_INT(2, doubler.calls);
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get(source, &out));
    TEST_ASSERT_EQUAL_PTR(&value, out);

    kafka_common_KafkaFuture_BaseFunction_destroy(function);
    kafka_common_KafkaFuture_destroy(chained);
    kafka_common_KafkaFuture_destroy(derived);
    kafka_common_KafkaFuture_destroy(source);
    TEST_ASSERT_EQUAL_INT64(84, doubler.output);
}

static void test_then_apply_fails_with_the_functions_error_or_the_sources(void) {
    int64_t value = 1;
    kafka_common_KafkaFuture_t *source = kafka_common_KafkaFuture_completed_future(&value, NULL);
    kafka_common_KafkaFuture_BaseFunction_t *failing = kafka_common_KafkaFuture_BaseFunction_new(NULL, failing_apply);
    kafka_common_KafkaFuture_t *derived = kafka_common_KafkaFuture_then_apply(source, failing);
    void *out = NULL;
    kafka_common_Error_t *error = kafka_common_KafkaFuture_get(derived, &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("apply failed", kafka_common_Error_message(error));
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_Error_is_local_illegal_argument_error(error));
    kafka_common_Error_destroy(error);

    // A failed source propagates without calling the function.
    doubler_t doubler = {0, 0};
    kafka_common_KafkaFuture_BaseFunction_t *function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    kafka_common_KafkaFuture_t *failed_source =
        kafka_common_KafkaFuture_completed_future(NULL, kafka_common_Error_timeout("late"));
    kafka_common_KafkaFuture_t *from_failed = kafka_common_KafkaFuture_then_apply(failed_source, function);
    error = kafka_common_KafkaFuture_get(from_failed, &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("late", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);
    TEST_ASSERT_EQUAL_INT(0, doubler.calls);

    kafka_common_KafkaFuture_BaseFunction_destroy(failing);
    kafka_common_KafkaFuture_BaseFunction_destroy(function);
    kafka_common_KafkaFuture_destroy(derived);
    kafka_common_KafkaFuture_destroy(source);
    kafka_common_KafkaFuture_destroy(from_failed);
    kafka_common_KafkaFuture_destroy(failed_source);
}

#ifdef __linux__
// The number of threads in this process, from /proc/self/task.
static int thread_count(void) {
    DIR *dir = opendir("/proc/self/task");
    TEST_ASSERT_NOT_NULL(dir);
    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] != '.') count++;
    }
    closedir(dir);
    return count;
}
#endif

typedef struct {
    void *value;
    int calls;
} seen_t;

static void record_value(void *value, kafka_common_Error_t *error, void *opaque) {
    seen_t *seen = opaque;
    TEST_ASSERT_NULL(error);
    seen->value = value;
    seen->calls++;
}

// A future with no client (completed_future, and then_apply / all_of over
// such futures) is already complete: every get answers it on this thread,
// before returning, and none starts a thread to drive it.
static void test_a_future_without_a_client_starts_no_thread(void) {
#ifdef __linux__
    int threads = thread_count();
#endif
    int64_t value = 4;
    doubler_t doubler = {0, 0};
    kafka_common_KafkaFuture_t *source = kafka_common_KafkaFuture_completed_future(&value, NULL);
    kafka_common_KafkaFuture_BaseFunction_t *function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    kafka_common_KafkaFuture_t *derived = kafka_common_KafkaFuture_then_apply(source, function);
    kafka_common_KafkaFuture_BaseFunction_destroy(function);
    kafka_List_t *futures = kafka_List_new();
    kafka_List_add(futures, source);
    kafka_List_add(futures, derived);
    kafka_common_KafkaFuture_t *all = kafka_common_KafkaFuture_all_of(futures);
    kafka_List_destroy(futures);

    void *out = NULL;
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get_with_timeout(derived, 0, &out));
    TEST_ASSERT_EQUAL_INT64(8, *(int64_t *)out);
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get_with_timeout(derived, 1000, &out));
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    out = &value;
    TEST_ASSERT_NULL(kafka_common_KafkaFuture_get_with_timeout(all, 1000, &out));
    TEST_ASSERT_NULL(out);

    seen_t seen = {NULL, 0};
    kafka_common_KafkaFuture_get_with_timeout_cb(derived, 1000, record_value, &seen);
    TEST_ASSERT_EQUAL_INT(1, seen.calls);
    TEST_ASSERT_EQUAL_PTR(&doubler.output, seen.value);
    kafka_common_KafkaFuture_get_cb(source, record_value, &seen);
    TEST_ASSERT_EQUAL_INT(2, seen.calls);
    TEST_ASSERT_EQUAL_PTR(&value, seen.value);
    TEST_ASSERT_EQUAL_INT(1, doubler.calls);

#ifdef __linux__
    TEST_ASSERT_EQUAL_INT(threads, thread_count());
#endif
    kafka_common_KafkaFuture_destroy(all);
    kafka_common_KafkaFuture_destroy(derived);
    kafka_common_KafkaFuture_destroy(source);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_completed_future_delivers_the_value_or_the_error);
    RUN_TEST(test_all_of_resolves_to_null_and_reports_the_first_failure);
    RUN_TEST(test_base_function_is_reached_through_the_invoker);
    RUN_TEST(test_then_apply_runs_the_function_once_and_borrows_its_result);
    RUN_TEST(test_then_apply_fails_with_the_functions_error_or_the_sources);
    RUN_TEST(test_a_future_without_a_client_starts_no_thread);
    return UNITY_END();
}
