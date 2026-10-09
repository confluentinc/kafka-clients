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

    // `then_apply_try` is the same operation in C: every `apply` carries the
    // error slot. Chained off the derived future it doubles again.
    function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    kafka_common_KafkaFuture_t *chained = kafka_common_KafkaFuture_then_apply_try(derived, function);
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

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_completed_future_delivers_the_value_or_the_error);
    RUN_TEST(test_all_of_resolves_to_null_and_reports_the_first_failure);
    RUN_TEST(test_base_function_is_reached_through_the_invoker);
    RUN_TEST(test_then_apply_runs_the_function_once_and_borrows_its_result);
    RUN_TEST(test_then_apply_fails_with_the_functions_error_or_the_sources);
    return UNITY_END();
}
