// Copyright 2026 Confluent Inc.
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

// C binding tests for confluent_kafka_bindings.h, the header for language
// bindings: every fallible function and every _cb function of
// confluent_kafka.h under its own name, and every other function f as its
// twin f_err, which takes a trailing kafka_common_Error_t **out_err for a
// caught panic and otherwise returns what f returns.
//
// This file includes the bindings header alone, as a binding does: the two
// headers declare the same types, so no translation unit includes both, and
// test_support.h (which includes confluent_kafka.h) is not used here.
#include <confluent_kafka_bindings.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

/* Fails with the message of `err` unless it is NULL. */
static void assert_ok(kafka_common_Error_t *err) {
    if (err == NULL) {
        return;
    }
    kafka_common_Error_t *message_err = NULL;
    const char *message = kafka_common_Error_message_err(err, &message_err);
    TEST_ASSERT_NULL(message_err);
    TEST_FAIL_MESSAGE(message);
}

/* An infallible function's twin returns the function's value and leaves
 * *out_err untouched; so does a constructor's, whose value is the new handle.
 * A void function's twin returns nothing. */
static void test_twin_returns_the_value_and_leaves_out_err_alone(void) {
    kafka_common_Error_t *err = NULL;
    kafka_common_Uuid_t *uuid = kafka_common_Uuid_new_err(1, 2, &err);
    assert_ok(err);
    TEST_ASSERT_NOT_NULL(uuid);
    TEST_ASSERT_EQUAL_INT64(1, kafka_common_Uuid_most_significant_bits_err(uuid, &err));
    assert_ok(err);
    TEST_ASSERT_EQUAL_INT64(2, kafka_common_Uuid_least_significant_bits_err(uuid, &err));
    assert_ok(err);
    char *text = kafka_common_Uuid_to_string_err(uuid, &err);
    assert_ok(err);
    TEST_ASSERT_EQUAL_STRING("AAAAAAAAAAEAAAAAAAAAAg", text);
    kafka_string_destroy_err(text, &err);
    assert_ok(err);
    kafka_common_Uuid_destroy_err(uuid, &err);
    assert_ok(err);
}

/* A fallible function is declared under its own name, with the same out_err:
 * here the LocalIllegalArgument of Java's `Uuid.fromString`, returned with
 * NULL as the failure value. */
static void test_fallible_function_keeps_its_name_and_its_errors(void) {
    kafka_common_Error_t *err = NULL;
    kafka_common_Uuid_t *uuid = kafka_common_Uuid_from_string("AAAA", &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(uuid);
    kafka_common_Error_t *read_err = NULL;
    TEST_ASSERT_EQUAL_INT(kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT, kafka_common_Error__enum_err(err, &read_err));
    assert_ok(read_err);
    const char *message = kafka_common_Error_message_err(err, &read_err);
    assert_ok(read_err);
    TEST_ASSERT_EQUAL_STRING(
        "Input string `AAAA` decoded as 3 bytes, which is not equal to the expected 16 bytes of a "
        "base64-encoded UUID",
        message);
    kafka_common_Error_destroy_err(err, &read_err);
    assert_ok(read_err);

    /* The same function succeeds on a valid string, *out_err untouched. */
    err = NULL;
    uuid = kafka_common_Uuid_from_string("AAAAAAAAAAEAAAAAAAAAAg", &err);
    assert_ok(err);
    TEST_ASSERT_NOT_NULL(uuid);
    TEST_ASSERT_EQUAL_INT64(2, kafka_common_Uuid_least_significant_bits_err(uuid, &err));
    assert_ok(err);
    kafka_common_Uuid_destroy_err(uuid, &err);
    assert_ok(err);
}

/* A metrics view is iterated with the `_err` twins of kafka_Iterator_t, and
 * `next` past the end, Java's NoSuchElementException, is a caught panic
 * reported as an IllegalStateError. */
static void test_iterator_past_the_end_reports_an_illegal_state(void) {
    kafka_common_Error_t *err = NULL;
    kafka_common_metrics_Metrics_t *registry = kafka_common_metrics_Metrics_new_err(&err);
    assert_ok(err);
    kafka_common_metrics_MetricsView_t *view = kafka_common_metrics_Metrics_metrics_err(registry, &err);
    assert_ok(err);
    kafka_Iterator_t *it = kafka_common_metrics_MetricsView_iterator_err(view, &err);
    assert_ok(err);
    int count = 0;
    while (kafka_Iterator_has_next_err(it, &err)) {
        assert_ok(err);
        const kafka_Map_Entry_t *entry = kafka_Iterator_next_err(it, &err);
        assert_ok(err);
        const kafka_common_MetricName_t *name = kafka_Map_Entry_key_err(entry, &err);
        assert_ok(err);
        /* The registry's only metric is its built-in `count`. */
        TEST_ASSERT_EQUAL_STRING("count", kafka_common_MetricName_name_err(name, &err));
        assert_ok(err);
        TEST_ASSERT_NOT_NULL(kafka_Map_Entry_value_err(entry, &err));
        assert_ok(err);
        count++;
    }
    assert_ok(err);
    TEST_ASSERT_EQUAL_INT(1, count);
    TEST_ASSERT_NULL(kafka_Iterator_next_err(it, &err));
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_t *read_err = NULL;
    TEST_ASSERT_EQUAL_INT(kafka_common_Error_LOCAL_ILLEGAL_STATE, kafka_common_Error__enum_err(err, &read_err));
    assert_ok(read_err);
    const char *message = kafka_common_Error_message_err(err, &read_err);
    assert_ok(read_err);
    TEST_ASSERT_EQUAL_STRING(
        "Rust panic caught at the FFI boundary in kafka_Iterator_next_err: the iteration has no more elements",
        message);
    kafka_common_Error_destroy_err(err, &read_err);
    assert_ok(read_err);
    err = NULL;
    kafka_Iterator_destroy_err(it, &err);
    assert_ok(err);
    kafka_common_metrics_MetricsView_destroy_err(view, &err);
    assert_ok(err);
    kafka_common_metrics_Metrics_destroy_err(registry, &err);
    assert_ok(err);
}

static void on_closed(void *opaque) {
    atomic_fetch_add((atomic_int *)opaque, 1);
}

/* A `_cb` function is declared under its own name and keeps its completion as
 * the channel: queued on the client's callback queue, run exactly once by
 * `__execute_callbacks_err`, which reports how many it ran. */
static void test_cb_function_completes_through_the_callback_queue(void) {
    kafka_common_Error_t *err = NULL;
    kafka_admin_MockAdminClient_Builder_t *builder = kafka_admin_MockAdminClient_create_err(&err);
    assert_ok(err);
    kafka_admin_MockAdminClient_t *mock = kafka_admin_MockAdminClient_Builder_build(builder, &err);
    assert_ok(err);
    kafka_admin_MockAdminClient_Builder_destroy_err(builder, &err);
    assert_ok(err);
    TEST_ASSERT_NOT_NULL(mock);
    const kafka_admin_Admin_t *admin = kafka_admin_MockAdminClient__as_Admin_err(mock, &err);
    assert_ok(err);
    TEST_ASSERT_NOT_NULL(admin);

    atomic_int fired = 0;
    kafka_admin_Admin_close_cb(admin, on_closed, &fired);
    int32_t executed = 0;
    /* The completion is queued from a Rust task: poll for up to 10 s. */
    for (int i = 0; i < 1000 && atomic_load(&fired) == 0; i++) {
        int32_t ran = kafka_admin_Admin__execute_callbacks_err(admin, &err);
        assert_ok(err);
        TEST_ASSERT_TRUE(ran >= 0);
        executed += ran;
        if (atomic_load(&fired) == 0) {
            struct timespec pause = {0, 10 * 1000 * 1000};
            nanosleep(&pause, NULL);
        }
    }
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&fired));
    TEST_ASSERT_EQUAL_INT32(1, executed);
    /* Nothing is left to run, and the completion does not fire again. */
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_Admin__execute_callbacks_err(admin, &err));
    assert_ok(err);

    kafka_admin_MockAdminClient_destroy_err(mock, &err);
    assert_ok(err);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&fired));
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_twin_returns_the_value_and_leaves_out_err_alone);
    RUN_TEST(test_fallible_function_keeps_its_name_and_its_errors);
    RUN_TEST(test_cb_function_completes_through_the_callback_queue);
    RUN_TEST(test_iterator_past_the_end_reports_an_illegal_state);
    return UNITY_END();
}
