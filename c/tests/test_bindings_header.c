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
// bindings: every function f of confluent_kafka.h as its twin f_err, which
// returns kafka_common_Error_t * (NULL on success) and delivers any other
// return value through a trailing out_result.
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

/* Asserts `err` is NULL, failing with its message otherwise. */
static void assert_ok(kafka_common_Error_t *err) {
    if (err == NULL) {
        return;
    }
    const char *message = NULL;
    TEST_ASSERT_NULL(kafka_common_Error_message_err(err, &message));
    TEST_FAIL_MESSAGE(message);
}

/* An infallible getter's twin writes its value to out_result and returns
 * NULL; so does a constructor's, whose value is the new handle. */
static void test_value_twin_writes_out_result(void) {
    kafka_common_Uuid_t *uuid = NULL;
    assert_ok(kafka_common_Uuid_new_err(1, 2, &uuid));
    TEST_ASSERT_NOT_NULL(uuid);
    int64_t most = -7;
    assert_ok(kafka_common_Uuid_most_significant_bits_err(uuid, &most));
    TEST_ASSERT_EQUAL_INT64(1, most);
    int64_t least = -7;
    assert_ok(kafka_common_Uuid_least_significant_bits_err(uuid, &least));
    TEST_ASSERT_EQUAL_INT64(2, least);
    char *text = NULL;
    assert_ok(kafka_common_Uuid_to_string_err(uuid, &text));
    TEST_ASSERT_EQUAL_STRING("AAAAAAAAAAEAAAAAAAAAAg", text);
    /* A void function's twin returns NULL. */
    assert_ok(kafka_string_destroy_err(text));
    assert_ok(kafka_common_Uuid_destroy_err(uuid));
}

/* A fallible function's twin has its signature and its errors: here the
 * LocalIllegalArgument of Java's `Uuid.fromString`, out_<name> unwritten. */
static void test_error_twin_returns_the_same_error(void) {
    kafka_common_Uuid_t *uuid = NULL;
    kafka_common_Error_t *err = kafka_common_Uuid_from_string_err("AAAA", &uuid);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(uuid);
    kafka_common_Error_e code = kafka_common_Error_UNKNOWN_SERVER_ERROR;
    assert_ok(kafka_common_Error__enum_err(err, &code));
    TEST_ASSERT_EQUAL_INT(kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT, code);
    const char *message = NULL;
    assert_ok(kafka_common_Error_message_err(err, &message));
    TEST_ASSERT_EQUAL_STRING(
        "Input string `AAAA` decoded as 3 bytes, which is not equal to the expected 16 bytes of a "
        "base64-encoded UUID",
        message);
    assert_ok(kafka_common_Error_destroy_err(err));

    /* The same twin succeeds on a valid string. */
    assert_ok(kafka_common_Uuid_from_string_err("AAAAAAAAAAEAAAAAAAAAAg", &uuid));
    TEST_ASSERT_NOT_NULL(uuid);
    int64_t least = -7;
    assert_ok(kafka_common_Uuid_least_significant_bits_err(uuid, &least));
    TEST_ASSERT_EQUAL_INT64(2, least);
    assert_ok(kafka_common_Uuid_destroy_err(uuid));
}

static void on_closed(void *opaque) {
    atomic_fetch_add((atomic_int *)opaque, 1);
}

/* A `_cb` function's twin returns NULL and keeps its completion as the
 * channel: queued on the client's callback queue, run exactly once by
 * `__execute_callbacks_err`, which reports how many it ran. */
static void test_cb_twin_completes_through_the_callback_queue(void) {
    kafka_admin_MockAdminClient_t *mock = NULL;
    assert_ok(kafka_admin_MockAdminClient_create_err(1, &mock));
    TEST_ASSERT_NOT_NULL(mock);
    const kafka_admin_Admin_t *admin = NULL;
    assert_ok(kafka_admin_MockAdminClient__as_Admin_err(mock, &admin));
    TEST_ASSERT_NOT_NULL(admin);

    atomic_int fired = 0;
    assert_ok(kafka_admin_Admin_close_cb_err(admin, on_closed, &fired));
    int32_t executed = 0;
    /* The completion is queued from a Rust task: poll for up to 10 s. */
    for (int i = 0; i < 1000 && atomic_load(&fired) == 0; i++) {
        int32_t ran = -1;
        assert_ok(kafka_admin_Admin__execute_callbacks_err(admin, &ran));
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
    int32_t ran = -1;
    assert_ok(kafka_admin_Admin__execute_callbacks_err(admin, &ran));
    TEST_ASSERT_EQUAL_INT32(0, ran);

    /* `_destroy_err` frees the client and returns NULL. */
    assert_ok(kafka_admin_MockAdminClient_destroy_err(mock));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&fired));
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_value_twin_writes_out_result);
    RUN_TEST(test_error_twin_returns_the_same_error);
    RUN_TEST(test_cb_twin_completes_through_the_callback_queue);
    return UNITY_END();
}
