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

// C tests for confluent_kafka.h, the header for C users: a function with no
// error channel given a NULL mandatory argument aborts the process with
// Java's message on stderr, rather than crashing on the NULL. Each case runs
// in a forked child, which the parent expects to die of SIGABRT.
#include <confluent_kafka.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include "unity.h"
#include "test_filter.h"

void setUp(void) {}
void tearDown(void) {}

/* Runs `body` in a child and asserts the child was killed by SIGABRT. */
static void assert_child_aborts(void (*body)(void)) {
    pid_t pid = fork();
    TEST_ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        body();
        /* Reached only if the call returned: exit cleanly so the parent fails. */
        _exit(0);
    }
    int status = 0;
    TEST_ASSERT_EQUAL_INT(pid, waitpid(pid, &status, 0));
    TEST_ASSERT_TRUE_MESSAGE(WIFSIGNALED(status), "the child returned instead of aborting");
    TEST_ASSERT_EQUAL_INT(SIGABRT, WTERMSIG(status));
}

static void uuid_to_string_of_null(void) {
    kafka_string_destroy(kafka_common_Uuid_to_string(NULL));
}

/* A plain function given a NULL handle: "self_ must not be null", then abort. */
static void test_null_handle_aborts(void) {
    assert_child_aborts(uuid_to_string_of_null);
}

static void uuid_from_string_with_null_out_err(void) {
    kafka_common_Uuid_destroy(kafka_common_Uuid_from_string(NULL, NULL));
}

/* A fallible function given a NULL out_err has nowhere to store its error. */
static void test_null_out_err_aborts(void) {
    assert_child_aborts(uuid_from_string_with_null_out_err);
}

/* Destroying NULL stays a no-op: it is not a mandatory argument. */
static void test_destroy_of_null_is_a_no_op(void) {
    kafka_common_Uuid_destroy(NULL);
    kafka_string_destroy(NULL);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_null_handle_aborts);
    RUN_TEST(test_null_out_err_aborts);
    RUN_TEST(test_destroy_of_null_is_a_no_op);
    return UNITY_END();
}
