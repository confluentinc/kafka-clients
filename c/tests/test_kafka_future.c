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
#include <stdio.h>
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

static void *double_apply(void *self, const void *a, kafka_common_Error_t **out_err) {
    (void)out_err;
    doubler_t *doubler = self;
    doubler->calls++;
    doubler->output = *(const int64_t *)a * 2;
    return &doubler->output;
}

// Fails the way every fallible method pointer does: an owned error in
// *out_err, whose return value is then ignored.
static void *failing_apply(void *self, const void *a, kafka_common_Error_t **out_err) {
    (void)self;
    (void)a;
    *out_err = kafka_common_Error_local_illegal_argument("apply failed");
    return NULL;
}

static void test_completed_future_delivers_the_value_or_the_error(void) {
    int32_t value = 5;
    kafka_common_KafkaFuture_t *future = kafka_common_KafkaFuture_completed_future(&value, NULL);
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_KafkaFuture_is_done(future));

    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_KafkaFuture_get(future, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&value, out);
    out = kafka_common_KafkaFuture_get_with_timeout(future, 100, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&value, out);
    kafka_common_KafkaFuture_destroy(future);
    TEST_ASSERT_EQUAL_INT32(5, value);

    // The error is consumed by the future and comes back owned from `get`.
    future = kafka_common_KafkaFuture_completed_future(NULL, kafka_common_Error_timeout("boom"));
    out = kafka_common_KafkaFuture_get(future, &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(out);
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
    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_KafkaFuture_get(all, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(out);
    kafka_common_KafkaFuture_destroy(all);

    kafka_List_add(futures, failed);
    all = kafka_common_KafkaFuture_all_of(futures);
    out = kafka_common_KafkaFuture_get(all, &error);
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
    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_KafkaFuture_BaseFunction_apply(function, &a, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    TEST_ASSERT_EQUAL_INT64(42, *(int64_t *)out);
    TEST_ASSERT_EQUAL_INT(1, doubler.calls);

    kafka_common_KafkaFuture_BaseFunction_t *failing = kafka_common_KafkaFuture_BaseFunction_new(NULL, failing_apply);
    out = kafka_common_KafkaFuture_BaseFunction_apply(failing, &a, &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(out);
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

    // `then_apply` consumes the function handle; `self` must outlive the
    // derived future. The source is complete, so the function runs inside
    // `then_apply`.
    kafka_common_KafkaFuture_t *derived = kafka_common_KafkaFuture_then_apply(source, function);
    TEST_ASSERT_EQUAL_INT(1, doubler.calls);
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_KafkaFuture_is_done(derived));

    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_KafkaFuture_get(derived, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    TEST_ASSERT_EQUAL_INT64(42, *(int64_t *)out);
    out = kafka_common_KafkaFuture_get(derived, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    TEST_ASSERT_EQUAL_INT(1, doubler.calls);

    // Chained off the derived future it doubles again.
    function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    kafka_common_KafkaFuture_t *chained = kafka_common_KafkaFuture_then_apply(derived, function);
    out = kafka_common_KafkaFuture_get(chained, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_INT64(84, *(int64_t *)out);
    TEST_ASSERT_EQUAL_INT(2, doubler.calls);
    out = kafka_common_KafkaFuture_get(source, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&value, out);

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
    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_KafkaFuture_get(derived, &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(out);
    TEST_ASSERT_EQUAL_STRING("apply failed", kafka_common_Error_message(error));
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_Error_is_local_illegal_argument_error(error));
    kafka_common_Error_destroy(error);
    error = NULL;

    // A failed source propagates without calling the function.
    doubler_t doubler = {0, 0};
    kafka_common_KafkaFuture_BaseFunction_t *function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    kafka_common_KafkaFuture_t *failed_source =
        kafka_common_KafkaFuture_completed_future(NULL, kafka_common_Error_timeout("late"));
    kafka_common_KafkaFuture_t *from_failed = kafka_common_KafkaFuture_then_apply(failed_source, function);
    out = kafka_common_KafkaFuture_get(from_failed, &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("late", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);
    TEST_ASSERT_EQUAL_INT(0, doubler.calls);

    kafka_common_KafkaFuture_destroy(derived);
    kafka_common_KafkaFuture_destroy(source);
    kafka_common_KafkaFuture_destroy(from_failed);
    kafka_common_KafkaFuture_destroy(failed_source);
}

// A C `BaseFunction` over a record future: reads the offset of the
// `RecordMetadata` behind `a`, counting its calls.
typedef struct {
    int64_t offset;
    int calls;
} offset_reader_t;

static void *read_offset(void *self, const void *a, kafka_common_Error_t **out_err) {
    (void)out_err;
    offset_reader_t *reader = self;
    reader->calls++;
    reader->offset = kafka_producer_RecordMetadata_offset(a);
    return &reader->offset;
}

// Over a client's pending future, `then_apply`'s function runs once, when the
// source completes: queued on the client, since C code never runs on a Rust
// task, and run by its `__execute_callbacks`, which completes the dependant.
// Reading the dependant again does not run it again; over a source already
// complete it runs inline.
static void test_then_apply_runs_the_function_once_when_the_source_completes(void) {
    kafka_producer_MockProducer_t *mock = kafka_producer_MockProducer_with_auto_complete(0);
    const kafka_producer_Producer_t *producer = kafka_producer_MockProducer__as_Producer(mock);
    kafka_common_Error_t *error = NULL;
    kafka_producer_ProducerRecord_t *record =
        kafka_producer_ProducerRecord_with_partition_key("topic", 0, NULL, NULL, &error);
    TEST_ASSERT_NULL(error);
    kafka_common_KafkaFuture_t *source = kafka_producer_Producer_send(producer, record, &error);
    TEST_ASSERT_NULL(error);
    kafka_producer_ProducerRecord_destroy(record);
    kafka_producer_Producer__execute_callbacks(producer);

    offset_reader_t reader = {-1, 0};
    kafka_common_KafkaFuture_t *derived =
        kafka_common_KafkaFuture_then_apply(source, kafka_common_KafkaFuture_BaseFunction_new(&reader, read_offset));
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_KafkaFuture_is_done(derived));

    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(mock));
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_KafkaFuture_is_done(source));
    TEST_ASSERT_EQUAL_INT(0, reader.calls);
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_KafkaFuture_is_done(derived));

    TEST_ASSERT_TRUE(kafka_producer_Producer__execute_callbacks(producer) >= 1);
    TEST_ASSERT_EQUAL_INT(1, reader.calls);
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_KafkaFuture_is_done(derived));
    for (int i = 0; i < 3; i++) {
        void *out = kafka_common_KafkaFuture_get(derived, &error);
        TEST_ASSERT_NULL(error);
        TEST_ASSERT_EQUAL_PTR(&reader.offset, out);
        TEST_ASSERT_EQUAL_INT64(0, *(int64_t *)out);
    }
    TEST_ASSERT_EQUAL_INT(1, reader.calls);

    kafka_common_KafkaFuture_t *inline_derived =
        kafka_common_KafkaFuture_then_apply(source, kafka_common_KafkaFuture_BaseFunction_new(&reader, read_offset));
    TEST_ASSERT_EQUAL_INT(2, reader.calls);
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_KafkaFuture_is_done(inline_derived));

    kafka_common_KafkaFuture_destroy(inline_derived);
    kafka_common_KafkaFuture_destroy(derived);
    kafka_common_KafkaFuture_destroy(source);
    kafka_producer_MockProducer_destroy(mock);
}

// Destroying the dependant before its source completes cancels the function:
// the queued job then runs nothing, so `self` may be freed once `destroy`
// returns (kept alive here to count the calls).
static void test_then_apply_does_not_run_the_function_of_a_destroyed_future(void) {
    kafka_producer_MockProducer_t *mock = kafka_producer_MockProducer_with_auto_complete(0);
    const kafka_producer_Producer_t *producer = kafka_producer_MockProducer__as_Producer(mock);
    kafka_common_Error_t *error = NULL;
    kafka_producer_ProducerRecord_t *record =
        kafka_producer_ProducerRecord_with_partition_key("topic", 0, NULL, NULL, &error);
    TEST_ASSERT_NULL(error);
    kafka_common_KafkaFuture_t *source = kafka_producer_Producer_send(producer, record, &error);
    TEST_ASSERT_NULL(error);
    kafka_producer_ProducerRecord_destroy(record);
    kafka_producer_Producer__execute_callbacks(producer);

    offset_reader_t reader = {-1, 0};
    kafka_common_KafkaFuture_t *derived =
        kafka_common_KafkaFuture_then_apply(source, kafka_common_KafkaFuture_BaseFunction_new(&reader, read_offset));
    kafka_common_KafkaFuture_destroy(derived);

    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(mock));
    kafka_producer_Producer__execute_callbacks(producer);
    TEST_ASSERT_EQUAL_INT(0, reader.calls);

    kafka_common_KafkaFuture_destroy(source);
    kafka_producer_MockProducer_destroy(mock);
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

// Whether a thread of this process has a name starting with prefix, from
// /proc/self/task/*/comm (where Linux cuts a name to 15 bytes).
static int has_thread_named(const char *prefix) {
    DIR *dir = opendir("/proc/self/task");
    TEST_ASSERT_NOT_NULL(dir);
    int found = 0;
    struct dirent *entry;
    while (!found && (entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        char path[300];
        snprintf(path, sizeof path, "/proc/self/task/%s/comm", entry->d_name);
        FILE *comm = fopen(path, "r");
        if (comm == NULL) continue;  // the thread ended meanwhile
        char name[32] = {0};
        if (fgets(name, sizeof name, comm) != NULL) found = strncmp(name, prefix, strlen(prefix)) == 0;
        fclose(comm);
    }
    closedir(dir);
    return found;
}

// The thread count when the process started, before any test ran.
static int initial_threads;
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
// before returning, and none starts a thread to drive it. The count is
// compared with the one main() took before any test ran, since the tests
// before this one get client-less futures too: before d4475efa0 the first
// such get built a process-wide runtime with a worker named
// kafka-ffi-future, alive for the rest of the process.
static void test_a_future_without_a_client_starts_no_thread(void) {
    int64_t value = 4;
    doubler_t doubler = {0, 0};
    kafka_common_KafkaFuture_t *source = kafka_common_KafkaFuture_completed_future(&value, NULL);
    kafka_common_KafkaFuture_BaseFunction_t *function = kafka_common_KafkaFuture_BaseFunction_new(&doubler, double_apply);
    kafka_common_KafkaFuture_t *derived = kafka_common_KafkaFuture_then_apply(source, function);
    kafka_List_t *futures = kafka_List_new();
    kafka_List_add(futures, source);
    kafka_List_add(futures, derived);
    kafka_common_KafkaFuture_t *all = kafka_common_KafkaFuture_all_of(futures);
    kafka_List_destroy(futures);

    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_KafkaFuture_get_with_timeout(derived, 0, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_INT64(8, *(int64_t *)out);
    out = kafka_common_KafkaFuture_get_with_timeout(derived, 1000, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_PTR(&doubler.output, out);
    out = kafka_common_KafkaFuture_get_with_timeout(all, 1000, &error);
    TEST_ASSERT_NULL(error);
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
    TEST_ASSERT_FALSE(has_thread_named("kafka-ffi-futur"));
    TEST_ASSERT_EQUAL_INT(initial_threads, thread_count());
#endif
    kafka_common_KafkaFuture_destroy(all);
    kafka_common_KafkaFuture_destroy(derived);
    kafka_common_KafkaFuture_destroy(source);
}

int main(void) {
#ifdef __linux__
    initial_threads = thread_count();
#endif
    UNITY_BEGIN();
    RUN_TEST(test_completed_future_delivers_the_value_or_the_error);
    RUN_TEST(test_all_of_resolves_to_null_and_reports_the_first_failure);
    RUN_TEST(test_base_function_is_reached_through_the_invoker);
    RUN_TEST(test_then_apply_runs_the_function_once_and_borrows_its_result);
    RUN_TEST(test_then_apply_fails_with_the_functions_error_or_the_sources);
    RUN_TEST(test_then_apply_runs_the_function_once_when_the_source_completes);
    RUN_TEST(test_then_apply_does_not_run_the_function_of_a_destroyed_future);
    RUN_TEST(test_a_future_without_a_client_starts_no_thread);
    return UNITY_END();
}
