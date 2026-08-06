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

// End-to-end smoke tests for the real (broker-backed) AdminClient C FFI.
//
// These run without a reachable broker, mirroring test_kafka_consumer.c and
// test_kafka_producer.c: AdminClient construction only parses the config and
// spawns the background task (it does not connect synchronously), so it
// succeeds; every RPC below carries an explicit short timeout so it returns
// promptly instead of hanging on the unreachable bootstrap. We never assert a
// successful round-trip — if localhost:9092 happens to be live the RPC may
// succeed, and both outcomes are accepted (and freed).
//
// test_mock_admin.c covers the RPC semantics against MockAdminClient. What only
// this file can cover is the production path: building and *entering* the tokio
// runtime so `new_admin_client` can spawn its background task, close/destroy
// against `AdminKind::Kafka` rather than `AdminKind::Mock`, and `mock_ref`
// rejecting a production handle.

#include <confluent_kafka.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

/* An RPC timeout short enough that an unreachable bootstrap fails fast. */
#define RPC_TIMEOUT_MS (1000)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/* Spins up to ~10s for `*flag` to reach `expected`. Returns 1 on success. */
static int wait_for(atomic_int *flag, int expected) {
    for (int i = 0; i < 10000; i++) {
        if (atomic_load(flag) >= expected) {
            return 1;
        }
        struct timespec ts = {0, 1000000}; /* 1ms */
        nanosleep(&ts, NULL);
    }
    return atomic_load(flag) >= expected;
}

/* Builds a production AdminClient against an unreachable bootstrap. Asserts the
 * construction succeeded (the address is only parsed, not connected). */
static kafka_admin_AdminClient_t *create_admin(void) {
    const char *configs[] = {
        "bootstrap.servers",     "localhost:9092",
        "client.id",             "c-kafka-admin-test",
        "default.api.timeout.ms", "1000",
        "request.timeout.ms",     "500",
        NULL
    };
    kafka_admin_AdminClientProperties_t *props =
        kafka_admin_AdminClientProperties_from_configs(configs);
    TEST_ASSERT_NOT_NULL(props);

    kafka_common_KafkaError_t *err = NULL;
    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, &err);
    kafka_admin_AdminClientProperties_destroy(props);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(admin);
    return admin;
}

// ---------------------------------------------------------------------------
// Construction / lifecycle on the production client
// ---------------------------------------------------------------------------

/* The delicate part of construction: the tokio runtime is built and *entered*
 * so `new_admin_client` can `tokio::spawn` the admin background task. On
 * success `*out_error` is explicitly cleared. */
static void test_kafka_admin_new_succeeds(void) {
    kafka_admin_AdminClient_t *admin = create_admin();
    kafka_admin_AdminClient_destroy(admin);
}

/* Same, built one key at a time through `_put`, and with a NULL out_error (the
 * caller does not want error details). */
static void test_kafka_admin_new_via_put_and_null_out_error(void) {
    kafka_admin_AdminClientProperties_t *props = kafka_admin_AdminClientProperties_new();
    TEST_ASSERT_NOT_NULL(props);
    kafka_admin_AdminClientProperties_put(props, "bootstrap.servers", "localhost:9092");
    kafka_admin_AdminClientProperties_put(props, "client.id", "c-kafka-admin-put");

    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, NULL);
    kafka_admin_AdminClientProperties_destroy(props);
    TEST_ASSERT_NOT_NULL(admin);
    kafka_admin_AdminClient_destroy(admin);
}

/* close() then destroy() against AdminKind::Kafka: close awaits the background
 * task (Java's Admin.close(Duration)), destroy shuts down the runtime. */
static void test_kafka_admin_close_then_destroy(void) {
    kafka_admin_AdminClient_t *admin = create_admin();
    kafka_admin_AdminClient_close(admin, RPC_TIMEOUT_MS);
    kafka_admin_AdminClient_destroy(admin);
}

/* close() is idempotent: the second call finds no background task left to
 * await and returns at once. A negative timeout is Java's no-argument close()
 * (wait indefinitely), which must still return promptly here. */
static void test_kafka_admin_close_idempotent_and_negative_timeout(void) {
    kafka_admin_AdminClient_t *admin = create_admin();
    kafka_admin_AdminClient_close(admin, -1);
    kafka_admin_AdminClient_close(admin, RPC_TIMEOUT_MS);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_error;
} close_result_t;

static void on_close(kafka_common_KafkaError_t *error, void *user_data) {
    close_result_t *r = (close_result_t *)user_data;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

/* close_async against AdminKind::Kafka: the callback fires exactly once on the
 * dispatcher thread with a null error (Java's close(Duration) is void). */
static void test_kafka_admin_close_async(void) {
    kafka_admin_AdminClient_t *admin = create_admin();
    close_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_close_async(admin, RPC_TIMEOUT_MS, on_close, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_FALSE(r.had_error);
    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// Mock-only drivers must reject a production handle
// ---------------------------------------------------------------------------

/* `kafka_admin_MockAdminClient_*` take the same opaque handle as the rest of the
 * API, so a production handle has to be rejected at runtime rather than by the
 * type system. Only reachable with a real client, hence this test. */
static void test_kafka_admin_mock_driver_rejects_production_handle(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_common_KafkaError_t *err =
        kafka_admin_MockAdminClient_timeout_next_request(admin, 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("this operation is only supported on a MockAdminClient",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* The same driver with a NULL handle reports the null-handle error instead. */
static void test_kafka_admin_mock_driver_null_handle(void) {
    kafka_common_KafkaError_t *err =
        kafka_admin_MockAdminClient_timeout_next_request(NULL, 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("admin handle must not be null",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);
}

// ---------------------------------------------------------------------------
// RPCs against the unreachable bootstrap: they must return, not hang
// ---------------------------------------------------------------------------

/* A sync RPC on AdminKind::Kafka drives `admin_sync_value_op`'s block_on over
 * the real client. With no broker it fails (typically a timeout); if a broker
 * happens to be listening it may succeed. Either outcome is fine — what is
 * asserted is that exactly one of them is produced, promptly, and that the
 * ownership contract holds (a non-null return means *out_result was left
 * untouched). */
static void test_kafka_admin_list_topics_returns_without_hanging(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_ListTopicsResult_t *result = NULL;
    kafka_common_KafkaError_t *err =
        kafka_admin_AdminClient_list_topics(admin, RPC_TIMEOUT_MS, true, &result);
    if (err != NULL) {
        TEST_ASSERT_NULL(result);
        TEST_ASSERT_NOT_NULL(kafka_common_KafkaError_message(err));
        kafka_common_KafkaError_destroy(err);
    } else {
        TEST_ASSERT_NOT_NULL(result);
        kafka_admin_ListTopicsResult_destroy(result);
    }

    kafka_admin_AdminClient_close(admin, RPC_TIMEOUT_MS);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
} list_result_t;

static void on_list(kafka_admin_ListTopicsResult_t *result,
                    kafka_common_KafkaError_t *error, void *user_data) {
    list_result_t *r = (list_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        kafka_admin_ListTopicsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

/* The async twin: exactly one callback, with exactly one of result / error. */
static void test_kafka_admin_list_topics_async_fires_once(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    list_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_topics_async(admin, RPC_TIMEOUT_MS, true, on_list, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, r.had_result + r.had_error);

    kafka_admin_AdminClient_close(admin, RPC_TIMEOUT_MS);
    kafka_admin_AdminClient_destroy(admin);
}

/* Argument marshaling runs before the RPC is submitted, so an unparseable topic
 * id fails identically on the production client — with no network involved. */
static void test_kafka_admin_describe_topics_by_ids_rejects_bad_id(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    const char *bad_ids[1] = {"not-a-base64-uuid"};
    kafka_admin_DescribeTopicsResult_t *result = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_describe_topics_by_ids(
        admin, bad_ids, 1, RPC_TIMEOUT_MS, false, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* An empty batch never reaches the network either: every per-key future is
 * absent, so the flattened result is empty and the call succeeds. */
static void test_kafka_admin_empty_batches_need_no_broker(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_CreateTopicsResult_t *created = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_topics(admin, NULL, 0,
                                                           RPC_TIMEOUT_MS, false, true,
                                                           &created));
    TEST_ASSERT_NOT_NULL(created);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_CreateTopicsResult_count(created));
    kafka_admin_CreateTopicsResult_destroy(created);

    kafka_admin_DeleteTopicsResult_t *deleted = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_topics(admin, NULL, 0,
                                                           RPC_TIMEOUT_MS, true,
                                                           &deleted));
    TEST_ASSERT_NOT_NULL(deleted);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteTopicsResult_count(deleted));
    kafka_admin_DeleteTopicsResult_destroy(deleted);

    kafka_admin_CreatePartitionsResult_t *grown = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_partitions(admin, NULL, NULL, 0,
                                                               RPC_TIMEOUT_MS, false,
                                                               true, &grown));
    TEST_ASSERT_NOT_NULL(grown);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_CreatePartitionsResult_count(grown));
    kafka_admin_CreatePartitionsResult_destroy(grown);

    kafka_admin_DeleteRecordsResult_t *trimmed = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_records(admin, NULL, NULL, NULL, 0,
                                                            RPC_TIMEOUT_MS, &trimmed));
    TEST_ASSERT_NOT_NULL(trimmed);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteRecordsResult_count(trimmed));
    kafka_admin_DeleteRecordsResult_destroy(trimmed);

    kafka_admin_AdminClient_destroy(admin);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_kafka_admin_new_succeeds);
    RUN_TEST(test_kafka_admin_new_via_put_and_null_out_error);
    RUN_TEST(test_kafka_admin_close_then_destroy);
    RUN_TEST(test_kafka_admin_close_idempotent_and_negative_timeout);
    RUN_TEST(test_kafka_admin_close_async);
    RUN_TEST(test_kafka_admin_mock_driver_rejects_production_handle);
    RUN_TEST(test_kafka_admin_mock_driver_null_handle);
    RUN_TEST(test_kafka_admin_list_topics_returns_without_hanging);
    RUN_TEST(test_kafka_admin_list_topics_async_fires_once);
    RUN_TEST(test_kafka_admin_describe_topics_by_ids_rejects_bad_id);
    RUN_TEST(test_kafka_admin_empty_batches_need_no_broker);
    return UNITY_END();
}
