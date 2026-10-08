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
// test_kafka_producer.c: `kafka_admin_AdminClient_create` only parses the
// config and spawns the background task (it does not connect synchronously),
// so it succeeds; every RPC below returns its `*Result_t` at once, and the
// per-key `kafka_common_KafkaFuture_t`s it holds resolve within the configured
// `default.api.timeout.ms` instead of hanging on the unreachable bootstrap. We
// never assert a successful round-trip — if localhost:9092 happens to be live
// the future may succeed, and both outcomes are accepted (and freed).
//
// test_mock_admin.c covers the RPC semantics against MockAdminClient. What
// only this file can cover is the production path: building and *entering*
// the tokio runtime so `KafkaAdminClient::new` can spawn its background task,
// blocking on a future that a background task completes, and close/destroy
// against a `KafkaAdminClient` rather than a mock.
//
// Lifetimes follow the C API's conventions: a `const *` return is borrowed, a
// plain `*` return is owned; values delivered by `kafka_common_KafkaFuture_get`
// are borrowed from the future and die with it; a `_values()` map owns its
// futures and `kafka_Map_destroy` frees them.

#include <confluent_kafka.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "test_support.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

/* An RPC timeout short enough that an unreachable bootstrap fails fast. It is
 * the client's `default.api.timeout.ms`, and the per-call `_with_options`
 * forms below pass it explicitly too. */
#define RPC_TIMEOUT_MS (1000)

/* A bootstrap address that is genuinely dead, for the one test below that
 * asserts a *specific* failure rather than merely accepting both outcomes.
 * Port 1 (tcpmux) is privileged, so no unprivileged developer process can be
 * squatting on it, and it is effectively never served -- unlike 9092, which on
 * a Kafka developer's machine frequently has a real broker on it. The literal
 * 127.0.0.1 rather than `localhost` pins the stack, since `localhost` may
 * resolve to ::1 first. A refused connection also returns immediately, so the
 * "comes back inside the explicit timeout" property holds deterministically
 * instead of incidentally. */
#define NO_BROKER_BOOTSTRAP "127.0.0.1:1"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/* Builds a production AdminClient against `bootstrap`. Asserts the construction
 * succeeded (the address is only parsed, not connected). */
static kafka_admin_Admin_t *create_admin_at(const char *bootstrap) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)bootstrap);
    kafka_Map_put(props, (void *)"client.id", (void *)"c-kafka-admin-test");
    kafka_Map_put(props, (void *)"default.api.timeout.ms", (void *)"1000");
    kafka_Map_put(props, (void *)"request.timeout.ms", (void *)"500");
    kafka_admin_AdminClientConfig_t *config = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClientConfig_new(props, &config);
    if (err != NULL) {
        TEST_FAIL_MESSAGE(kafka_common_Error_message(err));
    }
    TEST_ASSERT_NOT_NULL(config);
    kafka_admin_Admin_t *admin = NULL;
    err = kafka_admin_AdminClient_create(config, &admin);
    kafka_admin_AdminClientConfig_destroy(config);
    kafka_Map_destroy(props);
    if (err != NULL) {
        TEST_FAIL_MESSAGE(kafka_common_Error_message(err));
    }
    TEST_ASSERT_NOT_NULL(admin);
    return admin;
}

/* The default: `localhost:9092`, which may or may not have a broker on it. Every
 * caller of this form accepts both outcomes, per the note at the top of the file. */
static kafka_admin_Admin_t *create_admin(void) {
    return create_admin_at("localhost:9092");
}

/* For assertions that only hold with nothing listening. */
static kafka_admin_Admin_t *create_admin_no_broker(void) {
    return create_admin_at(NO_BROKER_BOOTSTRAP);
}

/* Closes with the short timeout and destroys the handle. */
static void close_and_destroy(kafka_admin_Admin_t *admin) {
    kafka_admin_Admin_close_with_timeout(admin, RPC_TIMEOUT_MS);
    kafka_admin_Admin_destroy(admin);
}

/* Awaits an owned future whose outcome is not known in advance (a broker may
 * or may not be listening): exactly one of value / error is produced, the
 * error carries a message, and the future is destroyed here. Returns 1 when
 * the future succeeded. */
static int await_either_outcome(kafka_common_KafkaFuture_t *future) {
    TEST_ASSERT_NOT_NULL(future);
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(future, &value);
    int ok;
    if (err != NULL) {
        TEST_ASSERT_NOT_NULL(kafka_common_Error_message(err));
        kafka_common_Error_destroy(err);
        ok = 0;
    } else {
        ok = 1;
    }
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(future));
    kafka_common_KafkaFuture_destroy(future);
    return ok;
}

/* Awaits an owned future that must fail; returns the owned error. */
static kafka_common_Error_t *await_failure(kafka_common_KafkaFuture_t *future) {
    TEST_ASSERT_NOT_NULL(future);
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(future, &value);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaFuture_destroy(future);
    return err;
}

/* Awaits an owned `KafkaFuture<Void>` that must succeed. */
static void await_void_ok(kafka_common_KafkaFuture_t *future) {
    TEST_ASSERT_NOT_NULL(future);
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(future, &value);
    if (err != NULL) {
        TEST_FAIL_MESSAGE(kafka_common_Error_message(err));
    }
    TEST_ASSERT_NULL(value);
    kafka_common_KafkaFuture_destroy(future);
}

/* Asserts an owned error carries exactly `expected`, then destroys it. */
static void assert_error_message(kafka_common_Error_t *err, const char *expected) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING(expected, kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
}

static kafka_common_TopicPartition_t *tp_new(const char *topic, int32_t partition) {
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new(topic, partition);
    TEST_ASSERT_NOT_NULL(tp);
    return tp;
}

/* The callback pump (test_support.h) over `kafka_admin_Admin_execute_callbacks`:
 * the notify hook only records the wake-up, the test thread runs the queued
 * callbacks. */
static int32_t callback_pump_execute_admin(const void *client) {
    return kafka_admin_Admin_execute_callbacks((const kafka_admin_Admin_t *)client);
}

static void install_pump(callback_pump_t *pump, const kafka_admin_Admin_t *admin) {
    callback_pump_init(pump, admin, callback_pump_execute_admin);
    kafka_admin_Admin_set_callbacks_notify(admin, callback_pump_notify, pump);
}

typedef struct {
    atomic_int fired;
    int had_value;
    int had_error;
} cb_result_t;

static void on_future_done(void *value, kafka_common_Error_t *error, void *opaque) {
    cb_result_t *r = (cb_result_t *)opaque;
    r->had_value = value != NULL;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

typedef struct {
    atomic_int fired;
} close_result_t;

static void on_closed(void *opaque) {
    atomic_fetch_add(&((close_result_t *)opaque)->fired, 1);
}

// ---------------------------------------------------------------------------
// Construction / lifecycle on the production client
// ---------------------------------------------------------------------------

/* The delicate part of construction: the tokio runtime is built and *entered*
 * so `KafkaAdminClient::new` can `tokio::spawn` the admin background task. */
static void test_kafka_admin_create_succeeds(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_admin_Admin_destroy(admin);
}

/* Same, with only the mandatory key: every other setting takes its default. */
static void test_kafka_admin_create_with_defaults(void) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"localhost:9092");
    kafka_admin_AdminClientConfig_t *config = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClientConfig_new(props, &config));
    kafka_admin_Admin_t *admin = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create(config, &admin));
    TEST_ASSERT_NOT_NULL(admin);
    kafka_admin_AdminClientConfig_destroy(config);
    kafka_Map_destroy(props);
    kafka_admin_Admin_destroy(admin);
}

/* close() then destroy() against the production client: close awaits the
 * background task (Java's Admin.close(Duration)), destroy shuts down the
 * runtime. */
static void test_kafka_admin_close_then_destroy(void) {
    kafka_admin_Admin_t *admin = create_admin();
    close_and_destroy(admin);
}

/* close() is idempotent: the second call finds no background task left to
 * await and returns at once. The no-argument form is Java's close() (wait
 * indefinitely, `Long.MAX_VALUE` ms), which must still return promptly here
 * since nothing is in flight. */
static void test_kafka_admin_close_idempotent(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_admin_Admin_close(admin);
    kafka_admin_Admin_close_with_timeout(admin, RPC_TIMEOUT_MS);
    kafka_admin_Admin_close(admin);
    kafka_admin_Admin_destroy(admin);
}

/* `close_cb` against the production client: the completion is queued on the
 * client's callback vector, the notify hook fires, and `execute_callbacks`
 * runs it exactly once on this thread. */
static void test_kafka_admin_close_async(void) {
    kafka_admin_Admin_t *admin = create_admin();
    callback_pump_t pump;
    install_pump(&pump, admin);
    close_result_t r;
    memset(&r, 0, sizeof(r));
    kafka_admin_Admin_close_with_timeout_cb(admin, RPC_TIMEOUT_MS, on_closed, &r);
    TEST_ASSERT_TRUE(callback_pump_until(&pump, &r.fired, 1));
    TEST_ASSERT_TRUE(atomic_load(&pump.notified) >= 1);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&pump));
    kafka_admin_Admin_destroy(admin);
    callback_pump_destroy(&pump);
}

/* `destroy` runs a still-pending close callback, so it fires exactly once. */
static void test_kafka_admin_destroy_runs_pending_close_callback(void) {
    kafka_admin_Admin_t *admin = create_admin();
    callback_pump_t pump;
    install_pump(&pump, admin);
    close_result_t r;
    memset(&r, 0, sizeof(r));
    kafka_admin_Admin_close_cb(admin, on_closed, &r);
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&r.fired));
    kafka_admin_Admin_destroy(admin);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    callback_pump_destroy(&pump);
}

// ---------------------------------------------------------------------------
// RPCs against the unreachable bootstrap: they must return, not hang
// ---------------------------------------------------------------------------

/* A sync `get` on a future the production background task completes. With no
 * broker it fails (typically a timeout); if a broker happens to be listening
 * it may succeed. Either outcome is fine — what is asserted is that exactly
 * one of them is produced, promptly. */
static void test_kafka_admin_list_topics_returns_without_hanging(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_admin_ListTopicsOptions_t *options = kafka_admin_ListTopicsOptions_new();
    kafka_admin_ListTopicsOptions_set_timeout_ms(options, RPC_TIMEOUT_MS);
    kafka_admin_ListTopicsOptions_set_list_internal(options, 1);
    kafka_admin_ListTopicsResult_t *result = kafka_admin_Admin_list_topics_with_options(admin, options);
    kafka_admin_ListTopicsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    await_either_outcome(kafka_admin_ListTopicsResult_names(result));
    kafka_admin_ListTopicsResult_destroy(result);
    close_and_destroy(admin);
}

/* The `get_cb` form: exactly one callback, with exactly one of value / error,
 * delivered through the client's callback queue. */
static void test_kafka_admin_list_topics_async_fires_once(void) {
    kafka_admin_Admin_t *admin = create_admin();
    callback_pump_t pump;
    install_pump(&pump, admin);
    kafka_admin_ListTopicsResult_t *result = kafka_admin_Admin_list_topics(admin);
    kafka_common_KafkaFuture_t *names = kafka_admin_ListTopicsResult_names(result);
    cb_result_t r;
    memset(&r, 0, sizeof(r));
    kafka_common_KafkaFuture_get_cb(names, on_future_done, &r);
    TEST_ASSERT_TRUE(callback_pump_until(&pump, &r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, r.had_value + r.had_error);
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&pump));
    kafka_common_KafkaFuture_destroy(names);
    kafka_admin_ListTopicsResult_destroy(result);
    close_and_destroy(admin);
    callback_pump_destroy(&pump);
}

/* Constructor validation runs before any RPC is submitted, so a bad topic id
 * never reaches the production client at all: `Uuid.fromString` is Java's
 * IllegalArgumentException. */
static void test_kafka_admin_uuid_from_string_rejects_bad_id(void) {
    kafka_common_Uuid_t *id = NULL;
    kafka_common_Error_t *err = kafka_common_Uuid_from_string("not-a-base64-uuid", &id);
    TEST_ASSERT_NULL(id);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
}

/* An empty batch never reaches the network: there is no per-key future, so
 * `all()` resolves at once and the call needs no broker. */
static void test_kafka_admin_empty_batches_need_no_broker(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_List_t *none = kafka_List_new();

    kafka_admin_CreateTopicsResult_t *created = kafka_admin_Admin_create_topics(admin, none);
    TEST_ASSERT_NOT_NULL(created);
    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(created);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_CreateTopicsResult_all(created));
    kafka_admin_CreateTopicsResult_destroy(created);

    kafka_common_TopicCollection_t *no_topics = kafka_common_TopicCollection_of_topic_names(none);
    kafka_admin_DeleteTopicsResult_t *deleted = kafka_admin_Admin_delete_topics(admin, no_topics);
    TEST_ASSERT_NOT_NULL(deleted);
    values = kafka_admin_DeleteTopicsResult_topic_name_values(deleted);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_DeleteTopicsResult_all(deleted));
    kafka_admin_DeleteTopicsResult_destroy(deleted);
    kafka_common_TopicCollection_destroy(no_topics);

    kafka_Map_t *empty = kafka_Map_new();
    kafka_admin_CreatePartitionsResult_t *grown = kafka_admin_Admin_create_partitions(admin, empty);
    TEST_ASSERT_NOT_NULL(grown);
    values = kafka_admin_CreatePartitionsResult_values(grown);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_CreatePartitionsResult_all(grown));
    kafka_admin_CreatePartitionsResult_destroy(grown);

    kafka_admin_DeleteRecordsResult_t *trimmed = kafka_admin_Admin_delete_records(admin, empty);
    TEST_ASSERT_NOT_NULL(trimmed);
    values = kafka_admin_DeleteRecordsResult_low_watermarks(trimmed);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_DeleteRecordsResult_all(trimmed));
    kafka_admin_DeleteRecordsResult_destroy(trimmed);

    kafka_Map_destroy(empty);
    kafka_List_destroy(none);
    close_and_destroy(admin);
}

// ---------------------------------------------------------------------------
// Cluster, configs and log dirs on the production client
// ---------------------------------------------------------------------------

/* describeCluster resolves four futures rather than a per-key map, so with no
 * broker each fails; with one they succeed. Either is accepted — what matters
 * is that they return promptly. */
static void test_kafka_admin_describe_cluster_returns_without_hanging(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_admin_DescribeClusterOptions_t *options = kafka_admin_DescribeClusterOptions_new();
    kafka_admin_DescribeClusterOptions_set_timeout_ms(options, RPC_TIMEOUT_MS);
    kafka_admin_DescribeClusterResult_t *result = kafka_admin_Admin_describe_cluster_with_options(admin, options);
    kafka_admin_DescribeClusterOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    int ok = await_either_outcome(kafka_admin_DescribeClusterResult_cluster_id(result));
    /* The sibling futures share the one response, so they agree. */
    TEST_ASSERT_EQUAL_INT(ok, await_either_outcome(kafka_admin_DescribeClusterResult_nodes(result)));
    TEST_ASSERT_EQUAL_INT(ok, await_either_outcome(kafka_admin_DescribeClusterResult_controller(result)));
    kafka_admin_DescribeClusterResult_destroy(result);
    close_and_destroy(admin);
}

/* The `get_cb` form: exactly one callback, with exactly one of value / error. */
static void test_kafka_admin_describe_cluster_async_fires_once(void) {
    kafka_admin_Admin_t *admin = create_admin();
    callback_pump_t pump;
    install_pump(&pump, admin);
    kafka_admin_DescribeClusterResult_t *result = kafka_admin_Admin_describe_cluster(admin);
    kafka_common_KafkaFuture_t *cluster_id = kafka_admin_DescribeClusterResult_cluster_id(result);
    cb_result_t r;
    memset(&r, 0, sizeof(r));
    kafka_common_KafkaFuture_get_cb(cluster_id, on_future_done, &r);
    TEST_ASSERT_TRUE(callback_pump_until(&pump, &r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, r.had_value + r.had_error);
    kafka_common_KafkaFuture_destroy(cluster_id);
    kafka_admin_DescribeClusterResult_destroy(result);
    close_and_destroy(admin);
    callback_pump_destroy(&pump);
}

/* listConfigResources has a single future in Java, so it behaves like
 * listTopics: one outcome, promptly. */
static void test_kafka_admin_list_config_resources_returns_without_hanging(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_admin_ListConfigResourcesResult_t *result = kafka_admin_Admin_list_config_resources(admin);
    TEST_ASSERT_NOT_NULL(result);
    await_either_outcome(kafka_admin_ListConfigResourcesResult_all(result));
    kafka_admin_ListConfigResourcesResult_destroy(result);
    close_and_destroy(admin);
}

/* An unknown `AlterConfigOp.OpType` code has no singleton, so a bad op type
 * cannot be built, let alone submitted — the same place an unparseable topic
 * id is stopped. */
static void test_kafka_admin_alter_config_op_type_for_id_rejects_unknown_code(void) {
    TEST_ASSERT_NULL(kafka_admin_AlterConfigOp_OpType_for_id(123));
    TEST_ASSERT_TRUE(kafka_admin_AlterConfigOp_OpType_for_id(0) == kafka_admin_AlterConfigOp_OpType_set());
}

/* Empty config and log-dir batches never reach the network either. */
static void test_kafka_admin_config_empty_batches_need_no_broker(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_List_t *none = kafka_List_new();
    kafka_Map_t *empty = kafka_Map_new();

    kafka_admin_DescribeConfigsResult_t *described = kafka_admin_Admin_describe_configs(admin, none);
    TEST_ASSERT_NOT_NULL(described);
    kafka_Map_t *values = kafka_admin_DescribeConfigsResult_values(described);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_either_outcome(kafka_admin_DescribeConfigsResult_all(described));
    kafka_admin_DescribeConfigsResult_destroy(described);

    kafka_admin_AlterConfigsResult_t *altered = kafka_admin_Admin_incremental_alter_configs(admin, empty);
    TEST_ASSERT_NOT_NULL(altered);
    values = kafka_admin_AlterConfigsResult_values(altered);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_AlterConfigsResult_all(altered));
    kafka_admin_AlterConfigsResult_destroy(altered);

    kafka_admin_DescribeLogDirsResult_t *log_dirs = kafka_admin_Admin_describe_log_dirs(admin, none);
    TEST_ASSERT_NOT_NULL(log_dirs);
    values = kafka_admin_DescribeLogDirsResult_descriptions(log_dirs);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_either_outcome(kafka_admin_DescribeLogDirsResult_all_descriptions(log_dirs));
    kafka_admin_DescribeLogDirsResult_destroy(log_dirs);

    kafka_admin_AlterReplicaLogDirsResult_t *moved = kafka_admin_Admin_alter_replica_log_dirs(admin, empty);
    TEST_ASSERT_NOT_NULL(moved);
    values = kafka_admin_AlterReplicaLogDirsResult_values(moved);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_AlterReplicaLogDirsResult_all(moved));
    kafka_admin_AlterReplicaLogDirsResult_destroy(moved);

    kafka_admin_DescribeReplicaLogDirsResult_t *replicas = kafka_admin_Admin_describe_replica_log_dirs(admin, none);
    TEST_ASSERT_NOT_NULL(replicas);
    values = kafka_admin_DescribeReplicaLogDirsResult_values(replicas);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_either_outcome(kafka_admin_DescribeReplicaLogDirsResult_all(replicas));
    kafka_admin_DescribeReplicaLogDirsResult_destroy(replicas);

    kafka_Map_destroy(empty);
    kafka_List_destroy(none);
    close_and_destroy(admin);
}

// ---------------------------------------------------------------------------
// Reassignments, elections and offsets
// ---------------------------------------------------------------------------

/* Empty reassignment / listOffsets batches resolve with no network round
 * trip; `listPartitionReassignments()` (all partitions) does need the
 * controller, so it must *time out* rather than hang, which is the property
 * this suite exists to check.
 *
 * This is the one test in the file that asserts a specific *failure* rather
 * than accepting either outcome, so it is the one that needs the bootstrap to
 * be genuinely dead -- hence NO_BROKER_BOOTSTRAP rather than the shared
 * `create_admin()`. Against `localhost:9092` it fails on any machine that
 * happens to have a broker there (a stray container from another project is
 * enough), because the controller answers and the call succeeds. */
static void test_kafka_admin_reassignment_empty_batches_need_no_broker(void) {
    kafka_admin_Admin_t *admin = create_admin_no_broker();
    kafka_Map_t *empty = kafka_Map_new();

    kafka_admin_AlterPartitionReassignmentsResult_t *altered = kafka_admin_Admin_alter_partition_reassignments(admin, empty);
    TEST_ASSERT_NOT_NULL(altered);
    kafka_Map_t *values = kafka_admin_AlterPartitionReassignmentsResult_values(altered);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_AlterPartitionReassignmentsResult_all(altered));
    kafka_admin_AlterPartitionReassignmentsResult_destroy(altered);

    kafka_admin_ListOffsetsResult_t *offsets = kafka_admin_Admin_list_offsets(admin, empty);
    TEST_ASSERT_NOT_NULL(offsets);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListOffsetsResult_all(offsets);
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(all, &value);
    if (err != NULL) {
        TEST_FAIL_MESSAGE(kafka_common_Error_message(err));
    }
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size((const kafka_Map_t *)value));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListOffsetsResult_destroy(offsets);

    /* Needs the controller, so this one really does go to the network and comes
     * back with an error inside the explicit timeout. */
    kafka_admin_ListPartitionReassignmentsOptions_t *options = kafka_admin_ListPartitionReassignmentsOptions_new();
    kafka_admin_ListPartitionReassignmentsOptions_set_timeout_ms(options, RPC_TIMEOUT_MS);
    kafka_admin_ListPartitionReassignmentsResult_t *listed = kafka_admin_Admin_list_partition_reassignments_with_options(admin, options);
    kafka_admin_ListPartitionReassignmentsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(listed);
    err = await_failure(kafka_admin_ListPartitionReassignmentsResult_reassignments(listed));
    TEST_ASSERT_NOT_NULL(kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_admin_ListPartitionReassignmentsResult_destroy(listed);

    kafka_Map_destroy(empty);
    close_and_destroy(admin);
}

/* Argument validation happens in the constructors, before anything can be
 * enqueued, so these return immediately with no broker: the error names the
 * offending value rather than being a timeout. */
static void test_kafka_admin_reassignment_constructors_reject_bad_arguments(void) {
    const kafka_common_ElectionType_t *type = NULL;
    kafka_common_Error_t *err = kafka_common_ElectionType_value_of(3, &type);
    TEST_ASSERT_NULL(type);
    assert_error_message(err, "Value 3 must be one of [PREFERRED, UNCLEAN]");

    kafka_List_t *no_replicas = kafka_List_new();
    kafka_admin_NewPartitionReassignment_t *reassignment = NULL;
    err = kafka_admin_NewPartitionReassignment_new(no_replicas, &reassignment);
    TEST_ASSERT_NULL(reassignment);
    assert_error_message(err, "Cannot create a new partition reassignment without any replicas");
    kafka_List_destroy(no_replicas);

    const kafka_common_IsolationLevel_t *level = NULL;
    err = kafka_common_IsolationLevel_for_id(5, &level);
    TEST_ASSERT_NULL(level);
    assert_error_message(err, "Unknown isolation level 5");
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

/* Group-side argument validation also lives in the constructors. */
static void test_kafka_admin_group_constructors_reject_bad_arguments(void) {
    /* A negative offset: Java's OffsetAndMetadata constructor throws. */
    kafka_consumer_OffsetAndMetadata_t *om = NULL;
    kafka_common_Error_t *err = kafka_consumer_OffsetAndMetadata_new(-2, &om);
    TEST_ASSERT_NULL(om);
    assert_error_message(err, "Invalid negative offset");

    /* An empty member list without `remove_all`: Java's Collection constructor
     * throws rather than treating it as "remove everything". */
    kafka_List_t *no_members = kafka_List_new();
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t *options = NULL;
    err = kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(no_members, &options);
    TEST_ASSERT_NULL(options);
    assert_error_message(err, "Invalid empty members has been provided");
    kafka_List_destroy(no_members);
}

/* With no keys requested there is nothing to send, so these return promptly
 * against an unreachable broker: `describeConsumerGroups`,
 * `describeClassicGroups`, `deleteConsumerGroups` and
 * `listConsumerGroupOffsets` with no group all resolve to an empty result
 * rather than waiting for a coordinator. */
static void test_kafka_admin_group_empty_batches_need_no_broker(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_List_t *none = kafka_List_new();
    kafka_Map_t *empty = kafka_Map_new();

    kafka_admin_DescribeConsumerGroupsResult_t *described = kafka_admin_Admin_describe_consumer_groups(admin, none);
    TEST_ASSERT_NOT_NULL(described);
    kafka_Map_t *values = kafka_admin_DescribeConsumerGroupsResult_described_groups(described);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_either_outcome(kafka_admin_DescribeConsumerGroupsResult_all(described));
    kafka_admin_DescribeConsumerGroupsResult_destroy(described);

    kafka_admin_DescribeClassicGroupsResult_t *classic = kafka_admin_Admin_describe_classic_groups(admin, none);
    TEST_ASSERT_NOT_NULL(classic);
    values = kafka_admin_DescribeClassicGroupsResult_described_groups(classic);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_either_outcome(kafka_admin_DescribeClassicGroupsResult_all(classic));
    kafka_admin_DescribeClassicGroupsResult_destroy(classic);

    kafka_admin_DeleteConsumerGroupsResult_t *deleted = kafka_admin_Admin_delete_consumer_groups(admin, none);
    TEST_ASSERT_NOT_NULL(deleted);
    values = kafka_admin_DeleteConsumerGroupsResult_deleted_groups(deleted);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_DeleteConsumerGroupsResult_all(deleted));
    kafka_admin_DeleteConsumerGroupsResult_destroy(deleted);

    kafka_admin_ListConsumerGroupOffsetsResult_t *listed = kafka_admin_Admin_list_consumer_group_offsets_with_group_specs(admin, empty);
    TEST_ASSERT_NOT_NULL(listed);
    await_either_outcome(kafka_admin_ListConsumerGroupOffsetsResult_all(listed));
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(listed);

    kafka_Map_destroy(empty);
    kafka_List_destroy(none);
    close_and_destroy(admin);
}

// ---------------------------------------------------------------------------
// Features and SCRAM credentials
// ---------------------------------------------------------------------------

/* `updateFeatures` is the one admin RPC whose *client-side* validation can
 * fail before the request is enqueued (KafkaAdminClient.updateFeatures throws
 * IllegalArgumentException for an empty map), so that arm is only reachable
 * through a production handle -- Java's MockAdminClient does not check. The
 * error comes back in the call's own error slot, with no result. */
static void test_kafka_admin_update_features_rejects_an_empty_map(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_Map_t *empty = kafka_Map_new();
    kafka_admin_UpdateFeaturesResult_t *updated = NULL;
    kafka_common_Error_t *err = kafka_admin_Admin_update_features(admin, empty, &updated);
    TEST_ASSERT_NULL(updated);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "Feature updates can not be null or empty.");
    kafka_admin_UpdateFeaturesOptions_t *options = kafka_admin_UpdateFeaturesOptions_new();
    err = kafka_admin_Admin_update_features_with_options(admin, empty, options, &updated);
    kafka_admin_UpdateFeaturesOptions_destroy(options);
    TEST_ASSERT_NULL(updated);
    assert_error_message(err, "Feature updates can not be null or empty.");
    kafka_Map_destroy(empty);
    close_and_destroy(admin);
}

/* An empty SCRAM batch resolves without a broker, as every other empty batch
 * does. The delegation-token and feature RPCs always talk to the broker --
 * there is no empty request for `createDelegationToken` or
 * `describeFeatures` -- so they are not listed here. */
static void test_kafka_admin_scram_empty_batch_needs_no_broker(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_List_t *none = kafka_List_new();
    kafka_admin_AlterUserScramCredentialsResult_t *altered = kafka_admin_Admin_alter_user_scram_credentials(admin, none);
    TEST_ASSERT_NOT_NULL(altered);
    kafka_Map_t *values = kafka_admin_AlterUserScramCredentialsResult_values(altered);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_AlterUserScramCredentialsResult_all(altered));
    kafka_admin_AlterUserScramCredentialsResult_destroy(altered);
    kafka_List_destroy(none);
    close_and_destroy(admin);
}

/* An upsertion with an empty password is NOT a whole-call rejection.
 * KafkaAdminClient records `UnacceptableCredentialException("Password must not
 * be empty")` against that user only (KafkaAdminClient.java:4414-4416) and
 * still builds and sends every other user's alteration. What this asserts is
 * exactly that: the call is accepted and yields a per-user future for BOTH
 * users, and alice's fails. Which error bob's carries depends on whether
 * anything is listening on localhost:9092 (a timeout without a broker, the
 * broker's answer with one), so only its presence is asserted -- but a
 * whole-call rejection would fail this outright. */
static void test_kafka_admin_scram_empty_password_is_a_per_user_error(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_admin_ScramCredentialInfo_t *info = kafka_admin_ScramCredentialInfo_new(kafka_admin_ScramMechanism_scram_sha256(), 4096);
    kafka_admin_UserScramCredentialUpsertion_t *alice = kafka_admin_UserScramCredentialUpsertion_with_str("alice", info, "");
    kafka_admin_UserScramCredentialDeletion_t *bob = kafka_admin_UserScramCredentialDeletion_new("bob", kafka_admin_ScramMechanism_scram_sha512());
    kafka_admin_UserScramCredentialAlteration_t *alice_alt = kafka_admin_UserScramCredentialAlteration_upsertion(alice);
    kafka_admin_UserScramCredentialAlteration_t *bob_alt = kafka_admin_UserScramCredentialAlteration_deletion(bob);
    kafka_List_t *alterations = kafka_List_new();
    kafka_List_add(alterations, alice_alt);
    kafka_List_add(alterations, bob_alt);
    kafka_admin_AlterUserScramCredentialsOptions_t *options = kafka_admin_AlterUserScramCredentialsOptions_new();
    kafka_admin_AlterUserScramCredentialsOptions_set_timeout_ms(options, RPC_TIMEOUT_MS);
    kafka_admin_AlterUserScramCredentialsResult_t *altered = kafka_admin_Admin_alter_user_scram_credentials_with_options(admin, alterations, options);
    kafka_admin_AlterUserScramCredentialsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(altered);
    kafka_Map_t *values = kafka_admin_AlterUserScramCredentialsResult_values(altered);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    TEST_ASSERT_EQUAL_STRING("alice", (const char *)kafka_Map_key(values, 0));
    TEST_ASSERT_EQUAL_STRING("bob", (const char *)kafka_Map_key(values, 1));
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get((const kafka_common_KafkaFuture_t *)kafka_Map_value(values, 0), &value);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NOT_NULL(kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    err = kafka_common_KafkaFuture_get((const kafka_common_KafkaFuture_t *)kafka_Map_value(values, 1), &value);
    if (err != NULL) {
        kafka_common_Error_destroy(err);
    }
    kafka_Map_destroy(values);
    kafka_admin_AlterUserScramCredentialsResult_destroy(altered);
    kafka_List_destroy(alterations);
    kafka_admin_UserScramCredentialAlteration_destroy(alice_alt);
    kafka_admin_UserScramCredentialAlteration_destroy(bob_alt);
    kafka_admin_UserScramCredentialDeletion_destroy(bob);
    kafka_admin_UserScramCredentialUpsertion_destroy(alice);
    kafka_admin_ScramCredentialInfo_destroy(info);
    close_and_destroy(admin);
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/* With no keys requested there is nothing to send, so these return promptly
 * against an unreachable broker. `listTransactions` is not here: it always has
 * to discover the broker list, so it has no empty request. */
static void test_kafka_admin_transaction_empty_batches_need_no_broker(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_List_t *none = kafka_List_new();

    kafka_admin_DescribeProducersResult_t *producers = kafka_admin_Admin_describe_producers(admin, none);
    TEST_ASSERT_NOT_NULL(producers);
    await_either_outcome(kafka_admin_DescribeProducersResult_all(producers));
    kafka_admin_DescribeProducersResult_destroy(producers);

    kafka_admin_DescribeTransactionsResult_t *transactions = kafka_admin_Admin_describe_transactions(admin, none);
    TEST_ASSERT_NOT_NULL(transactions);
    await_either_outcome(kafka_admin_DescribeTransactionsResult_all(transactions));
    kafka_admin_DescribeTransactionsResult_destroy(transactions);

    kafka_admin_FenceProducersResult_t *fenced = kafka_admin_Admin_fence_producers(admin, none);
    TEST_ASSERT_NOT_NULL(fenced);
    kafka_Map_t *values = kafka_admin_FenceProducersResult_fenced_producers(fenced);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    await_void_ok(kafka_admin_FenceProducersResult_all(fenced));
    kafka_admin_FenceProducersResult_destroy(fenced);

    kafka_List_destroy(none);
    close_and_destroy(admin);
}

/* `listTransactions` against an unreachable broker: it must return rather than
 * hang, and either outcome (a timeout error, or a real listing if something is
 * listening on localhost:9092) is accepted and freed. */
static void test_kafka_admin_list_transactions_returns_without_hanging(void) {
    kafka_admin_Admin_t *admin = create_admin();
    kafka_admin_ListTransactionsOptions_t *options = kafka_admin_ListTransactionsOptions_new();
    kafka_admin_ListTransactionsOptions_set_timeout_ms(options, RPC_TIMEOUT_MS);
    kafka_admin_ListTransactionsResult_t *result = kafka_admin_Admin_list_transactions_with_options(admin, options);
    kafka_admin_ListTransactionsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    await_either_outcome(kafka_admin_ListTransactionsResult_all(result));
    kafka_admin_ListTransactionsResult_destroy(result);
    close_and_destroy(admin);
}

/* `abortTransaction` on a dead bootstrap: its one future must come back inside
 * the timeout rather than hang. */
static void test_kafka_admin_abort_transaction_returns_without_hanging(void) {
    kafka_admin_Admin_t *admin = create_admin_no_broker();
    kafka_common_TopicPartition_t *tp = tp_new("txn-topic", 0);
    kafka_admin_AbortTransactionSpec_t *spec = kafka_admin_AbortTransactionSpec_new(tp, 1, 1, 1);
    kafka_admin_AbortTransactionResult_t *result = kafka_admin_Admin_abort_transaction(admin, spec);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_Error_t *err = await_failure(kafka_admin_AbortTransactionResult_all(result));
    TEST_ASSERT_NOT_NULL(kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_admin_AbortTransactionResult_destroy(result);
    kafka_admin_AbortTransactionSpec_destroy(spec);
    kafka_common_TopicPartition_destroy(tp);
    close_and_destroy(admin);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_kafka_admin_create_succeeds);
    RUN_TEST(test_kafka_admin_create_with_defaults);
    RUN_TEST(test_kafka_admin_close_then_destroy);
    RUN_TEST(test_kafka_admin_close_idempotent);
    RUN_TEST(test_kafka_admin_close_async);
    RUN_TEST(test_kafka_admin_destroy_runs_pending_close_callback);
    RUN_TEST(test_kafka_admin_list_topics_returns_without_hanging);
    RUN_TEST(test_kafka_admin_list_topics_async_fires_once);
    RUN_TEST(test_kafka_admin_uuid_from_string_rejects_bad_id);
    RUN_TEST(test_kafka_admin_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_describe_cluster_returns_without_hanging);
    RUN_TEST(test_kafka_admin_describe_cluster_async_fires_once);
    RUN_TEST(test_kafka_admin_list_config_resources_returns_without_hanging);
    RUN_TEST(test_kafka_admin_alter_config_op_type_for_id_rejects_unknown_code);
    RUN_TEST(test_kafka_admin_config_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_reassignment_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_reassignment_constructors_reject_bad_arguments);
    RUN_TEST(test_kafka_admin_group_constructors_reject_bad_arguments);
    RUN_TEST(test_kafka_admin_group_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_update_features_rejects_an_empty_map);
    RUN_TEST(test_kafka_admin_scram_empty_batch_needs_no_broker);
    RUN_TEST(test_kafka_admin_scram_empty_password_is_a_per_user_error);
    RUN_TEST(test_kafka_admin_transaction_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_list_transactions_returns_without_hanging);
    RUN_TEST(test_kafka_admin_abort_transaction_returns_without_hanging);
    return UNITY_END();
}
