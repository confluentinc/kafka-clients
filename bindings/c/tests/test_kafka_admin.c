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

// ---------------------------------------------------------------------------
// B2 — cluster, configs and log dirs on the production client
// ---------------------------------------------------------------------------

/* `ConfigResource.Type.id()` / `AlterConfigOp.OpType.id()` codes. */
#define RESOURCE_TYPE_BROKER (4)
#define OP_TYPE_SET (0)

/* describeCluster resolves four futures rather than a per-key map, so with no
 * broker the whole call fails; with one it succeeds. Either is accepted — what
 * matters is that it returns promptly and honours the ownership contract. */
static void test_kafka_admin_describe_cluster_returns_without_hanging(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_DescribeClusterResult_t *result = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_describe_cluster(
        admin, RPC_TIMEOUT_MS, false, false, &result);
    if (err != NULL) {
        TEST_ASSERT_NULL(result);
        TEST_ASSERT_NOT_NULL(kafka_common_KafkaError_message(err));
        kafka_common_KafkaError_destroy(err);
    } else {
        TEST_ASSERT_NOT_NULL(result);
        TEST_ASSERT_NOT_NULL(kafka_admin_DescribeClusterResult_cluster_id(result));
        kafka_admin_DescribeClusterResult_destroy(result);
    }

    kafka_admin_AdminClient_close(admin, RPC_TIMEOUT_MS);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
} cluster_result_t;

static void on_describe_cluster(kafka_admin_DescribeClusterResult_t *result,
                                kafka_common_KafkaError_t *error, void *user_data) {
    cluster_result_t *r = (cluster_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        kafka_admin_DescribeClusterResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

/* The async twin: exactly one callback, with exactly one of result / error. */
static void test_kafka_admin_describe_cluster_async_fires_once(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    cluster_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_cluster_async(admin, RPC_TIMEOUT_MS, true, false,
                                                   on_describe_cluster, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, r.had_result + r.had_error);

    kafka_admin_AdminClient_close(admin, RPC_TIMEOUT_MS);
    kafka_admin_AdminClient_destroy(admin);
}

/* listConfigResources has a single future in Java, so it behaves like
 * listTopics: one outcome, promptly. */
static void test_kafka_admin_list_config_resources_returns_without_hanging(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_ListConfigResourcesResult_t *result = NULL;
    kafka_common_KafkaError_t *err =
        kafka_admin_AdminClient_list_config_resources(admin, NULL, 0, RPC_TIMEOUT_MS, &result);
    if (err != NULL) {
        TEST_ASSERT_NULL(result);
        kafka_common_KafkaError_destroy(err);
    } else {
        TEST_ASSERT_NOT_NULL(result);
        kafka_admin_ListConfigResourcesResult_destroy(result);
    }

    kafka_admin_AdminClient_close(admin, RPC_TIMEOUT_MS);
    kafka_admin_AdminClient_destroy(admin);
}

/* Argument marshaling runs before the RPC is submitted, so an unknown
 * AlterConfigOp op-type code fails on the production client with no network
 * involved — the same path an unparseable topic id takes. */
static void test_kafka_admin_incremental_alter_configs_rejects_bad_op_type(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    const int32_t types[1] = {RESOURCE_TYPE_BROKER};
    const char *names[1] = {"0"};
    const char *keys[1] = {"some.config"};
    const char *values[1] = {"1"};
    const int32_t ops[1] = {123};

    kafka_admin_AlterConfigsResult_t *result = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_incremental_alter_configs(
        admin, types, names, keys, values, ops, 1, RPC_TIMEOUT_MS, false, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* Empty B2 batches never reach the network either: every per-key future is
 * absent, so the flattened result is empty and the call succeeds. */
static void test_kafka_admin_b2_empty_batches_need_no_broker(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_DescribeConfigsResult_t *described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_configs(admin, NULL, NULL, 0,
                                                              RPC_TIMEOUT_MS, false, false,
                                                              &described));
    TEST_ASSERT_NOT_NULL(described);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeConfigsResult_count(described));
    kafka_admin_DescribeConfigsResult_destroy(described);

    kafka_admin_AlterConfigsResult_t *altered = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_incremental_alter_configs(
        admin, NULL, NULL, NULL, NULL, NULL, 0, RPC_TIMEOUT_MS, false, &altered));
    TEST_ASSERT_NOT_NULL(altered);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterConfigsResult_count(altered));
    kafka_admin_AlterConfigsResult_destroy(altered);

    kafka_admin_DescribeLogDirsResult_t *log_dirs = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_log_dirs(admin, NULL, 0, RPC_TIMEOUT_MS, &log_dirs));
    TEST_ASSERT_NOT_NULL(log_dirs);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeLogDirsResult_count(log_dirs));
    kafka_admin_DescribeLogDirsResult_destroy(log_dirs);

    kafka_admin_AlterReplicaLogDirsResult_t *moved = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_replica_log_dirs(
        admin, NULL, NULL, NULL, NULL, 0, RPC_TIMEOUT_MS, &moved));
    TEST_ASSERT_NOT_NULL(moved);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterReplicaLogDirsResult_count(moved));
    kafka_admin_AlterReplicaLogDirsResult_destroy(moved);

    kafka_admin_DescribeReplicaLogDirsResult_t *replicas = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_replica_log_dirs(
        admin, NULL, NULL, NULL, 0, RPC_TIMEOUT_MS, &replicas));
    TEST_ASSERT_NOT_NULL(replicas);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeReplicaLogDirsResult_count(replicas));
    kafka_admin_DescribeReplicaLogDirsResult_destroy(replicas);

    kafka_admin_AdminClient_destroy(admin);
}

/* B3, on the production client with no broker reachable. Empty batches resolve
 * with no network round trip; `listPartitionReassignments` with
 * `all_partitions = true` does need the controller, so it must *time out*
 * rather than hang, which is the property this suite exists to check. */
static void test_kafka_admin_b3_empty_batches_need_no_broker(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_AlterPartitionReassignmentsResult_t *altered = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_partition_reassignments(
        admin, NULL, NULL, NULL, NULL, NULL, 0, RPC_TIMEOUT_MS, true, &altered));
    TEST_ASSERT_NOT_NULL(altered);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterPartitionReassignmentsResult_count(altered));
    kafka_admin_AlterPartitionReassignmentsResult_destroy(altered);

    kafka_admin_ListOffsetsResult_t *offsets = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_offsets(admin, NULL, NULL, NULL, NULL, 0,
                                                          RPC_TIMEOUT_MS, 0, &offsets));
    TEST_ASSERT_NOT_NULL(offsets);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListOffsetsResult_count(offsets));
    kafka_admin_ListOffsetsResult_destroy(offsets);

    /* Needs the controller, so this one really does go to the network and comes
     * back with an error inside the explicit timeout. */
    kafka_admin_ListPartitionReassignmentsResult_t *listed = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_list_partition_reassignments(
        admin, true, NULL, NULL, 0, RPC_TIMEOUT_MS, &listed);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(listed);
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* Argument validation happens before anything is enqueued, so these three
 * return immediately even with no broker: the error names the offending value
 * rather than being a timeout. */
static void test_kafka_admin_b3_rejects_bad_arguments(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_ElectLeadersResult_t *elected = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_elect_leaders(
        admin, 3, true, NULL, NULL, 0, RPC_TIMEOUT_MS, &elected);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(elected);
    TEST_ASSERT_EQUAL_STRING("Value 3 must be one of [PREFERRED, UNCLEAN]",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    const char *topics[1] = {"t"};
    const int32_t partitions[1] = {0};
    const bool cancel[1] = {false};
    const int32_t replicas[1] = {0};
    const int32_t *replica_ptrs[1] = {replicas};
    const int32_t replica_counts[1] = {0};
    kafka_admin_AlterPartitionReassignmentsResult_t *altered = NULL;
    err = kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, RPC_TIMEOUT_MS, true,
        &altered);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(altered);
    TEST_ASSERT_EQUAL_STRING(
        "reassignment for t-0 at index 0: Cannot create a new partition reassignment without any "
        "replicas",
        kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    const bool is_timestamp[1] = {false};
    const int64_t specs[1] = {-1};
    kafka_admin_ListOffsetsResult_t *offsets = NULL;
    err = kafka_admin_AdminClient_list_offsets(admin, topics, partitions, is_timestamp, specs, 1,
                                               RPC_TIMEOUT_MS, 5, &offsets);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(offsets);
    TEST_ASSERT_EQUAL_STRING("Unknown isolation level 5", kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* Argument validation happens on the calling thread before anything is
 * enqueued, so these return immediately even with no broker: the error names
 * the offending value rather than being a timeout. */
static void test_kafka_admin_b4_rejects_bad_arguments(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    /* A NULL group id. */
    const char *topics[1] = {"t"};
    const int32_t partitions[1] = {0};
    const int64_t offsets[1] = {0};
    kafka_admin_AlterConsumerGroupOffsetsResult_t *altered = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, NULL, topics, partitions, offsets, NULL, NULL, NULL, 1, RPC_TIMEOUT_MS, &altered);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(altered);
    TEST_ASSERT_EQUAL_STRING("group_id must not be null", kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    /* A negative offset: Java's OffsetAndMetadata constructor throws. */
    const int64_t bad_offsets[1] = {-2};
    altered = NULL;
    err = kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, "g", topics, partitions, bad_offsets, NULL, NULL, NULL, 1, RPC_TIMEOUT_MS,
        &altered);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(altered);
    TEST_ASSERT_EQUAL_STRING("offset at index 0: Invalid negative offset",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    /* A duplicate group id in the two-level listConsumerGroupOffsets request. */
    const char *groups[2] = {"g", "g"};
    const bool all_partitions[2] = {true, true};
    const int32_t counts[2] = {0, 0};
    kafka_admin_ListConsumerGroupOffsetsResult_t *listed = NULL;
    err = kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, groups, all_partitions, NULL, NULL, counts, 2, RPC_TIMEOUT_MS, false, &listed);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(listed);
    TEST_ASSERT_EQUAL_STRING("group id `g` appears more than once at index 1",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    /* An empty member list without `remove_all`: Java's Collection constructor
     * throws rather than treating it as "remove everything". */
    kafka_admin_RemoveMembersFromConsumerGroupResult_t *removed = NULL;
    err = kafka_admin_AdminClient_remove_members_from_consumer_group(
        admin, "g", false, NULL, 0, NULL, RPC_TIMEOUT_MS, &removed);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(removed);
    TEST_ASSERT_EQUAL_STRING("Invalid empty members has been provided",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* With no keys requested there is nothing to send, so these return promptly
 * against an unreachable broker. `deleteConsumerGroups` with no group ids and
 * `describeConsumerGroups` with none both resolve to an empty result rather
 * than waiting for a coordinator. */
static void test_kafka_admin_b4_empty_batches_need_no_broker(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_DescribeConsumerGroupsResult_t *described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_consumer_groups(admin, NULL, 0,
                                                                      RPC_TIMEOUT_MS, false,
                                                                      &described));
    TEST_ASSERT_NOT_NULL(described);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeConsumerGroupsResult_count(described));
    kafka_admin_DescribeConsumerGroupsResult_destroy(described);

    kafka_admin_DescribeClassicGroupsResult_t *classic = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_classic_groups(admin, NULL, 0,
                                                                     RPC_TIMEOUT_MS, false,
                                                                     &classic));
    TEST_ASSERT_NOT_NULL(classic);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeClassicGroupsResult_count(classic));
    kafka_admin_DescribeClassicGroupsResult_destroy(classic);

    kafka_admin_DeleteConsumerGroupsResult_t *deleted = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_delete_consumer_groups(admin, NULL, 0, RPC_TIMEOUT_MS, &deleted));
    TEST_ASSERT_NOT_NULL(deleted);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteConsumerGroupsResult_count(deleted));
    kafka_admin_DeleteConsumerGroupsResult_destroy(deleted);

    kafka_admin_ListConsumerGroupOffsetsResult_t *listed = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, NULL, NULL, NULL, NULL, NULL, 0, RPC_TIMEOUT_MS, false, &listed));
    TEST_ASSERT_NOT_NULL(listed);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListConsumerGroupOffsetsResult_count(listed));
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(listed);

    kafka_admin_AdminClient_destroy(admin);
}

/* B5b argument validation, all of it before any network I/O. `updateFeatures`
 * is the one admin RPC whose *client-side* validation can fail before the
 * request is enqueued (KafkaAdminClient.updateFeatures throws
 * IllegalArgumentException for an empty map), so that arm is only reachable
 * through a production handle -- Java's MockAdminClient does not check. */
static void test_kafka_admin_b5b_rejects_bad_arguments(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    /* A NULL renewer name: Java's KafkaPrincipal constructor rejects it. */
    const char *types[1] = {"User"};
    const char *names[1] = {NULL};
    kafka_admin_CreateDelegationTokenResult_t *created = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_create_delegation_token(
        admin, types, names, 1, NULL, NULL, -1, RPC_TIMEOUT_MS, &created);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(created);
    TEST_ASSERT_EQUAL_STRING("renewer principal name at index 0 must not be null",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    /* An upsertion with no password is not a legal credential. */
    const char *users[1] = {"alice"};
    const bool upsertion[1] = {false};
    const int32_t mechanisms[1] = {1};
    kafka_admin_AlterUserScramCredentialsResult_t *altered = NULL;
    err = kafka_admin_AdminClient_alter_user_scram_credentials(
        admin, users, upsertion, mechanisms, NULL, NULL, NULL, NULL, NULL, 1, RPC_TIMEOUT_MS,
        &altered);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(altered);
    TEST_ASSERT_EQUAL_STRING("scram alteration at index 0 is an upsertion with no password",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    /* An empty update map: the production client throws where the mock does
     * not, so this arm has no coverage in test_mock_admin.c. */
    kafka_admin_UpdateFeaturesResult_t *updated = NULL;
    err = kafka_admin_AdminClient_update_features(admin, NULL, NULL, NULL, 0, RPC_TIMEOUT_MS, false,
                                                  &updated);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(updated);
    TEST_ASSERT_EQUAL_STRING("Feature updates can not be null or empty.",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    /* A repeated feature name: Java takes a Map, where the second update would
     * silently replace the first. */
    const char *duplicated[2] = {"metadata.version", "metadata.version"};
    const int16_t levels[2] = {17, 18};
    const int32_t upgrade[2] = {1, 1};
    updated = NULL;
    err = kafka_admin_AdminClient_update_features(admin, duplicated, levels, upgrade, 2,
                                                  RPC_TIMEOUT_MS, false, &updated);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(updated);
    TEST_ASSERT_EQUAL_STRING("feature update at index 1 repeats feature `metadata.version`",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* An empty SCRAM batch resolves without a broker, as every other empty batch
 * does. The remaining six B5b RPCs always talk to the broker -- there is no
 * empty request for `createDelegationToken` or `describeFeatures` -- so they
 * are not listed here. */
static void test_kafka_admin_b5b_empty_batches_need_no_broker(void) {
    kafka_admin_AdminClient_t *admin = create_admin();

    kafka_admin_AlterUserScramCredentialsResult_t *altered = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_user_scram_credentials(
        admin, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, RPC_TIMEOUT_MS, &altered));
    TEST_ASSERT_NOT_NULL(altered);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterUserScramCredentialsResult_count(altered));
    kafka_admin_AlterUserScramCredentialsResult_destroy(altered);

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
    RUN_TEST(test_kafka_admin_describe_cluster_returns_without_hanging);
    RUN_TEST(test_kafka_admin_describe_cluster_async_fires_once);
    RUN_TEST(test_kafka_admin_list_config_resources_returns_without_hanging);
    RUN_TEST(test_kafka_admin_incremental_alter_configs_rejects_bad_op_type);
    RUN_TEST(test_kafka_admin_b2_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_b3_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_b3_rejects_bad_arguments);
    RUN_TEST(test_kafka_admin_b4_rejects_bad_arguments);
    RUN_TEST(test_kafka_admin_b4_empty_batches_need_no_broker);
    RUN_TEST(test_kafka_admin_b5b_rejects_bad_arguments);
    RUN_TEST(test_kafka_admin_b5b_empty_batches_need_no_broker);
    return UNITY_END();
}
