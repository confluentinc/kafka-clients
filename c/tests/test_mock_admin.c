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

// C binding tests for the admin client, against `MockAdminClient`.
//
// The Admin C API mirrors the Rust `Admin` trait, which mirrors Java's
// `Admin` interface (CLAUDE.md §4 and admin-client.md §1): every RPC is a
// synchronous call returning an owned `*Result_t` whose accessors hand out
// `kafka_common_KafkaFuture_t` handles — one per key for the batch RPCs
// (`_values()` maps), plus `_all()` and Java's `thenApply` refinements. The
// mock resolves every future before the RPC returns, so `get` never blocks
// here; the lifetime rules are what these tests exercise:
//
//   - a value delivered by `kafka_common_KafkaFuture_get` is borrowed from
//     the future and dies with `kafka_common_KafkaFuture_destroy`;
//   - a `_values()` map owns its keys and futures: `kafka_Map_destroy` frees
//     them all;
//   - a future returned as `const kafka_common_KafkaFuture_t *` is borrowed
//     from the result handle and is never destroyed by the caller;
//   - options handles are borrowed by the RPC and destroyed after the call;
//   - lists and maps built in C hold borrowed elements: the elements are
//     freed by the test, after the container, or before it — the container
//     does not touch them.
//
// `_cb` completions (`kafka_common_KafkaFuture_get_cb`, `Admin_close_cb`)
// are queued on the client's callback vector and run by
// `kafka_admin_Admin_execute_callbacks` on the calling thread; the
// `callback_pump_t` of test_support.h drives that, installed on the mock's
// `Admin` view by `admin_callback_pump_install` below.

#include <confluent_kafka.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "test_support.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// ---------------------------------------------------------------------------
// Fixture and the admin callback pump
// ---------------------------------------------------------------------------

/* test_support.h ships the producer and consumer pump installers; the admin
 * one lives here, binding the pump to the mock's `Admin` view. */
static int32_t callback_pump_execute_admin(const void *client) {
    return kafka_admin_Admin_execute_callbacks((const kafka_admin_Admin_t *)client);
}

static void admin_callback_pump_install(callback_pump_t *pump, const kafka_admin_Admin_t *admin) {
    callback_pump_init(pump, admin, callback_pump_execute_admin);
    kafka_admin_Admin_set_callbacks_notify(admin, callback_pump_notify, pump);
}

typedef struct {
    kafka_admin_MockAdminClient_t *mock;
    const kafka_admin_Admin_t *admin; /* the `__as_Admin` view, never destroyed */
    callback_pump_t pump;
} fixture_t;

/* `MockAdminClient.create(numBrokers)`: brokers `localhost:1000 + id`,
 * controller 0, default replication factor `min(numBrokers, 3)`. */
static void fixture_init(fixture_t *f, int32_t num_brokers) {
    f->mock = NULL;
    kafka_common_Error_t *err = kafka_admin_MockAdminClient_create(num_brokers, &f->mock);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(f->mock);
    f->admin = kafka_admin_MockAdminClient__as_Admin(f->mock);
    TEST_ASSERT_NOT_NULL(f->admin);
    admin_callback_pump_install(&f->pump, f->admin);
}

static void fixture_destroy(fixture_t *f) {
    kafka_admin_MockAdminClient_destroy(f->mock);
    callback_pump_destroy(&f->pump);
}

// ---------------------------------------------------------------------------
// Error and future helpers
// ---------------------------------------------------------------------------

/* Asserts `err` carries exactly `expected`, then frees it. */
static void assert_error_message(kafka_common_Error_t *err, const char *expected) {
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING(expected, kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
}

/* `future.get()` on a future expected to succeed: the value, borrowed from
 * the future (`NULL` for a `Void` future). The future is left alive. */
static void *get_ok(const kafka_common_KafkaFuture_t *future) {
    TEST_ASSERT_NOT_NULL(future);
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(future, &value);
    if (err != NULL) {
        char message[512];
        snprintf(message, sizeof message, "future failed: %s", kafka_common_Error_message(err));
        kafka_common_Error_destroy(err);
        TEST_FAIL_MESSAGE(message);
    }
    return value;
}

/* `future.get()` on a future expected to fail: the owned error. */
static kafka_common_Error_t *get_err(const kafka_common_KafkaFuture_t *future) {
    TEST_ASSERT_NOT_NULL(future);
    void *value = NULL;
    kafka_common_Error_t *err = kafka_common_KafkaFuture_get(future, &value);
    TEST_ASSERT_NOT_NULL(err);
    return err;
}

/* An owned `KafkaFuture<Void>` that must succeed; destroyed here. */
static void expect_void_ok(kafka_common_KafkaFuture_t *future) {
    TEST_ASSERT_NULL(get_ok(future));
    kafka_common_KafkaFuture_destroy(future);
}

/* An owned future that must fail with exactly `message`; destroyed here. */
static void expect_fails_with(kafka_common_KafkaFuture_t *future, const char *message) {
    assert_error_message(get_err(future), message);
    kafka_common_KafkaFuture_destroy(future);
}

/* An owned future that must fail with error code `code`; destroyed here. */
static void expect_fails_with_code(kafka_common_KafkaFuture_t *future, kafka_common_ErrorCode_e code) {
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_EQUAL_INT32((int32_t)code, (int32_t)kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(future);
}

/* The per-key future of a `_values()`-style map, looked up the way Java's
 * `values().get(key)` is: `kafka_Map_get` compares Rust-built keys by value
 * (string content, `TopicPartition` fields, ...), so a C-built key finds it.
 * The future stays owned by the map. */
static kafka_common_KafkaFuture_t *future_for(const kafka_Map_t *values, const void *key) {
    TEST_ASSERT_NOT_NULL(values);
    kafka_common_KafkaFuture_t *future = (kafka_common_KafkaFuture_t *)kafka_Map_get(values, (void *)key);
    TEST_ASSERT_NOT_NULL(future);
    return future;
}

/* The completion record of a `kafka_common_KafkaFuture_get_cb`: the callee
 * owns `error`, so only its message is kept. */
typedef struct {
    atomic_int fired;
    void *value;
    int had_error;
    char message[256];
} cb_result_t;

static void on_future_done(void *value, kafka_common_Error_t *error, void *opaque) {
    cb_result_t *r = (cb_result_t *)opaque;
    r->value = value;
    if (error != NULL) {
        r->had_error = 1;
        snprintf(r->message, sizeof r->message, "%s", kafka_common_Error_message(error));
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

/* `get_cb` on `future`, pumped to completion on this thread: the completion
 * must fire exactly once, through the client's callback vector. */
static void get_cb_pumped(fixture_t *f, const kafka_common_KafkaFuture_t *future, cb_result_t *r) {
    memset(r, 0, sizeof(*r));
    int notified_before = atomic_load(&f->pump.notified);
    kafka_common_KafkaFuture_get_cb(future, on_future_done, r);
    TEST_ASSERT_TRUE(callback_pump_until(&f->pump, &r->fired, 1));
    TEST_ASSERT_TRUE(atomic_load(&f->pump.notified) > notified_before);
    /* Nothing is left to run, and the completion is not delivered twice. */
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f->pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r->fired));
}

// ---------------------------------------------------------------------------
// Input builders (C-built containers borrow their elements)
// ---------------------------------------------------------------------------

/* A list of string literals, NULL-terminated. */
static kafka_List_t *string_list(const char *first, ...) {
    kafka_List_t *list = kafka_List_new();
    va_list args;
    va_start(args, first);
    for (const char *s = first; s != NULL; s = va_arg(args, const char *)) {
        kafka_List_add(list, (void *)s);
    }
    va_end(args);
    return list;
}

static kafka_common_TopicPartition_t *tp_new(const char *topic, int32_t partition) {
    kafka_common_TopicPartition_t *tp = kafka_common_TopicPartition_new(topic, partition);
    TEST_ASSERT_NOT_NULL(tp);
    return tp;
}

static kafka_common_config_ConfigResource_t *topic_resource(const char *name) {
    kafka_common_config_ConfigResource_t *r =
        kafka_common_config_ConfigResource_new(kafka_common_config_ConfigResource_Type_topic(), name);
    TEST_ASSERT_NOT_NULL(r);
    return r;
}

/* `createTopics(singleton(new NewTopic(name, partitions, rf)))`, asserting
 * the per-topic future succeeded. */
static void create_one(fixture_t *f, const char *name, int32_t partitions, int16_t rf) {
    kafka_admin_NewTopic_t *topic = kafka_admin_NewTopic_with_num_partitions_replication_factor(name, partitions, rf);
    TEST_ASSERT_NOT_NULL(topic);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, topic);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics(f->admin, topics);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    TEST_ASSERT_NULL(get_ok(future_for(values, name)));
    kafka_Map_destroy(values);
    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(topic);
}

/* `incrementalAlterConfigs(singletonMap(resource, singleton(new AlterConfigOp(
 * new ConfigEntry(key, value), op))))`, asserting the resource's future
 * succeeded. */
static void alter_one_config(fixture_t *f, const kafka_common_config_ConfigResource_Type_t *type,
                             const char *resource_name, const char *key, const char *value,
                             const kafka_admin_AlterConfigOp_OpType_t *op_type) {
    kafka_common_config_ConfigResource_t *resource = kafka_common_config_ConfigResource_new(type, resource_name);
    kafka_admin_ConfigEntry_t *entry = kafka_admin_ConfigEntry_new(key, value);
    kafka_admin_AlterConfigOp_t *op = kafka_admin_AlterConfigOp_new(entry, op_type);
    kafka_List_t *ops = kafka_List_new();
    kafka_List_add(ops, op);
    kafka_Map_t *configs = kafka_Map_new();
    kafka_Map_put(configs, resource, ops);
    kafka_admin_AlterConfigsResult_t *result = kafka_admin_Admin_incremental_alter_configs(f->admin, configs);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterConfigsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    TEST_ASSERT_NULL(get_ok(future_for(values, resource)));
    kafka_Map_destroy(values);
    kafka_admin_AlterConfigsResult_destroy(result);
    kafka_Map_destroy(configs);
    kafka_List_destroy(ops);
    kafka_admin_AlterConfigOp_destroy(op);
    kafka_admin_ConfigEntry_destroy(entry);
    kafka_common_config_ConfigResource_destroy(resource);
}

/* The mock lists a group once it has a GROUP config (`groupConfigs`), so
 * seeding one is how a test creates a group. */
static void seed_group(fixture_t *f, const char *group_id) {
    alter_one_config(f, kafka_common_config_ConfigResource_Type_group(), group_id,
                     "consumer.session.timeout.ms", "45000", kafka_admin_AlterConfigOp_OpType_set());
}

/* `alterPartitionReassignments(singletonMap(tp, Optional.of(new
 * NewPartitionReassignment(asList(1, 2)))))`, asserting success. */
static void reassign_one(fixture_t *f, const char *topic, int32_t partition) {
    int32_t b1 = 1, b2 = 2;
    kafka_List_t *replicas = kafka_List_new();
    kafka_List_add(replicas, &b1);
    kafka_List_add(replicas, &b2);
    kafka_admin_NewPartitionReassignment_t *reassignment = NULL;
    TEST_ASSERT_NULL(kafka_admin_NewPartitionReassignment_new(replicas, &reassignment));
    kafka_common_TopicPartition_t *tp = tp_new(topic, partition);
    kafka_Map_t *reassignments = kafka_Map_new();
    kafka_Map_put(reassignments, tp, reassignment);
    kafka_admin_AlterPartitionReassignmentsResult_t *result =
        kafka_admin_Admin_alter_partition_reassignments(f->admin, reassignments);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterPartitionReassignmentsResult_values(result);
    TEST_ASSERT_NULL(get_ok(future_for(values, tp)));
    kafka_Map_destroy(values);
    kafka_admin_AlterPartitionReassignmentsResult_destroy(result);
    kafka_Map_destroy(reassignments);
    kafka_common_TopicPartition_destroy(tp);
    kafka_admin_NewPartitionReassignment_destroy(reassignment);
    kafka_List_destroy(replicas);
}

/* Builds the `Map<TopicPartition, Long>` the mock's `update*Offsets` take
 * from parallel arrays and hands it to `update`; the map borrows the pairs,
 * which are freed here. */
static void update_offsets(fixture_t *f,
                           void (*update)(const kafka_admin_MockAdminClient_t *, const kafka_Map_t *),
                           const char *topic, const int32_t *partitions, int64_t *offsets, int32_t count) {
    kafka_common_TopicPartition_t *tps[8];
    TEST_ASSERT_TRUE(count <= 8);
    kafka_Map_t *map = kafka_Map_new();
    for (int32_t i = 0; i < count; i++) {
        tps[i] = tp_new(topic, partitions[i]);
        kafka_Map_put(map, tps[i], &offsets[i]);
    }
    update(f->mock, map);
    kafka_Map_destroy(map);
    for (int32_t i = 0; i < count; i++) {
        kafka_common_TopicPartition_destroy(tps[i]);
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/* `MockAdminClient.create(1)`, the `Admin` view, `close()` in both forms, and
 * `destroy`. `close()` is `void` in Java and never reports anything. */
static void test_mock_admin_create_close_destroy(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_Admin_close(f.admin);
    /* Idempotent, and a negative timeout is clamped to 0 rather than rejected
     * (Java's Duration cannot be negative). */
    kafka_admin_Admin_close_with_timeout(f.admin, 1000);
    kafka_admin_Admin_close_with_timeout(f.admin, -1);
    fixture_destroy(&f);
}

/* Java's builder refuses a cluster without brokers with an
 * IllegalArgumentException; the C constructor returns it and leaves the slot
 * untouched. */
static void test_mock_admin_rejects_zero_brokers(void) {
    kafka_admin_MockAdminClient_t *mock = NULL;
    kafka_common_Error_t *err = kafka_admin_MockAdminClient_create(0, &mock);
    TEST_ASSERT_NULL(mock);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "num_brokers must be at least 1, was 0");
    err = kafka_admin_MockAdminClient_create(-1, &mock);
    TEST_ASSERT_NULL(mock);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "num_brokers must be at least 1, was -1");
}

typedef struct {
    atomic_int fired;
} close_result_t;

static void on_closed(void *opaque) {
    atomic_fetch_add(&((close_result_t *)opaque)->fired, 1);
}

/* `close_cb` queues its completion on the client's callback vector: the
 * notify hook fires once, `execute_callbacks` runs it on this thread. */
static void test_mock_admin_close_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    close_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_admin_Admin_close_cb(f.admin, on_closed, &result);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &result.fired, 1));
    TEST_ASSERT_TRUE(atomic_load(&f.pump.notified) >= 1);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    /* The timed form, after the client is already closed, still completes. */
    close_result_t timed;
    memset(&timed, 0, sizeof(timed));
    kafka_admin_Admin_close_with_timeout_cb(f.admin, 1000, on_closed, &timed);
    TEST_ASSERT_TRUE(callback_pump_until(&f.pump, &timed.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&timed.fired));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired)); /* not fired again */
    fixture_destroy(&f);
}

/* `_destroy` runs the callbacks still pending, so a `close_cb` nobody pumped
 * fires exactly once, during the destroy. */
static void test_mock_admin_destroy_runs_pending_close_callback(void) {
    fixture_t f;
    fixture_init(&f, 1);
    close_result_t result;
    memset(&result, 0, sizeof(result));
    kafka_admin_Admin_close_cb(f.admin, on_closed, &result);
    /* Wait for the completion to be queued, without running it. */
    TEST_ASSERT_TRUE(callback_pump_wait_notify(&f.pump));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&result.fired));
    kafka_admin_MockAdminClient_destroy(f.mock);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&result.fired));
    callback_pump_destroy(&f.pump);
}

/* `new AdminClientConfig(Map<String, String>)` from a C-built map of string
 * pairs; the map stays the caller's. */
static void test_admin_client_config_lifecycle(void) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"localhost:9092");
    kafka_Map_put(props, (void *)"client.id", (void *)"c-mock-admin-test");
    kafka_admin_AdminClientConfig_t *config = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClientConfig_new(props, &config));
    TEST_ASSERT_NOT_NULL(config);
    kafka_admin_AdminClientConfig_destroy(config);
    kafka_admin_AdminClientConfig_destroy(NULL);
    kafka_Map_destroy(props);
}

/* Without `bootstrap.servers` no client can be built: the failure surfaces
 * either when the configuration is parsed or when the client is created. */
static void test_admin_client_create_rejects_empty_bootstrap(void) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_admin_AdminClientConfig_t *config = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClientConfig_new(props, &config);
    if (err == NULL) {
        TEST_ASSERT_NOT_NULL(config);
        kafka_admin_Admin_t *admin = NULL;
        err = kafka_admin_AdminClient_create(config, &admin);
        TEST_ASSERT_NULL(admin);
        kafka_admin_AdminClientConfig_destroy(config);
    } else {
        TEST_ASSERT_NULL(config);
    }
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NOT_NULL(kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(props);
}

// ---------------------------------------------------------------------------
// createTopics
// ---------------------------------------------------------------------------

/* The sync path: one topic, `values()` keyed by name, `all()`, and the four
 * `thenApply` refinements of `CreateTopicsResult`. */
static void test_mock_admin_create_topics_sync(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_NewTopic_t *topic = kafka_admin_NewTopic_with_num_partitions_replication_factor("sync-topic", 2, 1);
    TEST_ASSERT_EQUAL_STRING("sync-topic", kafka_admin_NewTopic_name(topic));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_NewTopic_num_partitions(topic));
    TEST_ASSERT_EQUAL_INT16(1, kafka_admin_NewTopic_replication_factor(topic));
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, topic);
    kafka_admin_CreateTopicsOptions_t *options = kafka_admin_CreateTopicsOptions_new();
    kafka_admin_CreateTopicsOptions_set_timeout_ms(options, 5000);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics_with_options(f.admin, topics, options);
    kafka_admin_CreateTopicsOptions_destroy(options); /* borrowed by the call only */
    TEST_ASSERT_NOT_NULL(result);

    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    TEST_ASSERT_EQUAL_STRING("sync-topic", (const char *)kafka_Map_key(values, 0));
    kafka_common_KafkaFuture_t *created = future_for(values, "sync-topic");
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(created));
    TEST_ASSERT_NULL(get_ok(created)); /* KafkaFuture<Void> */
    TEST_ASSERT_NULL(kafka_Map_get(values, (void *)"other"));
    kafka_Map_destroy(values);

    expect_void_ok(kafka_admin_CreateTopicsResult_all(result));

    kafka_common_KafkaFuture_t *partitions = kafka_admin_CreateTopicsResult_num_partitions(result, "sync-topic");
    TEST_ASSERT_EQUAL_INT32(2, *(const int32_t *)get_ok(partitions));
    kafka_common_KafkaFuture_destroy(partitions);
    kafka_common_KafkaFuture_t *rf = kafka_admin_CreateTopicsResult_replication_factor(result, "sync-topic");
    TEST_ASSERT_EQUAL_INT32(1, *(const int32_t *)get_ok(rf));
    kafka_common_KafkaFuture_destroy(rf);
    kafka_common_KafkaFuture_t *id = kafka_admin_CreateTopicsResult_topic_id(result, "sync-topic");
    char *id_string = kafka_common_Uuid_to_string((const kafka_common_Uuid_t *)get_ok(id));
    TEST_ASSERT_TRUE(strlen(id_string) > 0);
    kafka_string_destroy(id_string);
    kafka_common_KafkaFuture_destroy(id);
    kafka_common_KafkaFuture_t *config = kafka_admin_CreateTopicsResult_config(result, "sync-topic");
    const kafka_admin_Config_t *cfg = (const kafka_admin_Config_t *)get_ok(config);
    TEST_ASSERT_NOT_NULL(cfg);
    /* No configs were set on the NewTopic, so the reported config is empty. */
    kafka_List_t *entries = kafka_admin_Config_entries(cfg);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(entries));
    kafka_List_destroy(entries);
    kafka_common_KafkaFuture_destroy(config);

    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_admin_CreateTopicsResult_destroy(NULL);
    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(topic);
    fixture_destroy(&f);
}

/* One topic already exists: its future fails with TopicExistsException while
 * the other succeeds, and `all()` fails. A per-key failure is never a call
 * failure. */
static void test_mock_admin_create_topics_partial_failure(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "existing", 1, 1);
    kafka_admin_NewTopic_t *existing = kafka_admin_NewTopic_with_num_partitions_replication_factor("existing", 1, 1);
    kafka_admin_NewTopic_t *fresh = kafka_admin_NewTopic_with_num_partitions_replication_factor("fresh", 1, 1);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, existing);
    kafka_List_add(topics, fresh);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics(f.admin, topics);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    kafka_common_Error_t *err = get_err(future_for(values, "existing"));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_TOPIC_ALREADY_EXISTS, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_topic_exists_error(err));
    assert_error_message(err, "Topic existing exists already.");
    TEST_ASSERT_NULL(get_ok(future_for(values, "fresh")));
    kafka_Map_destroy(values);
    expect_fails_with_code(kafka_admin_CreateTopicsResult_all(result), kafka_common_ErrorCode_e_TOPIC_ALREADY_EXISTS);
    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(existing);
    kafka_admin_NewTopic_destroy(fresh);
    fixture_destroy(&f);
}

/* The regression test for per-key granularity (admin-client.md §5): the
 * futures of one batch resolve independently of one another, `all()`
 * aggregates them, and a refinement for a topic not in the request is a
 * failed future rather than a NULL handle. */
static void test_mock_admin_per_key_futures_resolve_independently(void) {
    fixture_t f;
    fixture_init(&f, 3);
    create_one(&f, "pk-existing", 1, 1);
    kafka_admin_NewTopic_t *existing = kafka_admin_NewTopic_with_num_partitions_replication_factor("pk-existing", 1, 1);
    kafka_admin_NewTopic_t *fresh = kafka_admin_NewTopic_with_num_partitions_replication_factor("pk-fresh", 4, 2);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, existing);
    kafka_List_add(topics, fresh);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics(f.admin, topics);
    TEST_ASSERT_NOT_NULL(result);

    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    kafka_common_KafkaFuture_t *existing_future = future_for(values, "pk-existing");
    kafka_common_KafkaFuture_t *fresh_future = future_for(values, "pk-fresh");
    TEST_ASSERT_TRUE(existing_future != fresh_future);
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(existing_future));
    TEST_ASSERT_TRUE(kafka_common_KafkaFuture_is_done(fresh_future));
    /* The existing topic fails ... */
    kafka_common_Error_t *err = get_err(existing_future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_topic_exists_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_error(err));
    kafka_common_Error_destroy(err);
    /* ... the other succeeds, and `get` is re-callable. */
    TEST_ASSERT_NULL(get_ok(fresh_future));
    TEST_ASSERT_NULL(get_ok(fresh_future));
    err = get_err(existing_future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_topic_exists_error(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(values);

    /* `all()` fails because one key failed. */
    kafka_common_KafkaFuture_t *all = kafka_admin_CreateTopicsResult_all(result);
    err = get_err(all);
    TEST_ASSERT_TRUE(kafka_common_Error_is_topic_exists_error(err));
    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(all);

    /* The refinements of the successful topic. */
    kafka_common_KafkaFuture_t *partitions = kafka_admin_CreateTopicsResult_num_partitions(result, "pk-fresh");
    const int32_t *count = (const int32_t *)get_ok(partitions);
    TEST_ASSERT_NOT_NULL(count);
    TEST_ASSERT_EQUAL_INT32(4, *count);
    kafka_common_KafkaFuture_destroy(partitions);
    kafka_common_KafkaFuture_t *rf = kafka_admin_CreateTopicsResult_replication_factor(result, "pk-fresh");
    TEST_ASSERT_EQUAL_INT32(2, *(const int32_t *)get_ok(rf));
    kafka_common_KafkaFuture_destroy(rf);
    kafka_common_KafkaFuture_t *id = kafka_admin_CreateTopicsResult_topic_id(result, "pk-fresh");
    const kafka_common_Uuid_t *uuid = (const kafka_common_Uuid_t *)get_ok(id);
    TEST_ASSERT_NOT_NULL(uuid);
    char *id_string = kafka_common_Uuid_to_string(uuid);
    TEST_ASSERT_EQUAL_INT(22, (int)strlen(id_string));
    kafka_string_destroy(id_string);
    kafka_common_KafkaFuture_destroy(id);
    kafka_common_KafkaFuture_t *config = kafka_admin_CreateTopicsResult_config(result, "pk-fresh");
    TEST_ASSERT_NOT_NULL(get_ok(config));
    kafka_common_KafkaFuture_destroy(config);
    /* The refinements of the failed topic fail the same way. */
    kafka_common_KafkaFuture_t *failed = kafka_admin_CreateTopicsResult_num_partitions(result, "pk-existing");
    err = get_err(failed);
    TEST_ASSERT_TRUE(kafka_common_Error_is_topic_exists_error(err));
    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(failed);
    /* A topic that was not part of the request: Java's IllegalArgumentException,
     * delivered through a failed future, not a NULL handle. */
    kafka_common_KafkaFuture_t *unknown = kafka_admin_CreateTopicsResult_num_partitions(result, "pk-never-requested");
    TEST_ASSERT_NOT_NULL(unknown);
    err = get_err(unknown);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(unknown);
    unknown = kafka_admin_CreateTopicsResult_topic_id(result, "pk-never-requested");
    TEST_ASSERT_NOT_NULL(unknown);
    err = get_err(unknown);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(unknown);

    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(existing);
    kafka_admin_NewTopic_destroy(fresh);
    fixture_destroy(&f);
}

/* `-1` for the partition count and replication factor asks the broker for
 * its defaults: the mock answers 1 partition and `min(numBrokers, 3)`
 * replicas. A replication factor above the broker count is an
 * InvalidReplicationFactorException. */
static void test_mock_admin_create_topics_broker_defaults(void) {
    fixture_t f;
    fixture_init(&f, 3);
    kafka_admin_NewTopic_t *defaults = kafka_admin_NewTopic_with_num_partitions_replication_factor("defaults", -1, -1);
    kafka_admin_NewTopic_t *too_many = kafka_admin_NewTopic_with_num_partitions_replication_factor("too-many", 1, 9);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, defaults);
    kafka_List_add(topics, too_many);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics(f.admin, topics);
    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(result);
    TEST_ASSERT_NULL(get_ok(future_for(values, "defaults")));
    kafka_common_Error_t *err = get_err(future_for(values, "too-many"));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_INVALID_REPLICATION_FACTOR, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_invalid_replication_factor_error(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(values);
    kafka_common_KafkaFuture_t *partitions = kafka_admin_CreateTopicsResult_num_partitions(result, "defaults");
    TEST_ASSERT_EQUAL_INT32(1, *(const int32_t *)get_ok(partitions));
    kafka_common_KafkaFuture_destroy(partitions);
    kafka_common_KafkaFuture_t *rf = kafka_admin_CreateTopicsResult_replication_factor(result, "defaults");
    TEST_ASSERT_EQUAL_INT32(3, *(const int32_t *)get_ok(rf));
    kafka_common_KafkaFuture_destroy(rf);
    expect_fails_with_code(kafka_admin_CreateTopicsResult_replication_factor(result, "too-many"),
                           kafka_common_ErrorCode_e_INVALID_REPLICATION_FACTOR);
    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(defaults);
    kafka_admin_NewTopic_destroy(too_many);
    fixture_destroy(&f);
}

/* `new NewTopic(name, Map<Integer, List<Integer>> replicasAssignments)`. The
 * mock ignores the explicit assignment and applies its defaults (1 partition,
 * 3 replicas on 3 brokers), which `describeTopics` then reports. */
static void test_mock_admin_create_topics_replicas_assignment(void) {
    fixture_t f;
    fixture_init(&f, 3);
    int32_t p0 = 0, p1 = 1;
    int32_t b0 = 0, b1 = 1, b2 = 2;
    kafka_List_t *replicas0 = kafka_List_new();
    kafka_List_add(replicas0, &b0);
    kafka_List_add(replicas0, &b1);
    kafka_List_t *replicas1 = kafka_List_new();
    kafka_List_add(replicas1, &b1);
    kafka_List_add(replicas1, &b2);
    kafka_Map_t *assignments = kafka_Map_new();
    kafka_Map_put(assignments, &p0, replicas0);
    kafka_Map_put(assignments, &p1, replicas1);
    kafka_admin_NewTopic_t *topic = kafka_admin_NewTopic_with_replicas_assignments("assigned", assignments);
    TEST_ASSERT_NOT_NULL(topic);
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_NewTopic_num_partitions(topic));
    TEST_ASSERT_EQUAL_INT16(-1, kafka_admin_NewTopic_replication_factor(topic));
    /* The getter is an owned copy: partition ids in ascending order, by value. */
    kafka_Map_t *copy = kafka_admin_NewTopic_replicas_assignments(topic);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(copy));
    TEST_ASSERT_EQUAL_INT32(0, *(const int32_t *)kafka_Map_key(copy, 0));
    const kafka_List_t *copy1 = (const kafka_List_t *)kafka_Map_get(copy, &p1);
    TEST_ASSERT_NOT_NULL(copy1);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(copy1));
    TEST_ASSERT_EQUAL_INT32(2, *(const int32_t *)kafka_List_get(copy1, 1));
    kafka_Map_destroy(copy);

    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, topic);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics(f.admin, topics);
    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(result);
    TEST_ASSERT_NULL(get_ok(future_for(values, "assigned")));
    kafka_Map_destroy(values);
    kafka_common_KafkaFuture_t *partitions = kafka_admin_CreateTopicsResult_num_partitions(result, "assigned");
    TEST_ASSERT_EQUAL_INT32(1, *(const int32_t *)get_ok(partitions));
    kafka_common_KafkaFuture_destroy(partitions);
    kafka_common_KafkaFuture_t *rf = kafka_admin_CreateTopicsResult_replication_factor(result, "assigned");
    TEST_ASSERT_EQUAL_INT32(3, *(const int32_t *)get_ok(rf));
    kafka_common_KafkaFuture_destroy(rf);
    kafka_admin_CreateTopicsResult_destroy(result);

    /* describeTopics agrees with the metadata the create call reported. */
    kafka_List_t *names = string_list("assigned", NULL);
    kafka_admin_DescribeTopicsResult_t *described = kafka_admin_Admin_describe_topics_with_topic_names(f.admin, names);
    kafka_Map_t *descriptions = kafka_admin_DescribeTopicsResult_topic_name_values(described);
    const kafka_admin_TopicDescription_t *d =
        (const kafka_admin_TopicDescription_t *)get_ok(future_for(descriptions, "assigned"));
    kafka_List_t *infos = kafka_admin_TopicDescription_partitions(d);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(infos));
    kafka_List_t *replicas =
        kafka_common_TopicPartitionInfo_replicas((const kafka_common_TopicPartitionInfo_t *)kafka_List_get(infos, 0));
    TEST_ASSERT_EQUAL_INT32(3, kafka_List_size(replicas));
    kafka_List_destroy(replicas);
    kafka_List_destroy(infos);
    kafka_Map_destroy(descriptions);
    kafka_admin_DescribeTopicsResult_destroy(described);
    kafka_List_destroy(names);

    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(topic);
    kafka_Map_destroy(assignments);
    kafka_List_destroy(replicas0);
    kafka_List_destroy(replicas1);
    fixture_destroy(&f);
}

/* `NewTopic.configs(Map)` and the getters: Java returns null until configs
 * are set, and the mock reports the configs back from `createTopics`. */
static void test_mock_admin_new_topic_configs(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_NewTopic_t *topic = kafka_admin_NewTopic_with_num_partitions_replication_factor("configured", 1, 1);
    TEST_ASSERT_NULL(kafka_admin_NewTopic_configs(topic));
    TEST_ASSERT_NULL(kafka_admin_NewTopic_replicas_assignments(topic));
    kafka_Map_t *configs = kafka_Map_new();
    kafka_Map_put(configs, (void *)"retention.ms", (void *)"60000");
    kafka_Map_put(configs, (void *)"cleanup.policy", (void *)"compact");
    kafka_admin_NewTopic_set_configs(topic, configs);
    kafka_Map_destroy(configs); /* copied by the setter */
    kafka_Map_t *copy = kafka_admin_NewTopic_configs(topic);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(copy));
    TEST_ASSERT_EQUAL_STRING("cleanup.policy", (const char *)kafka_Map_key(copy, 0)); /* ascending */
    TEST_ASSERT_EQUAL_STRING("60000", (const char *)kafka_Map_get(copy, (void *)"retention.ms"));
    kafka_Map_destroy(copy);
    char *text = kafka_admin_NewTopic_to_string(topic);
    TEST_ASSERT_NOT_NULL(strstr(text, "configured"));
    kafka_string_destroy(text);

    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, topic);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics(f.admin, topics);
    expect_void_ok(kafka_admin_CreateTopicsResult_all(result));
    kafka_common_KafkaFuture_t *config = kafka_admin_CreateTopicsResult_config(result, "configured");
    const kafka_admin_Config_t *cfg = (const kafka_admin_Config_t *)get_ok(config);
    const kafka_admin_ConfigEntry_t *entry = kafka_admin_Config_get(cfg, "retention.ms");
    TEST_ASSERT_NOT_NULL(entry);
    TEST_ASSERT_EQUAL_STRING("60000", kafka_admin_ConfigEntry_value(entry));
    TEST_ASSERT_NULL(kafka_admin_Config_get(cfg, "no.such.config"));
    kafka_common_KafkaFuture_destroy(config);
    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(topic);
    fixture_destroy(&f);
}

/* The `_cb` path over per-key futures: both completions are queued on the
 * mock's callback vector and run by the pump, each exactly once, with the
 * same outcome the blocking `get` reports. */
static void test_mock_admin_create_topics_async_partial_failure(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "existing", 1, 1);
    kafka_admin_NewTopic_t *existing = kafka_admin_NewTopic_with_num_partitions_replication_factor("existing", 1, 1);
    kafka_admin_NewTopic_t *fresh = kafka_admin_NewTopic_with_num_partitions_replication_factor("fresh", 1, 1);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, existing);
    kafka_List_add(topics, fresh);
    kafka_admin_CreateTopicsResult_t *result = kafka_admin_Admin_create_topics(f.admin, topics);
    kafka_Map_t *values = kafka_admin_CreateTopicsResult_values(result);
    cb_result_t failed;
    get_cb_pumped(&f, future_for(values, "existing"), &failed);
    TEST_ASSERT_TRUE(failed.had_error);
    TEST_ASSERT_NULL(failed.value);
    TEST_ASSERT_EQUAL_STRING("Topic existing exists already.", failed.message);
    cb_result_t ok;
    get_cb_pumped(&f, future_for(values, "fresh"), &ok);
    TEST_ASSERT_FALSE(ok.had_error);
    TEST_ASSERT_NULL(ok.value);
    /* The refinement's value is borrowed from the future, so it is still
     * readable here. */
    kafka_common_KafkaFuture_t *partitions = kafka_admin_CreateTopicsResult_num_partitions(result, "fresh");
    cb_result_t refined;
    get_cb_pumped(&f, partitions, &refined);
    TEST_ASSERT_FALSE(refined.had_error);
    TEST_ASSERT_EQUAL_INT32(1, *(const int32_t *)refined.value);
    kafka_common_KafkaFuture_destroy(partitions);
    kafka_Map_destroy(values);
    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_List_destroy(topics);
    kafka_admin_NewTopic_destroy(existing);
    kafka_admin_NewTopic_destroy(fresh);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// listTopics
// ---------------------------------------------------------------------------

/* `names()`, `listings()` and `namesToListings()`, all three refinements of
 * one `ListTopicsResult`. */
static void test_mock_admin_list_topics_sync(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "lt-b", 1, 1);
    create_one(&f, "lt-a", 2, 1);
    kafka_admin_ListTopicsOptions_t *options = kafka_admin_ListTopicsOptions_new();
    kafka_admin_ListTopicsOptions_set_list_internal(options, 1);
    TEST_ASSERT_TRUE(kafka_admin_ListTopicsOptions_should_list_internal(options));
    kafka_admin_ListTopicsResult_t *result = kafka_admin_Admin_list_topics_with_options(f.admin, options);
    kafka_admin_ListTopicsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);

    kafka_common_KafkaFuture_t *names = kafka_admin_ListTopicsResult_names(result);
    const kafka_List_t *name_list = (const kafka_List_t *)get_ok(names);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(name_list));
    TEST_ASSERT_EQUAL_STRING("lt-a", (const char *)kafka_List_get(name_list, 0)); /* sorted */
    TEST_ASSERT_EQUAL_STRING("lt-b", (const char *)kafka_List_get(name_list, 1));
    TEST_ASSERT_NULL(kafka_List_get(name_list, 2));
    kafka_common_KafkaFuture_destroy(names);

    kafka_common_KafkaFuture_t *listings = kafka_admin_ListTopicsResult_listings(result);
    const kafka_List_t *listing_list = (const kafka_List_t *)get_ok(listings);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(listing_list));
    const kafka_admin_TopicListing_t *first = (const kafka_admin_TopicListing_t *)kafka_List_get(listing_list, 0);
    TEST_ASSERT_EQUAL_STRING("lt-a", kafka_admin_TopicListing_name(first));
    TEST_ASSERT_FALSE(kafka_admin_TopicListing_is_internal(first));
    kafka_common_Uuid_t *id = kafka_admin_TopicListing_topic_id(first); /* owned copy */
    TEST_ASSERT_NOT_NULL(id);
    kafka_common_Uuid_destroy(id);
    kafka_common_KafkaFuture_destroy(listings);

    kafka_common_KafkaFuture_t *by_name = kafka_admin_ListTopicsResult_names_to_listings(result);
    const kafka_Map_t *map = (const kafka_Map_t *)get_ok(by_name);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(map));
    const kafka_admin_TopicListing_t *b = (const kafka_admin_TopicListing_t *)kafka_Map_get(map, (void *)"lt-b");
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_STRING("lt-b", kafka_admin_TopicListing_name(b));
    TEST_ASSERT_NULL(kafka_Map_get(map, (void *)"lt-c"));
    kafka_common_KafkaFuture_destroy(by_name);

    kafka_admin_ListTopicsResult_destroy(result);
    kafka_admin_ListTopicsResult_destroy(NULL);
    fixture_destroy(&f);
}

static void test_mock_admin_list_topics_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "lt-async", 1, 1);
    kafka_admin_ListTopicsResult_t *result = kafka_admin_Admin_list_topics(f.admin);
    kafka_common_KafkaFuture_t *names = kafka_admin_ListTopicsResult_names(result);
    cb_result_t r;
    get_cb_pumped(&f, names, &r);
    TEST_ASSERT_FALSE(r.had_error);
    const kafka_List_t *list = (const kafka_List_t *)r.value;
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(list));
    TEST_ASSERT_EQUAL_STRING("lt-async", (const char *)kafka_List_get(list, 0));
    kafka_common_KafkaFuture_destroy(names);
    kafka_admin_ListTopicsResult_destroy(result);
    fixture_destroy(&f);
}

/* `timeoutNextRequest(1)`: the next RPC's single future fails with a
 * TimeoutException, which is what `listTopics` has instead of per-key
 * futures. */
static void test_mock_admin_list_topics_call_error(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "lt-timeout", 1, 1);
    kafka_admin_MockAdminClient_timeout_next_request(f.mock, 1);
    kafka_admin_ListTopicsResult_t *result = kafka_admin_Admin_list_topics(f.admin);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *names = kafka_admin_ListTopicsResult_names(result);
    kafka_common_Error_t *err = get_err(names);
    TEST_ASSERT_TRUE(kafka_common_Error_is_timeout_error(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_retriable_error(err));
    assert_error_message(err, "The mock timed out the request.");
    kafka_common_KafkaFuture_destroy(names);
    kafka_admin_ListTopicsResult_destroy(result);
    /* Only the next request: the following one succeeds. */
    result = kafka_admin_Admin_list_topics(f.admin);
    names = kafka_admin_ListTopicsResult_names(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size((const kafka_List_t *)get_ok(names)));
    kafka_common_KafkaFuture_destroy(names);
    kafka_admin_ListTopicsResult_destroy(result);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// describeTopics
// ---------------------------------------------------------------------------

/* By name, with a partial failure: one description, one per-key error. The
 * description's partitions carry leader, replicas, ISR and the (empty, but
 * present) ELR sets the mock reports. */
static void test_mock_admin_describe_topics_by_names(void) {
    fixture_t f;
    fixture_init(&f, 3);
    create_one(&f, "described", 2, 2);
    kafka_List_t *names = string_list("described", "missing", NULL);
    kafka_admin_DescribeTopicsOptions_t *options = kafka_admin_DescribeTopicsOptions_new();
    kafka_admin_DescribeTopicsOptions_set_include_authorized_operations(options, 1);
    kafka_admin_DescribeTopicsResult_t *result =
        kafka_admin_Admin_describe_topics_with_topic_names_options(f.admin, names, options);
    kafka_admin_DescribeTopicsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT(kafka_admin_DescribeTopicsResult_e_by_topic_name, kafka_admin_DescribeTopicsResult__enum(result));
    TEST_ASSERT_NULL(kafka_admin_DescribeTopicsResult_topic_id_values(result));
    kafka_Map_t *values = kafka_admin_DescribeTopicsResult_topic_name_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));

    kafka_common_Error_t *err = get_err(future_for(values, "missing"));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_unknown_topic_or_partition_error(err));
    assert_error_message(err, "Topic missing not found.");

    const kafka_admin_TopicDescription_t *d =
        (const kafka_admin_TopicDescription_t *)get_ok(future_for(values, "described"));
    TEST_ASSERT_NOT_NULL(d);
    TEST_ASSERT_EQUAL_STRING("described", kafka_admin_TopicDescription_name(d));
    TEST_ASSERT_FALSE(kafka_admin_TopicDescription_is_internal(d));
    kafka_common_Uuid_t *id = kafka_admin_TopicDescription_topic_id(d);
    char *id_string = kafka_common_Uuid_to_string(id);
    TEST_ASSERT_TRUE(strlen(id_string) > 0);
    kafka_string_destroy(id_string);
    kafka_common_Uuid_destroy(id);
    /* The mock passes Collections.emptySet(): the operations were *reported*
     * and merely empty, which is a non-NULL list of size 0; Java's null would
     * be a NULL list. */
    kafka_List_t *operations = kafka_admin_TopicDescription_authorized_operations(d);
    TEST_ASSERT_NOT_NULL(operations);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(operations));
    kafka_List_destroy(operations);

    kafka_List_t *partitions = kafka_admin_TopicDescription_partitions(d);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(partitions));
    TEST_ASSERT_NULL(kafka_List_get(partitions, 2));
    const kafka_common_TopicPartitionInfo_t *p0 = (const kafka_common_TopicPartitionInfo_t *)kafka_List_get(partitions, 0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartitionInfo_partition(p0));
    /* The mock puts every partition's leader on broker 0 and its replicas on
     * the first `replicationFactor` brokers. */
    const kafka_common_Node_t *leader = kafka_common_TopicPartitionInfo_leader(p0);
    TEST_ASSERT_NOT_NULL(leader);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Node_id(leader));
    TEST_ASSERT_EQUAL_INT32(1000, kafka_common_Node_port(leader));
    TEST_ASSERT_EQUAL_STRING("localhost", kafka_common_Node_host(leader));
    kafka_List_t *replicas = kafka_common_TopicPartitionInfo_replicas(p0);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(replicas));
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_Node_id((const kafka_common_Node_t *)kafka_List_get(replicas, 1)));
    kafka_List_destroy(replicas);
    kafka_List_t *isr = kafka_common_TopicPartitionInfo_isr(p0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(isr));
    kafka_List_destroy(isr);
    /* The mock reports an empty (not absent) ELR set. */
    kafka_List_t *elr = kafka_common_TopicPartitionInfo_elr(p0);
    TEST_ASSERT_NOT_NULL(elr);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(elr));
    kafka_List_destroy(elr);
    kafka_List_t *last_known_elr = kafka_common_TopicPartitionInfo_last_known_elr(p0);
    TEST_ASSERT_NOT_NULL(last_known_elr);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(last_known_elr));
    kafka_List_destroy(last_known_elr);
    kafka_List_destroy(partitions);
    kafka_Map_destroy(values);

    /* `allTopicNames()` fails because one topic failed. */
    expect_fails_with_code(kafka_admin_DescribeTopicsResult_all_topic_names(result),
                           kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION);
    kafka_admin_DescribeTopicsResult_destroy(result);
    kafka_List_destroy(names);
    fixture_destroy(&f);
}

/* By id, through a `TopicCollection`: the map is keyed by `Uuid_t`, compared
 * by value, so the id obtained from `listTopics` finds its own entry. An
 * unknown id is an UnknownTopicIdException. */
static void test_mock_admin_describe_topics_by_ids(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "by-id", 1, 1);
    kafka_admin_ListTopicsResult_t *listed = kafka_admin_Admin_list_topics(f.admin);
    kafka_common_KafkaFuture_t *listings = kafka_admin_ListTopicsResult_listings(listed);
    const kafka_List_t *listing_list = (const kafka_List_t *)get_ok(listings);
    kafka_common_Uuid_t *id = kafka_admin_TopicListing_topic_id((const kafka_admin_TopicListing_t *)kafka_List_get(listing_list, 0));
    kafka_common_KafkaFuture_destroy(listings);
    kafka_admin_ListTopicsResult_destroy(listed);

    kafka_common_Uuid_t *unknown = kafka_common_Uuid_new(0x1234567890abcdefLL, 0x0fedcba098765432LL);
    kafka_List_t *ids = kafka_List_new();
    kafka_List_add(ids, id);
    kafka_List_add(ids, unknown);
    kafka_common_TopicCollection_t *collection = kafka_common_TopicCollection_of_topic_ids(ids);
    TEST_ASSERT_NOT_NULL(collection);
    kafka_admin_DescribeTopicsResult_t *result = kafka_admin_Admin_describe_topics_with_topics(f.admin, collection);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT(kafka_admin_DescribeTopicsResult_e_by_topic_id, kafka_admin_DescribeTopicsResult__enum(result));
    TEST_ASSERT_NULL(kafka_admin_DescribeTopicsResult_topic_name_values(result));
    kafka_Map_t *values = kafka_admin_DescribeTopicsResult_topic_id_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    const kafka_admin_TopicDescription_t *d = (const kafka_admin_TopicDescription_t *)get_ok(future_for(values, id));
    TEST_ASSERT_EQUAL_STRING("by-id", kafka_admin_TopicDescription_name(d));
    kafka_common_Error_t *err = get_err(future_for(values, unknown));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNKNOWN_TOPIC_ID, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_unknown_topic_id_error(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(values);
    expect_fails_with_code(kafka_admin_DescribeTopicsResult_all_topic_ids(result), kafka_common_ErrorCode_e_UNKNOWN_TOPIC_ID);
    kafka_admin_DescribeTopicsResult_destroy(result);
    kafka_common_TopicCollection_destroy(collection);
    kafka_List_destroy(ids);
    kafka_common_Uuid_destroy(id);
    kafka_common_Uuid_destroy(unknown);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_topics_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "dt-async", 1, 1);
    kafka_List_t *names = string_list("dt-async", NULL);
    kafka_admin_DescribeTopicsResult_t *result = kafka_admin_Admin_describe_topics_with_topic_names(f.admin, names);
    kafka_Map_t *values = kafka_admin_DescribeTopicsResult_topic_name_values(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(values, "dt-async"), &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("dt-async", kafka_admin_TopicDescription_name((const kafka_admin_TopicDescription_t *)r.value));
    kafka_Map_destroy(values);
    kafka_admin_DescribeTopicsResult_destroy(result);
    kafka_List_destroy(names);
    fixture_destroy(&f);
}

/* `Uuid.fromString` rejects a malformed id before any RPC is built. */
static void test_mock_admin_uuid_from_string_rejects_bad_id(void) {
    kafka_common_Uuid_t *id = NULL;
    kafka_common_Error_t *err = kafka_common_Uuid_from_string("not-a-uuid", &id);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(id);
    TEST_ASSERT_NOT_NULL(kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    /* A well-formed one round-trips. */
    TEST_ASSERT_NULL(kafka_common_Uuid_from_string("4A5xz_QZTB2CtL4wc0X0Jw", &id));
    TEST_ASSERT_NOT_NULL(id);
    char *text = kafka_common_Uuid_to_string(id);
    TEST_ASSERT_EQUAL_STRING("4A5xz_QZTB2CtL4wc0X0Jw", text);
    kafka_string_destroy(text);
    kafka_common_Uuid_destroy(id);
}

// ---------------------------------------------------------------------------
// deleteTopics
// ---------------------------------------------------------------------------

/* By name: a `DeleteTopicsResult` keyed by topic name, with an unknown topic
 * failing its own future only. The topic is gone from `listTopics` after. */
static void test_mock_admin_delete_topics_by_names(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "del-a", 1, 1);
    create_one(&f, "del-keep", 1, 1);
    kafka_List_t *names = string_list("del-a", "never-existed", NULL);
    kafka_common_TopicCollection_t *collection = kafka_common_TopicCollection_of_topic_names(names);
    kafka_admin_DeleteTopicsOptions_t *options = kafka_admin_DeleteTopicsOptions_new();
    kafka_admin_DeleteTopicsOptions_set_timeout_ms(options, 5000);
    kafka_admin_DeleteTopicsResult_t *result = kafka_admin_Admin_delete_topics_with_options(f.admin, collection, options);
    kafka_admin_DeleteTopicsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT(kafka_admin_DeleteTopicsResult_e_by_topic_name, kafka_admin_DeleteTopicsResult__enum(result));
    TEST_ASSERT_NULL(kafka_admin_DeleteTopicsResult_topic_id_values(result));
    kafka_Map_t *values = kafka_admin_DeleteTopicsResult_topic_name_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    TEST_ASSERT_EQUAL_STRING("del-a", (const char *)kafka_Map_key(values, 0));
    TEST_ASSERT_NULL(get_ok(future_for(values, "del-a")));
    kafka_common_Error_t *err = get_err(future_for(values, "never-existed"));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION, kafka_common_Error_code(err));
    assert_error_message(err, "Topic never-existed does not exist.");
    kafka_Map_destroy(values);
    expect_fails_with_code(kafka_admin_DeleteTopicsResult_all(result), kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION);
    kafka_admin_DeleteTopicsResult_destroy(result);
    kafka_admin_DeleteTopicsResult_destroy(NULL);
    kafka_common_TopicCollection_destroy(collection);
    kafka_List_destroy(names);

    kafka_admin_ListTopicsResult_t *listed = kafka_admin_Admin_list_topics(f.admin);
    kafka_common_KafkaFuture_t *listed_names = kafka_admin_ListTopicsResult_names(listed);
    const kafka_List_t *list = (const kafka_List_t *)get_ok(listed_names);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(list));
    TEST_ASSERT_EQUAL_STRING("del-keep", (const char *)kafka_List_get(list, 0));
    kafka_common_KafkaFuture_destroy(listed_names);
    kafka_admin_ListTopicsResult_destroy(listed);
    fixture_destroy(&f);
}

/* By id: keyed by `Uuid_t`, with `topicNameValues()` NULL. */
static void test_mock_admin_delete_topics_by_ids(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "del-id", 1, 1);
    kafka_admin_ListTopicsResult_t *listed = kafka_admin_Admin_list_topics(f.admin);
    kafka_common_KafkaFuture_t *listings = kafka_admin_ListTopicsResult_listings(listed);
    kafka_common_Uuid_t *id = kafka_admin_TopicListing_topic_id(
        (const kafka_admin_TopicListing_t *)kafka_List_get((const kafka_List_t *)get_ok(listings), 0));
    kafka_common_KafkaFuture_destroy(listings);
    kafka_admin_ListTopicsResult_destroy(listed);

    kafka_List_t *ids = kafka_List_new();
    kafka_List_add(ids, id);
    kafka_common_TopicCollection_t *collection = kafka_common_TopicCollection_of_topic_ids(ids);
    kafka_admin_DeleteTopicsResult_t *result = kafka_admin_Admin_delete_topics(f.admin, collection);
    TEST_ASSERT_EQUAL_INT(kafka_admin_DeleteTopicsResult_e_by_topic_id, kafka_admin_DeleteTopicsResult__enum(result));
    TEST_ASSERT_NULL(kafka_admin_DeleteTopicsResult_topic_name_values(result));
    kafka_Map_t *values = kafka_admin_DeleteTopicsResult_topic_id_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    TEST_ASSERT_NULL(get_ok(future_for(values, id)));
    kafka_Map_destroy(values);
    expect_void_ok(kafka_admin_DeleteTopicsResult_all(result));
    kafka_admin_DeleteTopicsResult_destroy(result);
    kafka_common_TopicCollection_destroy(collection);
    kafka_List_destroy(ids);
    kafka_common_Uuid_destroy(id);

    listed = kafka_admin_Admin_list_topics(f.admin);
    kafka_common_KafkaFuture_t *names = kafka_admin_ListTopicsResult_names(listed);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size((const kafka_List_t *)get_ok(names)));
    kafka_common_KafkaFuture_destroy(names);
    kafka_admin_ListTopicsResult_destroy(listed);
    fixture_destroy(&f);
}

static void test_mock_admin_delete_topics_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "del-async", 1, 1);
    kafka_List_t *names = string_list("del-async", "del-missing", NULL);
    kafka_common_TopicCollection_t *collection = kafka_common_TopicCollection_of_topic_names(names);
    kafka_admin_DeleteTopicsResult_t *result = kafka_admin_Admin_delete_topics(f.admin, collection);
    kafka_Map_t *values = kafka_admin_DeleteTopicsResult_topic_name_values(result);
    cb_result_t ok;
    get_cb_pumped(&f, future_for(values, "del-async"), &ok);
    TEST_ASSERT_FALSE(ok.had_error);
    cb_result_t failed;
    get_cb_pumped(&f, future_for(values, "del-missing"), &failed);
    TEST_ASSERT_TRUE(failed.had_error);
    TEST_ASSERT_EQUAL_STRING("Topic del-missing does not exist.", failed.message);
    kafka_Map_destroy(values);
    kafka_admin_DeleteTopicsResult_destroy(result);
    kafka_common_TopicCollection_destroy(collection);
    kafka_List_destroy(names);
    fixture_destroy(&f);
}

/* `all()` of a by-id delete through the `_cb` path. */
static void test_mock_admin_delete_topics_by_ids_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "del-id-async", 1, 1);
    kafka_admin_ListTopicsResult_t *listed = kafka_admin_Admin_list_topics(f.admin);
    kafka_common_KafkaFuture_t *listings = kafka_admin_ListTopicsResult_listings(listed);
    kafka_common_Uuid_t *id = kafka_admin_TopicListing_topic_id(
        (const kafka_admin_TopicListing_t *)kafka_List_get((const kafka_List_t *)get_ok(listings), 0));
    kafka_common_KafkaFuture_destroy(listings);
    kafka_admin_ListTopicsResult_destroy(listed);
    kafka_List_t *ids = kafka_List_new();
    kafka_List_add(ids, id);
    kafka_common_TopicCollection_t *collection = kafka_common_TopicCollection_of_topic_ids(ids);
    kafka_admin_DeleteTopicsResult_t *result = kafka_admin_Admin_delete_topics(f.admin, collection);
    kafka_common_KafkaFuture_t *all = kafka_admin_DeleteTopicsResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_NULL(r.value);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DeleteTopicsResult_destroy(result);
    kafka_common_TopicCollection_destroy(collection);
    kafka_List_destroy(ids);
    kafka_common_Uuid_destroy(id);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// createPartitions / deleteRecords
// ---------------------------------------------------------------------------

/* Java's `MockAdminClient.createPartitions` throws
 * UnsupportedOperationException("Not implemented yet"); the Rust mock fails
 * every per-topic future the same way, never the call. */
static void test_mock_admin_create_partitions_reports_unsupported_per_topic(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "grow", 1, 1);
    kafka_admin_NewPartitions_t *spec = kafka_admin_NewPartitions_increase_to(3);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_NewPartitions_total_count(spec));
    TEST_ASSERT_NULL(kafka_admin_NewPartitions_assignments(spec));
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, (void *)"grow", spec);
    kafka_admin_CreatePartitionsOptions_t *options = kafka_admin_CreatePartitionsOptions_new();
    kafka_admin_CreatePartitionsOptions_set_timeout_ms(options, 5000);
    kafka_admin_CreatePartitionsResult_t *result = kafka_admin_Admin_create_partitions_with_options(f.admin, request, options);
    kafka_admin_CreatePartitionsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_CreatePartitionsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    kafka_common_Error_t *err = get_err(future_for(values, "grow"));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_unsupported_version_error(err));
    assert_error_message(err, "Not implemented yet");
    kafka_Map_destroy(values);
    expect_fails_with(kafka_admin_CreatePartitionsResult_all(result), "Not implemented yet");
    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_admin_CreatePartitionsResult_destroy(NULL);
    kafka_Map_destroy(request);
    kafka_admin_NewPartitions_destroy(spec);
    kafka_admin_NewPartitions_destroy(NULL);
    fixture_destroy(&f);
}

/* `NewPartitions.increaseTo(n, List<List<Integer>>)` — including Java's legal
 * `increaseTo(n, emptyList())` — and a result keyed by every requested
 * topic, sorted by name. */
static void test_mock_admin_create_partitions_with_assignments_and_sorting(void) {
    fixture_t f;
    fixture_init(&f, 3);
    int32_t b0 = 0, b1 = 1, b2 = 2;
    kafka_List_t *brokers0 = kafka_List_new();
    kafka_List_add(brokers0, &b0);
    kafka_List_add(brokers0, &b1);
    kafka_List_t *brokers1 = kafka_List_new();
    kafka_List_add(brokers1, &b1);
    kafka_List_add(brokers1, &b2);
    kafka_List_t *assignments = kafka_List_new();
    kafka_List_add(assignments, brokers0);
    kafka_List_add(assignments, brokers1);
    kafka_List_t *no_assignments = kafka_List_new();
    kafka_admin_NewPartitions_t *plain = kafka_admin_NewPartitions_increase_to(2);
    kafka_admin_NewPartitions_t *assigned = kafka_admin_NewPartitions_increase_to_with_new_assignments(3, assignments);
    kafka_admin_NewPartitions_t *empty_list = kafka_admin_NewPartitions_increase_to_with_new_assignments(4, no_assignments);
    /* The getters copy: the inner lists hold `int32_t` by value. */
    kafka_List_t *copy = kafka_admin_NewPartitions_assignments(assigned);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(copy));
    const kafka_List_t *second = (const kafka_List_t *)kafka_List_get(copy, 1);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(second));
    TEST_ASSERT_EQUAL_INT32(1, *(const int32_t *)kafka_List_get(second, 0));
    TEST_ASSERT_EQUAL_INT32(2, *(const int32_t *)kafka_List_get(second, 1));
    kafka_List_destroy(copy);
    copy = kafka_admin_NewPartitions_assignments(empty_list);
    TEST_ASSERT_NOT_NULL(copy); /* present and empty, not absent */
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(copy));
    kafka_List_destroy(copy);
    char *text = kafka_admin_NewPartitions_to_string(assigned);
    TEST_ASSERT_NOT_NULL(strstr(text, "3"));
    kafka_string_destroy(text);

    /* Deliberately unsorted input; the result map is sorted by topic name. */
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, (void *)"zeta", plain);
    kafka_Map_put(request, (void *)"alpha", assigned);
    kafka_Map_put(request, (void *)"mu", empty_list);
    kafka_admin_CreatePartitionsResult_t *result = kafka_admin_Admin_create_partitions(f.admin, request);
    kafka_Map_t *values = kafka_admin_CreatePartitionsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_Map_size(values));
    TEST_ASSERT_EQUAL_STRING("alpha", (const char *)kafka_Map_key(values, 0));
    TEST_ASSERT_EQUAL_STRING("mu", (const char *)kafka_Map_key(values, 1));
    TEST_ASSERT_EQUAL_STRING("zeta", (const char *)kafka_Map_key(values, 2));
    for (int32_t i = 0; i < 3; i++) {
        kafka_common_Error_t *err = get_err((const kafka_common_KafkaFuture_t *)kafka_Map_value(values, i));
        assert_error_message(err, "Not implemented yet");
    }
    kafka_Map_destroy(values);
    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_admin_NewPartitions_destroy(plain);
    kafka_admin_NewPartitions_destroy(assigned);
    kafka_admin_NewPartitions_destroy(empty_list);
    kafka_List_destroy(assignments);
    kafka_List_destroy(no_assignments);
    kafka_List_destroy(brokers0);
    kafka_List_destroy(brokers1);
    fixture_destroy(&f);
}

/* An empty request is legal and yields an empty result whose `all()` is
 * already complete. */
static void test_mock_admin_create_partitions_empty_request(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_Map_t *request = kafka_Map_new();
    kafka_admin_CreatePartitionsResult_t *result = kafka_admin_Admin_create_partitions(f.admin, request);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_CreatePartitionsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    TEST_ASSERT_NULL(kafka_Map_key(values, 0));
    kafka_Map_destroy(values);
    expect_void_ok(kafka_admin_CreatePartitionsResult_all(result));
    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_Map_destroy(request);
    fixture_destroy(&f);
}

static void test_mock_admin_create_partitions_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "grow-async", 1, 1);
    kafka_admin_NewPartitions_t *spec = kafka_admin_NewPartitions_increase_to(2);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, (void *)"grow-async", spec);
    kafka_admin_CreatePartitionsResult_t *result = kafka_admin_Admin_create_partitions(f.admin, request);
    kafka_Map_t *values = kafka_admin_CreatePartitionsResult_values(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(values, "grow-async"), &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_Map_destroy(values);
    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_admin_NewPartitions_destroy(spec);
    fixture_destroy(&f);
}

/* Java's mock throws "Not implemented yet" for a non-empty `deleteRecords`;
 * the Rust mock fails each partition's future. `-1` is Java's documented
 * "truncate to the high watermark". The map is sorted by (topic, partition). */
static void test_mock_admin_delete_records_reports_unsupported_per_partition(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "trimmed", 2, 1);
    kafka_common_TopicPartition_t *t1 = tp_new("trimmed", 1);
    kafka_common_TopicPartition_t *a0 = tp_new("another", 0);
    kafka_common_TopicPartition_t *t0 = tp_new("trimmed", 0);
    kafka_admin_RecordsToDelete_t *r5 = kafka_admin_RecordsToDelete_with_offset(5);
    kafka_admin_RecordsToDelete_t *rhw = kafka_admin_RecordsToDelete_with_offset(-1);
    kafka_admin_RecordsToDelete_t *r10 = kafka_admin_RecordsToDelete_with_offset(10);
    TEST_ASSERT_EQUAL_INT64(5, kafka_admin_RecordsToDelete_before_offset(r5));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_RecordsToDelete_before_offset(rhw));
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, t1, r5);
    kafka_Map_put(request, a0, rhw);
    kafka_Map_put(request, t0, r10);
    kafka_admin_DeleteRecordsResult_t *result = kafka_admin_Admin_delete_records(f.admin, request);
    TEST_ASSERT_NOT_NULL(result); /* a per-partition failure is NOT a call failure */
    kafka_Map_t *watermarks = kafka_admin_DeleteRecordsResult_low_watermarks(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_Map_size(watermarks));
    const kafka_common_TopicPartition_t *k0 = (const kafka_common_TopicPartition_t *)kafka_Map_key(watermarks, 0);
    TEST_ASSERT_EQUAL_STRING("another", kafka_common_TopicPartition_topic(k0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartition_partition(k0));
    const kafka_common_TopicPartition_t *k2 = (const kafka_common_TopicPartition_t *)kafka_Map_key(watermarks, 2);
    TEST_ASSERT_EQUAL_STRING("trimmed", kafka_common_TopicPartition_topic(k2));
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_TopicPartition_partition(k2));
    for (int32_t i = 0; i < 3; i++) {
        kafka_common_Error_t *err = get_err((const kafka_common_KafkaFuture_t *)kafka_Map_value(watermarks, i));
        TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
        assert_error_message(err, "Not implemented yet");
    }
    /* Lookup by a C-built key compares by value. */
    TEST_ASSERT_NOT_NULL(kafka_Map_get(watermarks, t0));
    TEST_ASSERT_NULL(kafka_Map_key(watermarks, 3));
    kafka_Map_destroy(watermarks);
    expect_fails_with(kafka_admin_DeleteRecordsResult_all(result), "Not implemented yet");
    kafka_admin_DeleteRecordsResult_destroy(result);
    kafka_admin_DeleteRecordsResult_destroy(NULL);
    kafka_Map_destroy(request);
    kafka_admin_RecordsToDelete_destroy(r5);
    kafka_admin_RecordsToDelete_destroy(rhw);
    kafka_admin_RecordsToDelete_destroy(r10);
    kafka_common_TopicPartition_destroy(t1);
    kafka_common_TopicPartition_destroy(a0);
    kafka_common_TopicPartition_destroy(t0);
    fixture_destroy(&f);
}

/* The empty `deleteRecords` is the one path Java's mock does implement: an
 * empty result, `all()` complete. */
static void test_mock_admin_delete_records_empty_request(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_Map_t *request = kafka_Map_new();
    kafka_admin_DeleteRecordsResult_t *result = kafka_admin_Admin_delete_records(f.admin, request);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *watermarks = kafka_admin_DeleteRecordsResult_low_watermarks(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(watermarks));
    kafka_Map_destroy(watermarks);
    expect_void_ok(kafka_admin_DeleteRecordsResult_all(result));
    kafka_admin_DeleteRecordsResult_destroy(result);
    kafka_Map_destroy(request);
    fixture_destroy(&f);
}

static void test_mock_admin_delete_records_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "trim-async", 1, 1);
    kafka_common_TopicPartition_t *tp = tp_new("trim-async", 0);
    kafka_admin_RecordsToDelete_t *rtd = kafka_admin_RecordsToDelete_with_offset(3);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, tp, rtd);
    kafka_admin_DeleteRecordsResult_t *result = kafka_admin_Admin_delete_records(f.admin, request);
    kafka_Map_t *watermarks = kafka_admin_DeleteRecordsResult_low_watermarks(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(watermarks, tp), &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_Map_destroy(watermarks);
    kafka_admin_DeleteRecordsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_admin_RecordsToDelete_destroy(rtd);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// describeCluster
// ---------------------------------------------------------------------------

/* The four futures of a `DescribeClusterResult`. Java's mock ignores the
 * options and always completes `authorizedOperations` with an *empty* set,
 * never null — so the list is present, with size 0, with or without
 * `includeAuthorizedOperations`. */
static void test_mock_admin_describe_cluster_sync(void) {
    fixture_t f;
    fixture_init(&f, 3);
    kafka_admin_DescribeClusterResult_t *result = kafka_admin_Admin_describe_cluster(f.admin);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *cluster_id = kafka_admin_DescribeClusterResult_cluster_id(result);
    TEST_ASSERT_EQUAL_STRING("4A5xz_QZTB2CtL4wc0X0Jw", (const char *)get_ok(cluster_id));
    kafka_common_KafkaFuture_destroy(cluster_id);
    kafka_common_KafkaFuture_t *nodes = kafka_admin_DescribeClusterResult_nodes(result);
    const kafka_List_t *node_list = (const kafka_List_t *)get_ok(nodes);
    TEST_ASSERT_EQUAL_INT32(3, kafka_List_size(node_list));
    const kafka_common_Node_t *node = (const kafka_common_Node_t *)kafka_List_get(node_list, 0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Node_id(node));
    TEST_ASSERT_EQUAL_STRING("localhost", kafka_common_Node_host(node));
    TEST_ASSERT_EQUAL_INT32(1000, kafka_common_Node_port(node));
    TEST_ASSERT_FALSE(kafka_common_Node_has_rack(node));
    TEST_ASSERT_NULL(kafka_common_Node_rack(node));
    TEST_ASSERT_EQUAL_INT32(1002, kafka_common_Node_port((const kafka_common_Node_t *)kafka_List_get(node_list, 2)));
    TEST_ASSERT_NULL(kafka_List_get(node_list, 3));
    kafka_common_KafkaFuture_destroy(nodes);
    kafka_common_KafkaFuture_t *controller = kafka_admin_DescribeClusterResult_controller(result);
    const kafka_common_Node_t *c = (const kafka_common_Node_t *)get_ok(controller);
    TEST_ASSERT_NOT_NULL(c);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Node_id(c));
    kafka_common_KafkaFuture_destroy(controller);
    kafka_common_KafkaFuture_t *operations = kafka_admin_DescribeClusterResult_authorized_operations(result);
    const kafka_List_t *operation_list = (const kafka_List_t *)get_ok(operations);
    TEST_ASSERT_NOT_NULL(operation_list);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(operation_list));
    kafka_common_KafkaFuture_destroy(operations);
    kafka_admin_DescribeClusterResult_destroy(result);
    kafka_admin_DescribeClusterResult_destroy(NULL);

    kafka_admin_DescribeClusterOptions_t *options = kafka_admin_DescribeClusterOptions_new();
    kafka_admin_DescribeClusterOptions_set_include_authorized_operations(options, 1);
    TEST_ASSERT_TRUE(kafka_admin_DescribeClusterOptions_include_authorized_operations(options));
    result = kafka_admin_Admin_describe_cluster_with_options(f.admin, options);
    kafka_admin_DescribeClusterOptions_destroy(options);
    operations = kafka_admin_DescribeClusterResult_authorized_operations(result);
    operation_list = (const kafka_List_t *)get_ok(operations);
    TEST_ASSERT_NOT_NULL(operation_list);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(operation_list));
    kafka_common_KafkaFuture_destroy(operations);
    kafka_admin_DescribeClusterResult_destroy(result);
    fixture_destroy(&f);
}

/* `timeoutNextRequest(1)` fails all four futures of the next call. */
static void test_mock_admin_describe_cluster_call_error(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_MockAdminClient_timeout_next_request(f.mock, 1);
    kafka_admin_DescribeClusterResult_t *result = kafka_admin_Admin_describe_cluster(f.admin);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *cluster_id = kafka_admin_DescribeClusterResult_cluster_id(result);
    kafka_common_Error_t *err = get_err(cluster_id);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_REQUEST_TIMED_OUT, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_timeout_error(err));
    assert_error_message(err, "The mock timed out the request.");
    kafka_common_KafkaFuture_destroy(cluster_id);
    expect_fails_with(kafka_admin_DescribeClusterResult_nodes(result), "The mock timed out the request.");
    expect_fails_with(kafka_admin_DescribeClusterResult_controller(result), "The mock timed out the request.");
    kafka_admin_DescribeClusterResult_destroy(result);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_cluster_async(void) {
    fixture_t f;
    fixture_init(&f, 2);
    kafka_admin_DescribeClusterResult_t *result = kafka_admin_Admin_describe_cluster(f.admin);
    kafka_common_KafkaFuture_t *nodes = kafka_admin_DescribeClusterResult_nodes(result);
    cb_result_t r;
    get_cb_pumped(&f, nodes, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size((const kafka_List_t *)r.value));
    kafka_common_KafkaFuture_destroy(nodes);
    kafka_common_KafkaFuture_t *cluster_id = kafka_admin_DescribeClusterResult_cluster_id(result);
    get_cb_pumped(&f, cluster_id, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("4A5xz_QZTB2CtL4wc0X0Jw", (const char *)r.value);
    kafka_common_KafkaFuture_destroy(cluster_id);
    kafka_admin_DescribeClusterResult_destroy(result);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// describeConfigs / incrementalAlterConfigs / listConfigResources
// ---------------------------------------------------------------------------

/* Four resources, three outcomes: a described topic, an unknown topic, the
 * broker's seeded `default.replication.factor`, and BROKER_LOGGER, which hits
 * `getResourceDescription`'s default branch
 * (UnsupportedOperationException("Not implemented yet")). The map is keyed by
 * `ConfigResource`, sorted by (type, name). */
static void test_mock_admin_describe_configs_partial_failure(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "cfg-topic", 1, 1);
    alter_one_config(&f, kafka_common_config_ConfigResource_Type_topic(), "cfg-topic", "retention.ms", "60000",
                     kafka_admin_AlterConfigOp_OpType_set());
    kafka_common_config_ConfigResource_t *topic = topic_resource("cfg-topic");
    kafka_common_config_ConfigResource_t *missing = topic_resource("missing-cfg-topic");
    kafka_common_config_ConfigResource_t *broker =
        kafka_common_config_ConfigResource_new(kafka_common_config_ConfigResource_Type_broker(), "0");
    kafka_common_config_ConfigResource_t *logger =
        kafka_common_config_ConfigResource_new(kafka_common_config_ConfigResource_Type_broker_logger(), "0");
    TEST_ASSERT_EQUAL_STRING("0", kafka_common_config_ConfigResource_name(broker));
    TEST_ASSERT_EQUAL_INT(kafka_common_config_ConfigResource_Type_e_broker,
                          kafka_common_config_ConfigResource_Type__enum(kafka_common_config_ConfigResource_type(broker)));
    kafka_List_t *resources = kafka_List_new();
    kafka_List_add(resources, logger);
    kafka_List_add(resources, topic);
    kafka_List_add(resources, broker);
    kafka_List_add(resources, missing);
    kafka_admin_DescribeConfigsOptions_t *options = kafka_admin_DescribeConfigsOptions_new();
    kafka_admin_DescribeConfigsOptions_set_include_synonyms(options, 1);
    kafka_admin_DescribeConfigsOptions_set_include_documentation(options, 1);
    TEST_ASSERT_TRUE(kafka_admin_DescribeConfigsOptions_include_synonyms(options));
    kafka_admin_DescribeConfigsResult_t *result = kafka_admin_Admin_describe_configs_with_options(f.admin, resources, options);
    kafka_admin_DescribeConfigsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result); /* per-resource failures are not call failures */
    kafka_Map_t *values = kafka_admin_DescribeConfigsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(4, kafka_Map_size(values));
    /* Two resources share the name "0" and stay distinct keys: the key is the
     * (type, name) pair, compared by value. */
    int brokers_named_0 = 0;
    for (int32_t i = 0; i < 4; i++) {
        const kafka_common_config_ConfigResource_t *k = (const kafka_common_config_ConfigResource_t *)kafka_Map_key(values, i);
        brokers_named_0 += strcmp(kafka_common_config_ConfigResource_name(k), "0") == 0;
    }
    TEST_ASSERT_EQUAL_INT(2, brokers_named_0);

    /* 1. the topic we altered. The mock builds entries with
     * `new ConfigEntry(name, value)` (`toConfigObject`), which leaves source
     * UNKNOWN, type UNKNOWN, no documentation and no synonyms — even though
     * this request asked for synonyms and documentation. */
    const kafka_admin_Config_t *config = (const kafka_admin_Config_t *)get_ok(future_for(values, topic));
    kafka_List_t *entries = kafka_admin_Config_entries(config);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(entries));
    const kafka_admin_ConfigEntry_t *entry = (const kafka_admin_ConfigEntry_t *)kafka_List_get(entries, 0);
    TEST_ASSERT_EQUAL_STRING("retention.ms", kafka_admin_ConfigEntry_name(entry));
    TEST_ASSERT_EQUAL_STRING("60000", kafka_admin_ConfigEntry_value(entry));
    TEST_ASSERT_TRUE(kafka_admin_ConfigEntry_source(entry) == kafka_admin_ConfigEntry_ConfigSource_unknown());
    TEST_ASSERT_EQUAL_INT(kafka_admin_ConfigEntry_ConfigSource_e_unknown,
                          kafka_admin_ConfigEntry_ConfigSource__enum(kafka_admin_ConfigEntry_source(entry)));
    TEST_ASSERT_EQUAL_INT(kafka_admin_ConfigEntry_ConfigType_e_unknown,
                          kafka_admin_ConfigEntry_ConfigType__enum(kafka_admin_ConfigEntry_type(entry)));
    TEST_ASSERT_FALSE(kafka_admin_ConfigEntry_is_default(entry));
    TEST_ASSERT_FALSE(kafka_admin_ConfigEntry_is_sensitive(entry));
    TEST_ASSERT_FALSE(kafka_admin_ConfigEntry_is_read_only(entry));
    TEST_ASSERT_NULL(kafka_admin_ConfigEntry_documentation(entry));
    kafka_List_t *synonyms = kafka_admin_ConfigEntry_synonyms(entry);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(synonyms));
    kafka_List_destroy(synonyms);
    kafka_List_destroy(entries);
    /* Java's Config.get(name). */
    TEST_ASSERT_NOT_NULL(kafka_admin_Config_get(config, "retention.ms"));
    TEST_ASSERT_NULL(kafka_admin_Config_get(config, "no.such.config"));
    /* 2. unknown topic -> UNKNOWN_TOPIC_OR_PARTITION. */
    kafka_common_Error_t *err = get_err(future_for(values, missing));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    /* 3. broker 0 carries the seeded default.replication.factor. */
    config = (const kafka_admin_Config_t *)get_ok(future_for(values, broker));
    entry = kafka_admin_Config_get(config, "default.replication.factor");
    TEST_ASSERT_NOT_NULL(entry);
    TEST_ASSERT_EQUAL_STRING("1", kafka_admin_ConfigEntry_value(entry));
    /* 4. BROKER_LOGGER. */
    err = get_err(future_for(values, logger));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(values);
    /* `all()` surfaces one of the per-key failures; which one follows the
     * map's key order, which is not part of the contract. */
    kafka_common_KafkaFuture_t *all = kafka_admin_DescribeConfigsResult_all(result);
    kafka_common_Error_t *all_err = get_err(all);
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(all_err));
    kafka_common_Error_destroy(all_err);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DescribeConfigsResult_destroy(result);
    kafka_admin_DescribeConfigsResult_destroy(NULL);
    kafka_List_destroy(resources);
    kafka_common_config_ConfigResource_destroy(topic);
    kafka_common_config_ConfigResource_destroy(missing);
    kafka_common_config_ConfigResource_destroy(broker);
    kafka_common_config_ConfigResource_destroy(logger);
    fixture_destroy(&f);
}

/* An empty batch yields an empty map and a complete `all()`. */
static void test_mock_admin_describe_configs_empty_batch(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *resources = kafka_List_new();
    kafka_admin_DescribeConfigsResult_t *result = kafka_admin_Admin_describe_configs(f.admin, resources);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_DescribeConfigsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    kafka_common_KafkaFuture_t *all = kafka_admin_DescribeConfigsResult_all(result);
    const kafka_Map_t *all_map = (const kafka_Map_t *)get_ok(all);
    TEST_ASSERT_NOT_NULL(all_map);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(all_map));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DescribeConfigsResult_destroy(result);
    kafka_List_destroy(resources);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_configs_async_partial_failure(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "cfg-async", 1, 1);
    kafka_common_config_ConfigResource_t *topic = topic_resource("cfg-async");
    kafka_common_config_ConfigResource_t *missing = topic_resource("cfg-async-missing");
    kafka_List_t *resources = kafka_List_new();
    kafka_List_add(resources, topic);
    kafka_List_add(resources, missing);
    kafka_admin_DescribeConfigsResult_t *result = kafka_admin_Admin_describe_configs(f.admin, resources);
    kafka_Map_t *values = kafka_admin_DescribeConfigsResult_values(result);
    cb_result_t ok;
    get_cb_pumped(&f, future_for(values, topic), &ok);
    TEST_ASSERT_FALSE(ok.had_error);
    TEST_ASSERT_NOT_NULL(ok.value);
    kafka_List_t *entries = kafka_admin_Config_entries((const kafka_admin_Config_t *)ok.value);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(entries)); /* a fresh topic has no configs */
    kafka_List_destroy(entries);
    cb_result_t failed;
    get_cb_pumped(&f, future_for(values, missing), &failed);
    TEST_ASSERT_TRUE(failed.had_error);
    TEST_ASSERT_NULL(failed.value);
    kafka_Map_destroy(values);
    kafka_admin_DescribeConfigsResult_destroy(result);
    kafka_List_destroy(resources);
    kafka_common_config_ConfigResource_destroy(topic);
    kafka_common_config_ConfigResource_destroy(missing);
    fixture_destroy(&f);
}

/* Two SETs on one resource are one key in the result; DELETE takes a NULL
 * value, which is what Java sends for a removal. */
static void test_mock_admin_incremental_alter_configs_set_then_delete(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "alter-topic", 1, 1);
    kafka_common_config_ConfigResource_t *resource = topic_resource("alter-topic");
    kafka_admin_ConfigEntry_t *retention = kafka_admin_ConfigEntry_new("retention.ms", "1000");
    kafka_admin_ConfigEntry_t *segment = kafka_admin_ConfigEntry_new("segment.ms", "2000");
    kafka_admin_AlterConfigOp_t *op1 = kafka_admin_AlterConfigOp_new(retention, kafka_admin_AlterConfigOp_OpType_set());
    kafka_admin_AlterConfigOp_t *op2 = kafka_admin_AlterConfigOp_new(segment, kafka_admin_AlterConfigOp_OpType_set());
    TEST_ASSERT_TRUE(kafka_admin_AlterConfigOp_op_type(op1) == kafka_admin_AlterConfigOp_OpType_set());
    TEST_ASSERT_EQUAL_STRING("retention.ms", kafka_admin_ConfigEntry_name(kafka_admin_AlterConfigOp_config_entry(op1)));
    kafka_List_t *ops = kafka_List_new();
    kafka_List_add(ops, op1);
    kafka_List_add(ops, op2);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, resource, ops);
    kafka_admin_AlterConfigsOptions_t *options = kafka_admin_AlterConfigsOptions_new();
    kafka_admin_AlterConfigsOptions_set_timeout_ms(options, 5000);
    kafka_admin_AlterConfigsResult_t *result = kafka_admin_Admin_incremental_alter_configs_with_options(f.admin, request, options);
    kafka_admin_AlterConfigsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterConfigsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    const kafka_common_config_ConfigResource_t *key = (const kafka_common_config_ConfigResource_t *)kafka_Map_key(values, 0);
    TEST_ASSERT_EQUAL_STRING("alter-topic", kafka_common_config_ConfigResource_name(key));
    TEST_ASSERT_NULL(get_ok(future_for(values, resource)));
    kafka_Map_destroy(values);
    expect_void_ok(kafka_admin_AlterConfigsResult_all(result));
    kafka_admin_AlterConfigsResult_destroy(result);
    kafka_admin_AlterConfigsResult_destroy(NULL);
    kafka_Map_destroy(request);
    kafka_List_destroy(ops);
    kafka_admin_AlterConfigOp_destroy(op1);
    kafka_admin_AlterConfigOp_destroy(op2);
    kafka_admin_ConfigEntry_destroy(retention);
    kafka_admin_ConfigEntry_destroy(segment);

    /* Both keys landed. */
    kafka_List_t *resources = kafka_List_new();
    kafka_List_add(resources, resource);
    kafka_admin_DescribeConfigsResult_t *described = kafka_admin_Admin_describe_configs(f.admin, resources);
    values = kafka_admin_DescribeConfigsResult_values(described);
    const kafka_admin_Config_t *config = (const kafka_admin_Config_t *)get_ok(future_for(values, resource));
    kafka_List_t *entries = kafka_admin_Config_entries(config);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(entries));
    kafka_List_destroy(entries);
    TEST_ASSERT_EQUAL_STRING("1000", kafka_admin_ConfigEntry_value(kafka_admin_Config_get(config, "retention.ms")));
    kafka_Map_destroy(values);
    kafka_admin_DescribeConfigsResult_destroy(described);

    alter_one_config(&f, kafka_common_config_ConfigResource_Type_topic(), "alter-topic", "retention.ms", NULL,
                     kafka_admin_AlterConfigOp_OpType_delete());
    described = kafka_admin_Admin_describe_configs(f.admin, resources);
    values = kafka_admin_DescribeConfigsResult_values(described);
    config = (const kafka_admin_Config_t *)get_ok(future_for(values, resource));
    entries = kafka_admin_Config_entries(config);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(entries));
    kafka_List_destroy(entries);
    TEST_ASSERT_NULL(kafka_admin_Config_get(config, "retention.ms"));
    kafka_Map_destroy(values);
    kafka_admin_DescribeConfigsResult_destroy(described);
    kafka_List_destroy(resources);
    kafka_common_config_ConfigResource_destroy(resource);
    fixture_destroy(&f);
}

/* APPEND is rejected by the mock's `handleIncrementalResourceAlteration`
 * default branch (InvalidRequestException), failing the whole resource; an
 * unknown topic fails its own key. */
static void test_mock_admin_incremental_alter_configs_partial_failure(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "alter-ok", 1, 1);
    kafka_common_config_ConfigResource_t *ok = topic_resource("alter-ok");
    kafka_common_config_ConfigResource_t *missing = topic_resource("alter-missing");
    kafka_admin_ConfigEntry_t *retention = kafka_admin_ConfigEntry_new("retention.ms", "1000");
    kafka_admin_ConfigEntry_t *policy = kafka_admin_ConfigEntry_new("cleanup.policy", "compact");
    kafka_admin_AlterConfigOp_t *set_op = kafka_admin_AlterConfigOp_new(retention, kafka_admin_AlterConfigOp_OpType_set());
    kafka_admin_AlterConfigOp_t *append_op = kafka_admin_AlterConfigOp_new(policy, kafka_admin_AlterConfigOp_OpType_append());
    kafka_List_t *ok_ops = kafka_List_new();
    kafka_List_add(ok_ops, set_op);
    kafka_List_add(ok_ops, append_op);
    kafka_List_t *missing_ops = kafka_List_new();
    kafka_List_add(missing_ops, set_op);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, ok, ok_ops);
    kafka_Map_put(request, missing, missing_ops);
    kafka_admin_AlterConfigsResult_t *result = kafka_admin_Admin_incremental_alter_configs(f.admin, request);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterConfigsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    kafka_common_Error_t *err = get_err(future_for(values, ok));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_INVALID_REQUEST, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_invalid_request_error(err));
    kafka_common_Error_destroy(err);
    err = get_err(future_for(values, missing));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(values);
    kafka_admin_AlterConfigsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_List_destroy(ok_ops);
    kafka_List_destroy(missing_ops);
    kafka_admin_AlterConfigOp_destroy(set_op);
    kafka_admin_AlterConfigOp_destroy(append_op);
    kafka_admin_ConfigEntry_destroy(retention);
    kafka_admin_ConfigEntry_destroy(policy);
    kafka_common_config_ConfigResource_destroy(ok);
    kafka_common_config_ConfigResource_destroy(missing);
    fixture_destroy(&f);
}

/* `AlterConfigOp.OpType.forId`: the four singletons round-trip through their
 * ids; an unknown id is NULL (Java returns null too). */
static void test_mock_admin_alter_config_op_type_for_id(void) {
    const kafka_admin_AlterConfigOp_OpType_t *set = kafka_admin_AlterConfigOp_OpType_set();
    TEST_ASSERT_EQUAL_INT(kafka_admin_AlterConfigOp_OpType_e_set, kafka_admin_AlterConfigOp_OpType__enum(set));
    TEST_ASSERT_EQUAL_INT8(0, kafka_admin_AlterConfigOp_OpType_id(set));
    TEST_ASSERT_TRUE(kafka_admin_AlterConfigOp_OpType_for_id(0) == set);
    TEST_ASSERT_TRUE(kafka_admin_AlterConfigOp_OpType_for_id(1) == kafka_admin_AlterConfigOp_OpType_delete());
    TEST_ASSERT_TRUE(kafka_admin_AlterConfigOp_OpType_for_id(2) == kafka_admin_AlterConfigOp_OpType_append());
    TEST_ASSERT_TRUE(kafka_admin_AlterConfigOp_OpType_for_id(3) == kafka_admin_AlterConfigOp_OpType_subtract());
    TEST_ASSERT_EQUAL_INT(kafka_admin_AlterConfigOp_OpType_e_subtract,
                          kafka_admin_AlterConfigOp_OpType__enum(kafka_admin_AlterConfigOp_OpType_subtract()));
    TEST_ASSERT_NULL(kafka_admin_AlterConfigOp_OpType_for_id(99));
    TEST_ASSERT_NULL(kafka_admin_AlterConfigOp_OpType_for_id(-1));
}

static void test_mock_admin_incremental_alter_configs_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "alter-async", 1, 1);
    kafka_common_config_ConfigResource_t *resource = topic_resource("alter-async");
    kafka_admin_ConfigEntry_t *entry = kafka_admin_ConfigEntry_new("retention.ms", "1000");
    kafka_admin_AlterConfigOp_t *op = kafka_admin_AlterConfigOp_new(entry, kafka_admin_AlterConfigOp_OpType_set());
    kafka_List_t *ops = kafka_List_new();
    kafka_List_add(ops, op);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, resource, ops);
    kafka_admin_AlterConfigsResult_t *result = kafka_admin_Admin_incremental_alter_configs(f.admin, request);
    kafka_Map_t *values = kafka_admin_AlterConfigsResult_values(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(values, resource), &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_NULL(r.value);
    kafka_Map_destroy(values);
    kafka_admin_AlterConfigsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_List_destroy(ops);
    kafka_admin_AlterConfigOp_destroy(op);
    kafka_admin_ConfigEntry_destroy(entry);
    kafka_common_config_ConfigResource_destroy(resource);
    fixture_destroy(&f);
}

static int32_t count_config_resources_of_type(const kafka_List_t *resources,
                                              const kafka_common_config_ConfigResource_Type_t *type) {
    int32_t count = 0;
    for (int32_t i = 0; i < kafka_List_size(resources); i++) {
        const kafka_common_config_ConfigResource_t *r = (const kafka_common_config_ConfigResource_t *)kafka_List_get(resources, i);
        if (kafka_common_config_ConfigResource_type(r) == type) {
            count++;
        }
    }
    return count;
}

/* Filtered to TOPIC, then unfiltered: every supported type — the 2 topics
 * plus one BROKER and one BROKER_LOGGER per broker, no CLIENT_METRICS. The
 * list comes back in broker order, so only counts are asserted. */
static void test_mock_admin_list_config_resources(void) {
    fixture_t f;
    fixture_init(&f, 2);
    create_one(&f, "lcr-b", 1, 1);
    create_one(&f, "lcr-a", 1, 1);
    kafka_List_t *topic_only = kafka_List_new();
    kafka_List_add(topic_only, (void *)kafka_common_config_ConfigResource_Type_topic());
    kafka_admin_ListConfigResourcesOptions_t *options = kafka_admin_ListConfigResourcesOptions_new();
    kafka_admin_ListConfigResourcesResult_t *result =
        kafka_admin_Admin_list_config_resources_with_options(f.admin, topic_only, options);
    kafka_admin_ListConfigResourcesOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListConfigResourcesResult_all(result);
    const kafka_List_t *resources = (const kafka_List_t *)get_ok(all);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(resources));
    TEST_ASSERT_EQUAL_INT32(2, count_config_resources_of_type(resources, kafka_common_config_ConfigResource_Type_topic()));
    int found_a = 0, found_b = 0;
    for (int32_t i = 0; i < 2; i++) {
        const char *name =
            kafka_common_config_ConfigResource_name((const kafka_common_config_ConfigResource_t *)kafka_List_get(resources, i));
        found_a |= strcmp(name, "lcr-a") == 0;
        found_b |= strcmp(name, "lcr-b") == 0;
    }
    TEST_ASSERT_TRUE(found_a && found_b);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListConfigResourcesResult_destroy(result);
    kafka_List_destroy(topic_only);

    result = kafka_admin_Admin_list_config_resources(f.admin);
    all = kafka_admin_ListConfigResourcesResult_all(result);
    resources = (const kafka_List_t *)get_ok(all);
    TEST_ASSERT_EQUAL_INT32(2, count_config_resources_of_type(resources, kafka_common_config_ConfigResource_Type_topic()));
    TEST_ASSERT_EQUAL_INT32(2, count_config_resources_of_type(resources, kafka_common_config_ConfigResource_Type_broker()));
    TEST_ASSERT_EQUAL_INT32(2, count_config_resources_of_type(resources, kafka_common_config_ConfigResource_Type_broker_logger()));
    TEST_ASSERT_EQUAL_INT32(0, count_config_resources_of_type(resources, kafka_common_config_ConfigResource_Type_client_metrics()));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListConfigResourcesResult_destroy(result);
    kafka_admin_ListConfigResourcesResult_destroy(NULL);
    fixture_destroy(&f);
}

static void test_mock_admin_list_config_resources_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "lcr-async", 1, 1);
    kafka_List_t *topic_only = kafka_List_new();
    kafka_List_add(topic_only, (void *)kafka_common_config_ConfigResource_Type_topic());
    kafka_admin_ListConfigResourcesOptions_t *options = kafka_admin_ListConfigResourcesOptions_new();
    kafka_admin_ListConfigResourcesResult_t *result =
        kafka_admin_Admin_list_config_resources_with_options(f.admin, topic_only, options);
    kafka_admin_ListConfigResourcesOptions_destroy(options);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListConfigResourcesResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size((const kafka_List_t *)r.value));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListConfigResourcesResult_destroy(result);
    kafka_List_destroy(topic_only);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// describeLogDirs / alterReplicaLogDirs / describeReplicaLogDirs
// ---------------------------------------------------------------------------

/* Broker 0 reports one log dir ("/tmp/kafka-logs") holding every replica it
 * hosts; broker 7, which does not exist, reports an empty map. The outer map
 * is keyed by broker id (`int32_t`), each value a future of a map keyed by
 * log dir. */
static void test_mock_admin_describe_log_dirs(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "ld-topic", 2, 1);
    int32_t broker0 = 0, broker7 = 7;
    kafka_List_t *brokers = kafka_List_new();
    kafka_List_add(brokers, &broker7);
    kafka_List_add(brokers, &broker0);
    kafka_admin_DescribeLogDirsOptions_t *options = kafka_admin_DescribeLogDirsOptions_new();
    kafka_admin_DescribeLogDirsOptions_set_timeout_ms(options, 5000);
    kafka_admin_DescribeLogDirsResult_t *result = kafka_admin_Admin_describe_log_dirs_with_options(f.admin, brokers, options);
    kafka_admin_DescribeLogDirsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *descriptions = kafka_admin_DescribeLogDirsResult_descriptions(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(descriptions));
    TEST_ASSERT_EQUAL_INT32(0, *(const int32_t *)kafka_Map_key(descriptions, 0)); /* sorted */
    TEST_ASSERT_EQUAL_INT32(7, *(const int32_t *)kafka_Map_key(descriptions, 1));

    const kafka_Map_t *dirs = (const kafka_Map_t *)get_ok(future_for(descriptions, &broker0));
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(dirs));
    TEST_ASSERT_EQUAL_STRING("/tmp/kafka-logs", (const char *)kafka_Map_key(dirs, 0));
    const kafka_admin_LogDirDescription_t *dir = (const kafka_admin_LogDirDescription_t *)kafka_Map_get(dirs, (void *)"/tmp/kafka-logs");
    TEST_ASSERT_NOT_NULL(dir);
    TEST_ASSERT_NULL(kafka_admin_LogDirDescription_error(dir));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_LogDirDescription_total_bytes(dir)); /* OptionalLong.empty() */
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_LogDirDescription_usable_bytes(dir));
    TEST_ASSERT_FALSE(kafka_admin_LogDirDescription_is_cordoned(dir));
    kafka_Map_t *replicas = kafka_admin_LogDirDescription_replica_infos(dir); /* owned copy */
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(replicas));
    const kafka_common_TopicPartition_t *rk = (const kafka_common_TopicPartition_t *)kafka_Map_key(replicas, 1);
    TEST_ASSERT_EQUAL_STRING("ld-topic", kafka_common_TopicPartition_topic(rk));
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_TopicPartition_partition(rk));
    kafka_common_TopicPartition_t *lookup = tp_new("ld-topic", 0);
    const kafka_admin_ReplicaInfo_t *info = (const kafka_admin_ReplicaInfo_t *)kafka_Map_get(replicas, lookup);
    TEST_ASSERT_NOT_NULL(info);
    TEST_ASSERT_EQUAL_INT64(0, kafka_admin_ReplicaInfo_size(info));
    TEST_ASSERT_EQUAL_INT64(0, kafka_admin_ReplicaInfo_offset_lag(info));
    TEST_ASSERT_FALSE(kafka_admin_ReplicaInfo_is_future(info));
    kafka_common_TopicPartition_destroy(lookup);
    kafka_Map_destroy(replicas);
    char *text = kafka_admin_LogDirDescription_to_string(dir);
    TEST_ASSERT_NOT_NULL(text);
    kafka_string_destroy(text);

    const kafka_Map_t *none = (const kafka_Map_t *)get_ok(future_for(descriptions, &broker7));
    TEST_ASSERT_NOT_NULL(none);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(none));
    kafka_Map_destroy(descriptions);

    kafka_common_KafkaFuture_t *all = kafka_admin_DescribeLogDirsResult_all_descriptions(result);
    const kafka_Map_t *all_map = (const kafka_Map_t *)get_ok(all);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(all_map));
    const kafka_Map_t *all_dirs0 = (const kafka_Map_t *)kafka_Map_get(all_map, &broker0);
    TEST_ASSERT_NOT_NULL(all_dirs0);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(all_dirs0));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DescribeLogDirsResult_destroy(result);
    kafka_admin_DescribeLogDirsResult_destroy(NULL);
    kafka_List_destroy(brokers);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_log_dirs_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "ld-async", 1, 1);
    int32_t broker0 = 0;
    kafka_List_t *brokers = kafka_List_new();
    kafka_List_add(brokers, &broker0);
    kafka_admin_DescribeLogDirsResult_t *result = kafka_admin_Admin_describe_log_dirs(f.admin, brokers);
    kafka_Map_t *descriptions = kafka_admin_DescribeLogDirsResult_descriptions(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(descriptions, &broker0), &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size((const kafka_Map_t *)r.value));
    kafka_Map_destroy(descriptions);
    kafka_admin_DescribeLogDirsResult_destroy(result);
    kafka_List_destroy(brokers);
    fixture_destroy(&f);
}

/* Four replicas, three outcomes: the move to the broker's one log dir
 * succeeds; a move to any other dir is a KafkaStorageException; a replica
 * the broker does not host (unknown topic, or broker 9) is
 * ReplicaNotAvailable. Keys are `TopicPartitionReplica`, by value. */
static void test_mock_admin_alter_replica_log_dirs_partial_failure(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "mv-topic", 2, 1);
    kafka_common_TopicPartitionReplica_t *ok = kafka_common_TopicPartitionReplica_new("mv-topic", 0, 0);
    kafka_common_TopicPartitionReplica_t *bad_dir = kafka_common_TopicPartitionReplica_new("mv-topic", 1, 0);
    kafka_common_TopicPartitionReplica_t *no_topic = kafka_common_TopicPartitionReplica_new("mv-missing", 0, 0);
    kafka_common_TopicPartitionReplica_t *no_broker = kafka_common_TopicPartitionReplica_new("mv-topic", 0, 9);
    TEST_ASSERT_EQUAL_STRING("mv-topic", kafka_common_TopicPartitionReplica_topic(no_broker));
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartitionReplica_partition(no_broker));
    TEST_ASSERT_EQUAL_INT32(9, kafka_common_TopicPartitionReplica_broker_id(no_broker));
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, ok, (void *)"/tmp/kafka-logs");
    kafka_Map_put(request, bad_dir, (void *)"/data/other");
    kafka_Map_put(request, no_topic, (void *)"/tmp/kafka-logs");
    kafka_Map_put(request, no_broker, (void *)"/tmp/kafka-logs");
    kafka_admin_AlterReplicaLogDirsResult_t *result = kafka_admin_Admin_alter_replica_log_dirs(f.admin, request);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterReplicaLogDirsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(4, kafka_Map_size(values));
    TEST_ASSERT_NULL(get_ok(future_for(values, ok)));
    kafka_common_Error_t *err = get_err(future_for(values, bad_dir));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_KAFKA_STORAGE_ERROR, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_kafka_storage_error(err));
    kafka_common_Error_destroy(err);
    err = get_err(future_for(values, no_topic));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_REPLICA_NOT_AVAILABLE, kafka_common_Error_code(err));
    TEST_ASSERT_TRUE(kafka_common_Error_is_replica_not_available_error(err));
    kafka_common_Error_destroy(err);
    err = get_err(future_for(values, no_broker));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_REPLICA_NOT_AVAILABLE, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(values);
    /* `all()` reports one of the failures; which one is not contractual. */
    kafka_common_KafkaFuture_t *all = kafka_admin_AlterReplicaLogDirsResult_all(result);
    err = get_err(all);
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(err));
    kafka_common_Error_destroy(err);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_AlterReplicaLogDirsResult_destroy(result);
    kafka_admin_AlterReplicaLogDirsResult_destroy(NULL);
    kafka_Map_destroy(request);

    /* The successful move is recorded as the replica's future log dir. */
    kafka_List_t *replicas = kafka_List_new();
    kafka_List_add(replicas, ok);
    kafka_admin_DescribeReplicaLogDirsResult_t *described = kafka_admin_Admin_describe_replica_log_dirs(f.admin, replicas);
    kafka_Map_t *infos = kafka_admin_DescribeReplicaLogDirsResult_values(described);
    const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *info =
        (const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *)get_ok(future_for(infos, ok));
    TEST_ASSERT_EQUAL_STRING("/tmp/kafka-logs", kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir(info));
    kafka_Map_destroy(infos);
    kafka_admin_DescribeReplicaLogDirsResult_destroy(described);
    kafka_List_destroy(replicas);

    kafka_common_TopicPartitionReplica_destroy(ok);
    kafka_common_TopicPartitionReplica_destroy(bad_dir);
    kafka_common_TopicPartitionReplica_destroy(no_topic);
    kafka_common_TopicPartitionReplica_destroy(no_broker);
    fixture_destroy(&f);
}

static void test_mock_admin_alter_replica_log_dirs_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "mv-async", 1, 1);
    kafka_common_TopicPartitionReplica_t *replica = kafka_common_TopicPartitionReplica_new("mv-async", 0, 0);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, replica, (void *)"/data/other");
    kafka_admin_AlterReplicaLogDirsResult_t *result = kafka_admin_Admin_alter_replica_log_dirs(f.admin, request);
    kafka_Map_t *values = kafka_admin_AlterReplicaLogDirsResult_values(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(values, replica), &r);
    TEST_ASSERT_TRUE(r.had_error);
    kafka_Map_destroy(values);
    kafka_admin_AlterReplicaLogDirsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_common_TopicPartitionReplica_destroy(replica);
    fixture_destroy(&f);
}

/* Every hosted replica sits in "/tmp/kafka-logs" with lag 0 and no future
 * dir; a replica of an unknown topic is skipped by the mock, so the result
 * is shorter than the request. */
static void test_mock_admin_describe_replica_log_dirs(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "drl-topic", 2, 1);
    kafka_common_TopicPartitionReplica_t *r0 = kafka_common_TopicPartitionReplica_new("drl-topic", 0, 0);
    kafka_common_TopicPartitionReplica_t *r1 = kafka_common_TopicPartitionReplica_new("drl-topic", 1, 0);
    kafka_common_TopicPartitionReplica_t *unknown = kafka_common_TopicPartitionReplica_new("drl-missing", 0, 0);
    kafka_List_t *replicas = kafka_List_new();
    kafka_List_add(replicas, r1);
    kafka_List_add(replicas, unknown);
    kafka_List_add(replicas, r0);
    kafka_admin_DescribeReplicaLogDirsOptions_t *options = kafka_admin_DescribeReplicaLogDirsOptions_new();
    kafka_admin_DescribeReplicaLogDirsResult_t *result =
        kafka_admin_Admin_describe_replica_log_dirs_with_options(f.admin, replicas, options);
    kafka_admin_DescribeReplicaLogDirsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_DescribeReplicaLogDirsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    TEST_ASSERT_NULL(kafka_Map_get(values, unknown));
    const kafka_common_TopicPartitionReplica_t *k0 = (const kafka_common_TopicPartitionReplica_t *)kafka_Map_key(values, 0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_TopicPartitionReplica_partition(k0)); /* sorted */
    for (int32_t i = 0; i < 2; i++) {
        const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *info =
            (const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *)get_ok(
                (const kafka_common_KafkaFuture_t *)kafka_Map_value(values, i));
        TEST_ASSERT_EQUAL_STRING("/tmp/kafka-logs", kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir(info));
        TEST_ASSERT_EQUAL_INT64(0, kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_offset_lag(info));
        TEST_ASSERT_NULL(kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir(info));
        TEST_ASSERT_EQUAL_INT64(0, kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_offset_lag(info));
        char *text = kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_to_string(info);
        TEST_ASSERT_NOT_NULL(strstr(text, "/tmp/kafka-logs"));
        kafka_string_destroy(text);
    }
    kafka_Map_destroy(values);
    kafka_common_KafkaFuture_t *all = kafka_admin_DescribeReplicaLogDirsResult_all(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size((const kafka_Map_t *)get_ok(all)));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DescribeReplicaLogDirsResult_destroy(result);
    kafka_admin_DescribeReplicaLogDirsResult_destroy(NULL);
    kafka_List_destroy(replicas);
    kafka_common_TopicPartitionReplica_destroy(r0);
    kafka_common_TopicPartitionReplica_destroy(r1);
    kafka_common_TopicPartitionReplica_destroy(unknown);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_replica_log_dirs_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "drl-async", 1, 1);
    kafka_common_TopicPartitionReplica_t *replica = kafka_common_TopicPartitionReplica_new("drl-async", 0, 0);
    kafka_List_t *replicas = kafka_List_new();
    kafka_List_add(replicas, replica);
    kafka_admin_DescribeReplicaLogDirsResult_t *result = kafka_admin_Admin_describe_replica_log_dirs(f.admin, replicas);
    kafka_Map_t *values = kafka_admin_DescribeReplicaLogDirsResult_values(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(values, replica), &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("/tmp/kafka-logs",
                             kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir(
                                 (const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *)r.value));
    kafka_Map_destroy(values);
    kafka_admin_DescribeReplicaLogDirsResult_destroy(result);
    kafka_List_destroy(replicas);
    kafka_common_TopicPartitionReplica_destroy(replica);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// electLeaders
// ---------------------------------------------------------------------------

/* Java's mock throws "Not implemented yet" for `electLeaders`; both futures
 * of the result fail with it. */
static void test_mock_admin_elect_leaders_reports_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    create_one(&f, "elect", 1, 1);
    kafka_common_TopicPartition_t *tp = tp_new("elect", 0);
    kafka_List_t *partitions = kafka_List_new();
    kafka_List_add(partitions, tp);
    kafka_admin_ElectLeadersOptions_t *options = kafka_admin_ElectLeadersOptions_new();
    kafka_admin_ElectLeadersOptions_set_timeout_ms(options, 5000);
    kafka_admin_ElectLeadersResult_t *result =
        kafka_admin_Admin_elect_leaders_with_options(f.admin, kafka_common_ElectionType_preferred(), partitions, options);
    kafka_admin_ElectLeadersOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    expect_fails_with(kafka_admin_ElectLeadersResult_partitions(result), "Not implemented yet");
    expect_fails_with(kafka_admin_ElectLeadersResult_all(result), "Not implemented yet");
    kafka_admin_ElectLeadersResult_destroy(result);
    kafka_admin_ElectLeadersResult_destroy(NULL);
    /* UNCLEAN, and Java's `null` partitions = "all partitions". */
    result = kafka_admin_Admin_elect_leaders(f.admin, kafka_common_ElectionType_unclean(), NULL);
    TEST_ASSERT_NOT_NULL(result);
    expect_fails_with(kafka_admin_ElectLeadersResult_all(result), "Not implemented yet");
    kafka_admin_ElectLeadersResult_destroy(result);
    kafka_List_destroy(partitions);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

/* `ElectionType.valueOf(byte)`: the two singletons round-trip, anything else
 * is Java's IllegalArgumentException. */
static void test_mock_admin_election_type_value_of(void) {
    const kafka_common_ElectionType_t *type = NULL;
    TEST_ASSERT_NULL(kafka_common_ElectionType_value_of(0, &type));
    TEST_ASSERT_TRUE(type == kafka_common_ElectionType_preferred());
    TEST_ASSERT_EQUAL_INT(kafka_common_ElectionType_e_preferred, kafka_common_ElectionType__enum(type));
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_ElectionType_value(type));
    TEST_ASSERT_NULL(kafka_common_ElectionType_value_of(1, &type));
    TEST_ASSERT_TRUE(type == kafka_common_ElectionType_unclean());
    TEST_ASSERT_EQUAL_INT(kafka_common_ElectionType_e_unclean, kafka_common_ElectionType__enum(type));
    type = NULL;
    kafka_common_Error_t *err = kafka_common_ElectionType_value_of(7, &type);
    TEST_ASSERT_NULL(type);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "Value 7 must be one of [PREFERRED, UNCLEAN]");
    kafka_List_t *values = kafka_common_ElectionType_values();
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(values));
    kafka_List_destroy(values);
}

static void test_mock_admin_elect_leaders_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *partitions = kafka_List_new();
    kafka_admin_ElectLeadersResult_t *result = kafka_admin_Admin_elect_leaders(f.admin, kafka_common_ElectionType_preferred(), partitions);
    kafka_common_KafkaFuture_t *all = kafka_admin_ElectLeadersResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ElectLeadersResult_destroy(result);
    kafka_List_destroy(partitions);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// alterPartitionReassignments / listPartitionReassignments
// ---------------------------------------------------------------------------

/* One partition is reassigned, one belongs to an unknown topic. */
static void test_mock_admin_alter_partition_reassignments_partial_failure(void) {
    fixture_t f;
    fixture_init(&f, 3);
    create_one(&f, "ra-topic", 1, 1);
    int32_t b1 = 1, b2 = 2;
    kafka_List_t *replicas = kafka_List_new();
    kafka_List_add(replicas, &b1);
    kafka_List_add(replicas, &b2);
    kafka_admin_NewPartitionReassignment_t *reassignment = NULL;
    TEST_ASSERT_NULL(kafka_admin_NewPartitionReassignment_new(replicas, &reassignment));
    kafka_List_t *targets = kafka_admin_NewPartitionReassignment_target_replicas(reassignment);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(targets));
    TEST_ASSERT_EQUAL_INT32(2, *(const int32_t *)kafka_List_get(targets, 1));
    kafka_List_destroy(targets);
    kafka_common_TopicPartition_t *ok = tp_new("ra-topic", 0);
    kafka_common_TopicPartition_t *missing = tp_new("ra-missing", 0);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, ok, reassignment);
    kafka_Map_put(request, missing, reassignment);
    kafka_admin_AlterPartitionReassignmentsOptions_t *options = kafka_admin_AlterPartitionReassignmentsOptions_new();
    kafka_admin_AlterPartitionReassignmentsResult_t *result =
        kafka_admin_Admin_alter_partition_reassignments_with_options(f.admin, request, options);
    kafka_admin_AlterPartitionReassignmentsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterPartitionReassignmentsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    TEST_ASSERT_NULL(get_ok(future_for(values, ok)));
    kafka_common_Error_t *err = get_err(future_for(values, missing));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_Map_destroy(values);
    expect_fails_with_code(kafka_admin_AlterPartitionReassignmentsResult_all(result), kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION);
    kafka_admin_AlterPartitionReassignmentsResult_destroy(result);
    kafka_admin_AlterPartitionReassignmentsResult_destroy(NULL);
    kafka_Map_destroy(request);
    kafka_common_TopicPartition_destroy(ok);
    kafka_common_TopicPartition_destroy(missing);
    kafka_admin_NewPartitionReassignment_destroy(reassignment);
    kafka_List_destroy(replicas);
    fixture_destroy(&f);
}

/* `new NewPartitionReassignment(emptyList())` is Java's
 * IllegalArgumentException, raised by the constructor. */
static void test_mock_admin_new_partition_reassignment_rejects_empty_replicas(void) {
    kafka_List_t *empty = kafka_List_new();
    kafka_admin_NewPartitionReassignment_t *reassignment = NULL;
    kafka_common_Error_t *err = kafka_admin_NewPartitionReassignment_new(empty, &reassignment);
    TEST_ASSERT_NULL(reassignment);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "Cannot create a new partition reassignment without any replicas");
    kafka_List_destroy(empty);
}

/* Reassign to {1, 2} on a topic whose replica is {0}: the mock reports
 * replicas [0, 1, 2], adding [], removing [0]. Restricting the list to a
 * partition that is not being reassigned yields nothing; cancelling (Java's
 * `Optional.empty()`, a NULL map value) reverts it. */
static void test_mock_admin_list_partition_reassignments_round_trip(void) {
    fixture_t f;
    fixture_init(&f, 3);
    create_one(&f, "lr-topic", 2, 3);
    reassign_one(&f, "lr-topic", 0);

    kafka_admin_ListPartitionReassignmentsOptions_t *options = kafka_admin_ListPartitionReassignmentsOptions_new();
    kafka_admin_ListPartitionReassignmentsResult_t *result = kafka_admin_Admin_list_partition_reassignments_with_options(f.admin, options);
    kafka_admin_ListPartitionReassignmentsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *reassignments = kafka_admin_ListPartitionReassignmentsResult_reassignments(result);
    const kafka_Map_t *map = (const kafka_Map_t *)get_ok(reassignments);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(map));
    kafka_common_TopicPartition_t *tp0 = tp_new("lr-topic", 0);
    const kafka_admin_PartitionReassignment_t *pr = (const kafka_admin_PartitionReassignment_t *)kafka_Map_get(map, tp0);
    TEST_ASSERT_NOT_NULL(pr);
    kafka_List_t *replicas = kafka_admin_PartitionReassignment_replicas(pr);
    TEST_ASSERT_EQUAL_INT32(3, kafka_List_size(replicas));
    TEST_ASSERT_EQUAL_INT32(0, *(const int32_t *)kafka_List_get(replicas, 0));
    TEST_ASSERT_EQUAL_INT32(1, *(const int32_t *)kafka_List_get(replicas, 1));
    TEST_ASSERT_EQUAL_INT32(2, *(const int32_t *)kafka_List_get(replicas, 2));
    kafka_List_destroy(replicas);
    kafka_List_t *adding = kafka_admin_PartitionReassignment_adding_replicas(pr);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(adding));
    kafka_List_destroy(adding);
    kafka_List_t *removing = kafka_admin_PartitionReassignment_removing_replicas(pr);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(removing));
    TEST_ASSERT_EQUAL_INT32(0, *(const int32_t *)kafka_List_get(removing, 0));
    kafka_List_destroy(removing);
    char *text = kafka_admin_PartitionReassignment_to_string(pr);
    TEST_ASSERT_NOT_NULL(text);
    kafka_string_destroy(text);
    kafka_common_KafkaFuture_destroy(reassignments);
    kafka_admin_ListPartitionReassignmentsResult_destroy(result);
    kafka_admin_ListPartitionReassignmentsResult_destroy(NULL);

    /* Restricted to partition 1, which is not being reassigned. */
    kafka_common_TopicPartition_t *tp1 = tp_new("lr-topic", 1);
    kafka_List_t *only1 = kafka_List_new();
    kafka_List_add(only1, tp1);
    result = kafka_admin_Admin_list_partition_reassignments_with_partitions(f.admin, only1);
    reassignments = kafka_admin_ListPartitionReassignmentsResult_reassignments(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size((const kafka_Map_t *)get_ok(reassignments)));
    kafka_common_KafkaFuture_destroy(reassignments);
    kafka_admin_ListPartitionReassignmentsResult_destroy(result);
    kafka_List_destroy(only1);
    kafka_common_TopicPartition_destroy(tp1);

    /* Cancel: Optional.empty() is a NULL value. */
    kafka_Map_t *cancel = kafka_Map_new();
    kafka_Map_put(cancel, tp0, NULL);
    kafka_admin_AlterPartitionReassignmentsResult_t *altered = kafka_admin_Admin_alter_partition_reassignments(f.admin, cancel);
    expect_void_ok(kafka_admin_AlterPartitionReassignmentsResult_all(altered));
    kafka_admin_AlterPartitionReassignmentsResult_destroy(altered);
    kafka_Map_destroy(cancel);
    result = kafka_admin_Admin_list_partition_reassignments(f.admin);
    reassignments = kafka_admin_ListPartitionReassignmentsResult_reassignments(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size((const kafka_Map_t *)get_ok(reassignments)));
    kafka_common_KafkaFuture_destroy(reassignments);
    kafka_admin_ListPartitionReassignmentsResult_destroy(result);
    kafka_common_TopicPartition_destroy(tp0);
    fixture_destroy(&f);
}

/* A reassignment whose topic was deleted trips the mock's internal
 * consistency check (Java's IllegalStateException), failing the future. */
static void test_mock_admin_list_partition_reassignments_after_delete_returns_error(void) {
    fixture_t f;
    fixture_init(&f, 3);
    create_one(&f, "lr-gone", 1, 1);
    reassign_one(&f, "lr-gone", 0);
    kafka_List_t *names = string_list("lr-gone", NULL);
    kafka_common_TopicCollection_t *collection = kafka_common_TopicCollection_of_topic_names(names);
    kafka_admin_DeleteTopicsResult_t *deleted = kafka_admin_Admin_delete_topics(f.admin, collection);
    expect_void_ok(kafka_admin_DeleteTopicsResult_all(deleted));
    kafka_admin_DeleteTopicsResult_destroy(deleted);
    kafka_common_TopicCollection_destroy(collection);
    kafka_List_destroy(names);
    kafka_admin_ListPartitionReassignmentsResult_t *result = kafka_admin_Admin_list_partition_reassignments(f.admin);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *reassignments = kafka_admin_ListPartitionReassignmentsResult_reassignments(result);
    kafka_common_Error_t *err = get_err(reassignments);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    assert_error_message(err, "Internal MockAdminClient logic error: found reassignment for lr-gone-0, but no TopicMetadata");
    kafka_common_KafkaFuture_destroy(reassignments);
    kafka_admin_ListPartitionReassignmentsResult_destroy(result);
    fixture_destroy(&f);
}

static void test_mock_admin_alter_partition_reassignments_async(void) {
    fixture_t f;
    fixture_init(&f, 3);
    create_one(&f, "ra-async", 1, 1);
    int32_t b1 = 1;
    kafka_List_t *replicas = kafka_List_new();
    kafka_List_add(replicas, &b1);
    kafka_admin_NewPartitionReassignment_t *reassignment = NULL;
    TEST_ASSERT_NULL(kafka_admin_NewPartitionReassignment_new(replicas, &reassignment));
    kafka_common_TopicPartition_t *tp = tp_new("ra-async", 0);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, tp, reassignment);
    kafka_admin_AlterPartitionReassignmentsResult_t *result = kafka_admin_Admin_alter_partition_reassignments(f.admin, request);
    kafka_Map_t *values = kafka_admin_AlterPartitionReassignmentsResult_values(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(values, tp), &r);
    TEST_ASSERT_FALSE(r.had_error);
    kafka_Map_destroy(values);
    kafka_admin_AlterPartitionReassignmentsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_common_TopicPartition_destroy(tp);
    kafka_admin_NewPartitionReassignment_destroy(reassignment);
    kafka_List_destroy(replicas);
    fixture_destroy(&f);
}

static void test_mock_admin_list_partition_reassignments_async(void) {
    fixture_t f;
    fixture_init(&f, 3);
    create_one(&f, "lr-async", 1, 1);
    reassign_one(&f, "lr-async", 0);
    kafka_admin_ListPartitionReassignmentsResult_t *result = kafka_admin_Admin_list_partition_reassignments(f.admin);
    kafka_common_KafkaFuture_t *reassignments = kafka_admin_ListPartitionReassignmentsResult_reassignments(result);
    cb_result_t r;
    get_cb_pumped(&f, reassignments, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size((const kafka_Map_t *)r.value));
    kafka_common_KafkaFuture_destroy(reassignments);
    kafka_admin_ListPartitionReassignmentsResult_destroy(result);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// listOffsets
// ---------------------------------------------------------------------------

static void seed_lo_topic(fixture_t *f) {
    create_one(f, "lo-topic", 2, 1);
    int32_t partitions[2] = {0, 1};
    int64_t beginning[2] = {5, 7};
    int64_t end[2] = {105, 107};
    update_offsets(f, kafka_admin_MockAdminClient_update_beginning_offsets, "lo-topic", partitions, beginning, 2);
    update_offsets(f, kafka_admin_MockAdminClient_update_end_offsets, "lo-topic", partitions, end, 2);
}

/* EARLIEST reads the seeded beginning offsets, LATEST the end offsets, and
 * MAX_TIMESTAMP on an unseeded partition is -1. The mock reports timestamp
 * -1 and leader epoch -1 (`Optional.empty()`) throughout. */
static void test_mock_admin_list_offsets_earliest_and_latest(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_lo_topic(&f);
    create_one(&f, "lo-unseeded", 1, 1);
    kafka_common_TopicPartition_t *p0 = tp_new("lo-topic", 0);
    kafka_common_TopicPartition_t *p1 = tp_new("lo-topic", 1);
    kafka_common_TopicPartition_t *unseeded = tp_new("lo-unseeded", 0);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, p0, (void *)kafka_admin_OffsetSpec_earliest());
    kafka_Map_put(request, p1, (void *)kafka_admin_OffsetSpec_latest());
    kafka_Map_put(request, unseeded, (void *)kafka_admin_OffsetSpec_max_timestamp());
    kafka_admin_ListOffsetsOptions_t *options = kafka_admin_ListOffsetsOptions_with_isolation_level(kafka_common_IsolationLevel_read_committed());
    TEST_ASSERT_TRUE(kafka_admin_ListOffsetsOptions_isolation_level(options) == kafka_common_IsolationLevel_read_committed());
    kafka_admin_ListOffsetsOptions_set_timeout_ms(options, 5000);
    kafka_admin_ListOffsetsResult_t *result = kafka_admin_Admin_list_offsets_with_options(f.admin, request, options);
    kafka_admin_ListOffsetsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);

    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_partition_result(result, p0, &future));
    const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *info =
        (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *)get_ok(future);
    TEST_ASSERT_EQUAL_INT64(5, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(info));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp(info));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch(info));
    char *text = kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_to_string(info);
    TEST_ASSERT_NOT_NULL(strstr(text, "5"));
    kafka_string_destroy(text);
    kafka_common_KafkaFuture_destroy(future);
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_partition_result(result, p1, &future));
    info = (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *)get_ok(future);
    TEST_ASSERT_EQUAL_INT64(107, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(info));
    kafka_common_KafkaFuture_destroy(future);
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_partition_result(result, unseeded, &future));
    info = (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *)get_ok(future);
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(info));
    kafka_common_KafkaFuture_destroy(future);

    kafka_common_KafkaFuture_t *all = kafka_admin_ListOffsetsResult_all(result);
    const kafka_Map_t *all_map = (const kafka_Map_t *)get_ok(all);
    TEST_ASSERT_EQUAL_INT32(3, kafka_Map_size(all_map));
    info = (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *)kafka_Map_get(all_map, p1);
    TEST_ASSERT_NOT_NULL(info);
    TEST_ASSERT_EQUAL_INT64(107, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(info));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListOffsetsResult_destroy(result);
    kafka_admin_ListOffsetsResult_destroy(NULL);
    kafka_Map_destroy(request);
    kafka_common_TopicPartition_destroy(p0);
    kafka_common_TopicPartition_destroy(p1);
    kafka_common_TopicPartition_destroy(unseeded);
    fixture_destroy(&f);
}

/* `OffsetSpec.forTimestamp(ts)` is its own variant, distinct from the
 * singletons, and the mock refuses it as unsupported with Java's message
 * "Not implement yet" (sic — MockAdminClient.java:1231). */
static void test_mock_admin_list_offsets_for_timestamp_is_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_lo_topic(&f);
    kafka_admin_OffsetSpec_t *at = kafka_admin_OffsetSpec_for_timestamp(1234);
    TEST_ASSERT_EQUAL_INT(kafka_admin_OffsetSpec_e_timestamp, kafka_admin_OffsetSpec__enum(at));
    TEST_ASSERT_EQUAL_INT(kafka_admin_OffsetSpec_e_earliest, kafka_admin_OffsetSpec__enum(kafka_admin_OffsetSpec_earliest()));
    TEST_ASSERT_EQUAL_INT(kafka_admin_OffsetSpec_e_latest, kafka_admin_OffsetSpec__enum(kafka_admin_OffsetSpec_latest()));
    TEST_ASSERT_EQUAL_INT(kafka_admin_OffsetSpec_e_max_timestamp, kafka_admin_OffsetSpec__enum(kafka_admin_OffsetSpec_max_timestamp()));
    kafka_common_TopicPartition_t *p0 = tp_new("lo-topic", 0);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, p0, at);
    kafka_admin_ListOffsetsResult_t *result = kafka_admin_Admin_list_offsets(f.admin, request);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_partition_result(result, p0, &future));
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_EQUAL_INT32((int32_t)kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, (int32_t)kafka_common_Error_code(err));
    assert_error_message(err, "Not implement yet");
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_ListOffsetsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_admin_OffsetSpec_destroy(at);
    kafka_common_TopicPartition_destroy(p0);
    fixture_destroy(&f);
}

/* `IsolationLevel.forId` rejects an unknown id; `partitionResult` for a
 * partition that was not requested is Java's IllegalArgumentException,
 * delivered through the error slot. */
static void test_mock_admin_list_offsets_rejects_bad_inputs(void) {
    const kafka_common_IsolationLevel_t *level = NULL;
    TEST_ASSERT_NULL(kafka_common_IsolationLevel_for_id(0, &level));
    TEST_ASSERT_TRUE(level == kafka_common_IsolationLevel_read_uncommitted());
    TEST_ASSERT_EQUAL_INT(kafka_common_IsolationLevel_e_read_uncommitted, kafka_common_IsolationLevel__enum(level));
    TEST_ASSERT_NULL(kafka_common_IsolationLevel_for_id(1, &level));
    TEST_ASSERT_TRUE(level == kafka_common_IsolationLevel_read_committed());
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_IsolationLevel_id(level));
    level = NULL;
    kafka_common_Error_t *err = kafka_common_IsolationLevel_for_id(9, &level);
    TEST_ASSERT_NULL(level);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "Unknown isolation level 9");

    fixture_t f;
    fixture_init(&f, 1);
    seed_lo_topic(&f);
    kafka_common_TopicPartition_t *p0 = tp_new("lo-topic", 0);
    kafka_common_TopicPartition_t *p1 = tp_new("lo-topic", 1);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, p0, (void *)kafka_admin_OffsetSpec_earliest());
    kafka_admin_ListOffsetsResult_t *result = kafka_admin_Admin_list_offsets(f.admin, request);
    kafka_common_KafkaFuture_t *future = NULL;
    err = kafka_admin_ListOffsetsResult_partition_result(result, p1, &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    kafka_admin_ListOffsetsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_common_TopicPartition_destroy(p0);
    kafka_common_TopicPartition_destroy(p1);
    fixture_destroy(&f);
}

static void test_mock_admin_list_offsets_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_lo_topic(&f);
    kafka_common_TopicPartition_t *p1 = tp_new("lo-topic", 1);
    kafka_Map_t *request = kafka_Map_new();
    kafka_Map_put(request, p1, (void *)kafka_admin_OffsetSpec_earliest());
    kafka_admin_ListOffsetsResult_t *result = kafka_admin_Admin_list_offsets(f.admin, request);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_partition_result(result, p1, &future));
    cb_result_t r;
    get_cb_pumped(&f, future, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT64(7, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(
                                   (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *)r.value));
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_ListOffsetsResult_destroy(result);
    kafka_Map_destroy(request);
    kafka_common_TopicPartition_destroy(p1);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// listGroups / describeConsumerGroups / describeClassicGroups
// ---------------------------------------------------------------------------

static const kafka_admin_GroupListing_t *find_group_listing(const kafka_List_t *listings, const char *group_id) {
    for (int32_t i = 0; i < kafka_List_size(listings); i++) {
        const kafka_admin_GroupListing_t *listing = (const kafka_admin_GroupListing_t *)kafka_List_get(listings, i);
        if (strcmp(kafka_admin_GroupListing_group_id(listing), group_id) == 0) {
            return listing;
        }
    }
    return NULL;
}

/* The mock lists every group that has a GROUP config, building each listing
 * as CONSUMER / "consumer" / STABLE (MockAdminClient.java:730). `errors()`
 * is empty: the mock never reports a per-broker failure. */
static void test_mock_admin_list_groups_reports_seeded_groups(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_group(&f, "lg-a");
    seed_group(&f, "lg-b");
    kafka_admin_ListGroupsOptions_t *options = kafka_admin_ListGroupsOptions_new();
    kafka_List_t *types = kafka_List_new();
    kafka_List_add(types, (void *)kafka_common_GroupType_consumer());
    kafka_admin_ListGroupsOptions_with_types(options, types);
    kafka_List_t *states = kafka_List_new();
    kafka_List_add(states, (void *)kafka_common_GroupState_stable());
    kafka_admin_ListGroupsOptions_in_group_states(options, states);
    kafka_List_t *protocols = string_list("consumer", NULL);
    kafka_admin_ListGroupsOptions_with_protocol_types(options, protocols);
    kafka_admin_ListGroupsOptions_set_timeout_ms(options, 5000);
    kafka_List_t *copy = kafka_admin_ListGroupsOptions_types(options);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(copy));
    TEST_ASSERT_TRUE(kafka_List_get(copy, 0) == kafka_common_GroupType_consumer());
    kafka_List_destroy(copy);
    kafka_admin_ListGroupsResult_t *result = kafka_admin_Admin_list_groups_with_options(f.admin, options);
    kafka_admin_ListGroupsOptions_destroy(options);
    kafka_List_destroy(types);
    kafka_List_destroy(states);
    kafka_List_destroy(protocols);
    TEST_ASSERT_NOT_NULL(result);

    kafka_common_KafkaFuture_t *valid = kafka_admin_ListGroupsResult_valid(result);
    const kafka_List_t *listings = (const kafka_List_t *)get_ok(valid);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(listings));
    const kafka_admin_GroupListing_t *listing = find_group_listing(listings, "lg-a");
    TEST_ASSERT_NOT_NULL(listing);
    TEST_ASSERT_NOT_NULL(find_group_listing(listings, "lg-b"));
    TEST_ASSERT_NULL(find_group_listing(listings, "lg-c"));
    /* `GroupType.toString()` is "Consumer" (capitalised); the protocol type
     * is the lower-case wire string, and the two are unrelated. */
    TEST_ASSERT_TRUE(kafka_admin_GroupListing_type(listing) == kafka_common_GroupType_consumer());
    TEST_ASSERT_EQUAL_INT(kafka_common_GroupType_e_consumer, kafka_common_GroupType__enum(kafka_admin_GroupListing_type(listing)));
    char *type_text = kafka_common_GroupType_to_string(kafka_admin_GroupListing_type(listing));
    TEST_ASSERT_EQUAL_STRING("Consumer", type_text);
    kafka_string_destroy(type_text);
    TEST_ASSERT_EQUAL_STRING("consumer", kafka_admin_GroupListing_protocol(listing));
    TEST_ASSERT_TRUE(kafka_admin_GroupListing_group_state(listing) == kafka_common_GroupState_stable());
    TEST_ASSERT_EQUAL_INT(kafka_common_GroupState_e_stable, kafka_common_GroupState__enum(kafka_admin_GroupListing_group_state(listing)));
    char *state_text = kafka_common_GroupState_to_string(kafka_admin_GroupListing_group_state(listing));
    TEST_ASSERT_EQUAL_STRING("Stable", state_text);
    kafka_string_destroy(state_text);
    TEST_ASSERT_TRUE(kafka_common_GroupState_parse("Stable") == kafka_common_GroupState_stable());
    /* A CONSUMER-type group with a non-empty protocol is not simple. */
    TEST_ASSERT_FALSE(kafka_admin_GroupListing_is_simple_consumer_group(listing));
    char *text = kafka_admin_GroupListing_to_string(listing);
    TEST_ASSERT_NOT_NULL(strstr(text, "lg-a"));
    kafka_string_destroy(text);
    kafka_common_KafkaFuture_destroy(valid);

    kafka_common_KafkaFuture_t *errors = kafka_admin_ListGroupsResult_errors(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size((const kafka_List_t *)get_ok(errors)));
    kafka_common_KafkaFuture_destroy(errors);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListGroupsResult_all(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size((const kafka_List_t *)get_ok(all)));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListGroupsResult_destroy(result);
    kafka_admin_ListGroupsResult_destroy(NULL);
    fixture_destroy(&f);
}

static void test_mock_admin_list_groups_with_no_groups_is_empty(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_ListGroupsResult_t *result = kafka_admin_Admin_list_groups(f.admin);
    kafka_common_KafkaFuture_t *valid = kafka_admin_ListGroupsResult_valid(result);
    const kafka_List_t *listings = (const kafka_List_t *)get_ok(valid);
    TEST_ASSERT_NOT_NULL(listings);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(listings));
    TEST_ASSERT_NULL(kafka_List_get(listings, 0));
    kafka_common_KafkaFuture_destroy(valid);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListGroupsResult_all(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size((const kafka_List_t *)get_ok(all)));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListGroupsResult_destroy(result);
    fixture_destroy(&f);
}

static void test_mock_admin_list_groups_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_group(&f, "lg-async");
    kafka_admin_ListGroupsResult_t *result = kafka_admin_Admin_list_groups(f.admin);
    kafka_common_KafkaFuture_t *valid = kafka_admin_ListGroupsResult_valid(result);
    cb_result_t r;
    get_cb_pumped(&f, valid, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_NOT_NULL(find_group_listing((const kafka_List_t *)r.value, "lg-async"));
    kafka_common_KafkaFuture_destroy(valid);
    kafka_admin_ListGroupsResult_destroy(result);
    fixture_destroy(&f);
}

/* Java's mock throws "Not implemented yet" for `describeConsumerGroups`;
 * each group's future fails with it, keyed by group id, sorted. */
static void test_mock_admin_describe_consumer_groups_reports_unsupported_per_group(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *ids = string_list("dg-b", "dg-a", NULL);
    kafka_admin_DescribeConsumerGroupsOptions_t *options = kafka_admin_DescribeConsumerGroupsOptions_new();
    kafka_admin_DescribeConsumerGroupsOptions_set_include_authorized_operations(options, 1);
    kafka_admin_DescribeConsumerGroupsResult_t *result = kafka_admin_Admin_describe_consumer_groups_with_options(f.admin, ids, options);
    kafka_admin_DescribeConsumerGroupsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *groups = kafka_admin_DescribeConsumerGroupsResult_described_groups(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(groups));
    TEST_ASSERT_EQUAL_STRING("dg-a", (const char *)kafka_Map_key(groups, 0));
    TEST_ASSERT_EQUAL_STRING("dg-b", (const char *)kafka_Map_key(groups, 1));
    for (int32_t i = 0; i < 2; i++) {
        kafka_common_Error_t *err = get_err((const kafka_common_KafkaFuture_t *)kafka_Map_value(groups, i));
        TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
        assert_error_message(err, "Not implemented yet");
    }
    kafka_Map_destroy(groups);
    expect_fails_with(kafka_admin_DescribeConsumerGroupsResult_all(result), "Not implemented yet");
    kafka_admin_DescribeConsumerGroupsResult_destroy(result);
    kafka_admin_DescribeConsumerGroupsResult_destroy(NULL);
    kafka_List_destroy(ids);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_classic_groups_reports_unsupported_per_group(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *ids = string_list("cg-a", NULL);
    kafka_admin_DescribeClassicGroupsResult_t *result = kafka_admin_Admin_describe_classic_groups(f.admin, ids);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *groups = kafka_admin_DescribeClassicGroupsResult_described_groups(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(groups));
    assert_error_message(get_err(future_for(groups, "cg-a")), "Not implemented yet");
    kafka_Map_destroy(groups);
    expect_fails_with(kafka_admin_DescribeClassicGroupsResult_all(result), "Not implemented yet");
    kafka_admin_DescribeClassicGroupsResult_destroy(result);
    kafka_admin_DescribeClassicGroupsResult_destroy(NULL);
    kafka_List_destroy(ids);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_consumer_groups_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *ids = string_list("dg-async", NULL);
    kafka_admin_DescribeConsumerGroupsResult_t *result = kafka_admin_Admin_describe_consumer_groups(f.admin, ids);
    kafka_Map_t *groups = kafka_admin_DescribeConsumerGroupsResult_described_groups(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(groups, "dg-async"), &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_Map_destroy(groups);
    kafka_admin_DescribeConsumerGroupsResult_destroy(result);
    kafka_List_destroy(ids);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// listConsumerGroupOffsets
// ---------------------------------------------------------------------------

/* Seeds (og-a, 0) -> 17 and (og-b, 1) -> 23 in the mock's one committed
 * offsets map (`updateConsumerGroupOffsets` is an unvalidated `putAll`). */
static void seed_group_offsets(fixture_t *f, const char *topic_a, const char *topic_b, int64_t a0, int64_t b1) {
    int32_t pa[1] = {0};
    int64_t oa[1] = {a0};
    update_offsets(f, kafka_admin_MockAdminClient_update_consumer_group_offsets, topic_a, pa, oa, 1);
    int32_t pb[1] = {1};
    int64_t ob[1] = {b1};
    update_offsets(f, kafka_admin_MockAdminClient_update_consumer_group_offsets, topic_b, pb, ob, 1);
}

/* Listing a single group returns every seeded offset as an
 * `OffsetAndMetadata`; Java's one-argument constructor normalises the
 * metadata to "" and the leader epoch to `Optional.empty()` (-1 in C). */
static void test_mock_admin_list_consumer_group_offsets_round_trip(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_group_offsets(&f, "og-a", "og-b", 17, 23);
    kafka_admin_ListConsumerGroupOffsetsOptions_t *options = kafka_admin_ListConsumerGroupOffsetsOptions_new();
    kafka_admin_ListConsumerGroupOffsetsOptions_set_timeout_ms(options, 5000);
    kafka_admin_ListConsumerGroupOffsetsResult_t *result =
        kafka_admin_Admin_list_consumer_group_offsets_with_group_id_options(f.admin, "og-group", options);
    kafka_admin_ListConsumerGroupOffsetsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata(result, &future));
    const kafka_Map_t *offsets = (const kafka_Map_t *)get_ok(future);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(offsets));
    kafka_common_TopicPartition_t *a0 = tp_new("og-a", 0);
    const kafka_consumer_OffsetAndMetadata_t *om = (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_get(offsets, a0);
    TEST_ASSERT_NOT_NULL(om);
    TEST_ASSERT_EQUAL_INT64(17, kafka_consumer_OffsetAndMetadata_offset(om));
    TEST_ASSERT_EQUAL_STRING("", kafka_consumer_OffsetAndMetadata_metadata(om));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_consumer_OffsetAndMetadata_leader_epoch(om));
    kafka_common_TopicPartition_destroy(a0);
    kafka_common_TopicPartition_t *b1 = tp_new("og-b", 1);
    om = (const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_get(offsets, b1);
    TEST_ASSERT_NOT_NULL(om);
    TEST_ASSERT_EQUAL_INT64(23, kafka_consumer_OffsetAndMetadata_offset(om));
    kafka_common_TopicPartition_destroy(b1);
    kafka_common_KafkaFuture_t *by_group = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(result, "og-group", &by_group));
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size((const kafka_Map_t *)get_ok(by_group)));
    kafka_common_KafkaFuture_destroy(by_group);
    kafka_common_KafkaFuture_destroy(future);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListConsumerGroupOffsetsResult_all(result);
    const kafka_Map_t *all_map = (const kafka_Map_t *)get_ok(all);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(all_map));
    TEST_ASSERT_EQUAL_STRING("og-group", (const char *)kafka_Map_key(all_map, 0));
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size((const kafka_Map_t *)kafka_Map_value(all_map, 0)));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(NULL);
    fixture_destroy(&f);
}

/* A `ListConsumerGroupOffsetsSpec` with topic partitions narrows the result
 * to those partitions — which fails if the selection were ignored. */
static void test_mock_admin_list_consumer_group_offsets_filters_by_spec_partitions(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_group_offsets(&f, "og-a", "og-b", 17, 23);
    kafka_common_TopicPartition_t *b1 = tp_new("og-b", 1);
    kafka_List_t *partitions = kafka_List_new();
    kafka_List_add(partitions, b1);
    kafka_admin_ListConsumerGroupOffsetsSpec_t *spec = kafka_admin_ListConsumerGroupOffsetsSpec_new();
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsSpec_topic_partitions(spec)); /* null = all */
    kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions(spec, partitions);
    kafka_List_t *copy = kafka_admin_ListConsumerGroupOffsetsSpec_topic_partitions(spec);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(copy));
    TEST_ASSERT_EQUAL_STRING("og-b", kafka_common_TopicPartition_topic((const kafka_common_TopicPartition_t *)kafka_List_get(copy, 0)));
    kafka_List_destroy(copy);
    kafka_Map_t *specs = kafka_Map_new();
    kafka_Map_put(specs, (void *)"og-group", spec);
    kafka_admin_ListConsumerGroupOffsetsResult_t *result = kafka_admin_Admin_list_consumer_group_offsets_with_group_specs(f.admin, specs);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(result, "og-group", &future));
    const kafka_Map_t *offsets = (const kafka_Map_t *)get_ok(future);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(offsets));
    const kafka_common_TopicPartition_t *key = (const kafka_common_TopicPartition_t *)kafka_Map_key(offsets, 0);
    TEST_ASSERT_EQUAL_STRING("og-b", kafka_common_TopicPartition_topic(key));
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_TopicPartition_partition(key));
    TEST_ASSERT_EQUAL_INT64(23, kafka_consumer_OffsetAndMetadata_offset((const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_value(offsets, 0)));
    kafka_common_KafkaFuture_destroy(future);
    /* A group that was not requested is Java's IllegalArgumentException. */
    future = NULL;
    kafka_common_Error_t *err = kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(result, "other-group", &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);
    kafka_Map_destroy(specs);
    kafka_admin_ListConsumerGroupOffsetsSpec_destroy(spec);
    kafka_List_destroy(partitions);
    kafka_common_TopicPartition_destroy(b1);
    fixture_destroy(&f);
}

/* `updateConsumerGroupOffsets` does not validate, so -1 — Kafka's own
 * invalid-offset sentinel — is seedable. Listing then has to build an
 * `OffsetAndMetadata` from it, which Java rejects with
 * IllegalArgumentException("Invalid negative offset")
 * (MockAdminClient.java:756, OffsetAndMetadata.java:49-50); the Rust mock
 * fails the group's future rather than panicking, and the handle stays
 * usable: overwriting the bad offset makes the same call succeed. */
static void test_mock_admin_list_consumer_group_offsets_rejects_a_negative_seeded_offset(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_group_offsets(&f, "neg-a", "neg-b", 3, -1);
    kafka_admin_ListConsumerGroupOffsetsResult_t *result = kafka_admin_Admin_list_consumer_group_offsets_with_group_id(f.admin, "neg-group");
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata(result, &future));
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "Invalid negative offset");
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);

    int32_t pb[1] = {1};
    int64_t ob[1] = {9};
    update_offsets(&f, kafka_admin_MockAdminClient_update_consumer_group_offsets, "neg-b", pb, ob, 1);
    result = kafka_admin_Admin_list_consumer_group_offsets_with_group_id(f.admin, "neg-group");
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata(result, &future));
    const kafka_Map_t *offsets = (const kafka_Map_t *)get_ok(future);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(offsets));
    kafka_common_TopicPartition_t *b1 = tp_new("neg-b", 1);
    TEST_ASSERT_EQUAL_INT64(9, kafka_consumer_OffsetAndMetadata_offset((const kafka_consumer_OffsetAndMetadata_t *)kafka_Map_get(offsets, b1)));
    kafka_common_TopicPartition_destroy(b1);
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);
    fixture_destroy(&f);
}

/* `listConsumerGroupOffsets(Map)` with two groups is "Not implemented yet"
 * on the mock (`groupSpecs.size() != 1`), per group; and the one-group
 * accessor `partitionsToOffsetAndMetadata()` is Java's
 * IllegalStateException when the result holds two. */
static void test_mock_admin_list_consumer_group_offsets_two_groups_are_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_ListConsumerGroupOffsetsSpec_t *spec_a = kafka_admin_ListConsumerGroupOffsetsSpec_new();
    kafka_admin_ListConsumerGroupOffsetsSpec_t *spec_b = kafka_admin_ListConsumerGroupOffsetsSpec_new();
    kafka_Map_t *specs = kafka_Map_new();
    kafka_Map_put(specs, (void *)"two-a", spec_a);
    kafka_Map_put(specs, (void *)"two-b", spec_b);
    kafka_admin_ListConsumerGroupOffsetsOptions_t *options = kafka_admin_ListConsumerGroupOffsetsOptions_new();
    kafka_admin_ListConsumerGroupOffsetsResult_t *result =
        kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options(f.admin, specs, options);
    kafka_admin_ListConsumerGroupOffsetsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata(result, &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_state_error(err));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(result, "two-a", &future));
    expect_fails_with(future, "Not implemented yet");
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(result, "two-b", &future));
    expect_fails_with(future, "Not implemented yet");
    expect_fails_with(kafka_admin_ListConsumerGroupOffsetsResult_all(result), "Not implemented yet");
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);
    kafka_Map_destroy(specs);
    kafka_admin_ListConsumerGroupOffsetsSpec_destroy(spec_a);
    kafka_admin_ListConsumerGroupOffsetsSpec_destroy(spec_b);
    fixture_destroy(&f);
}

static void test_mock_admin_list_consumer_group_offsets_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_group_offsets(&f, "oga-a", "oga-b", 1, 2);
    kafka_admin_ListConsumerGroupOffsetsResult_t *result = kafka_admin_Admin_list_consumer_group_offsets_with_group_id(f.admin, "oga-group");
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata(result, &future));
    cb_result_t r;
    get_cb_pumped(&f, future, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size((const kafka_Map_t *)r.value));
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// alterConsumerGroupOffsets / deleteConsumerGroupOffsets / deleteConsumerGroups
// ---------------------------------------------------------------------------

/* Java's mock throws "Not implement yet" (sic) for `alterConsumerGroupOffsets`;
 * the Rust mock fails each partition's future, reachable through
 * `partitionResult(tp)`, which returns the future directly. */
static void test_mock_admin_alter_consumer_group_offsets_reports_unsupported_per_partition(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_TopicPartition_t *p0 = tp_new("acg-topic", 0);
    kafka_common_TopicPartition_t *p1 = tp_new("acg-topic", 1);
    kafka_consumer_OffsetAndMetadata_t *om0 = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_new(10, &om0));
    kafka_consumer_OffsetAndMetadata_t *om1 = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(20, 3, "meta", &om1));
    TEST_ASSERT_EQUAL_INT64(20, kafka_consumer_OffsetAndMetadata_offset(om1));
    TEST_ASSERT_EQUAL_INT32(3, kafka_consumer_OffsetAndMetadata_leader_epoch(om1));
    TEST_ASSERT_EQUAL_STRING("meta", kafka_consumer_OffsetAndMetadata_metadata(om1));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, p0, om0);
    kafka_Map_put(offsets, p1, om1);
    kafka_admin_AlterConsumerGroupOffsetsOptions_t *options = kafka_admin_AlterConsumerGroupOffsetsOptions_new();
    kafka_admin_AlterConsumerGroupOffsetsResult_t *result =
        kafka_admin_Admin_alter_consumer_group_offsets_with_options(f.admin, "acg", offsets, options);
    kafka_admin_AlterConsumerGroupOffsetsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = kafka_admin_AlterConsumerGroupOffsetsResult_partition_result(result, p0);
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implement yet");
    kafka_common_KafkaFuture_destroy(future);
    expect_fails_with(kafka_admin_AlterConsumerGroupOffsetsResult_partition_result(result, p1), "Not implement yet");
    expect_fails_with(kafka_admin_AlterConsumerGroupOffsetsResult_all(result), "Not implement yet");
    kafka_admin_AlterConsumerGroupOffsetsResult_destroy(result);
    kafka_admin_AlterConsumerGroupOffsetsResult_destroy(NULL);
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(om0);
    kafka_consumer_OffsetAndMetadata_destroy(om1);
    kafka_common_TopicPartition_destroy(p0);
    kafka_common_TopicPartition_destroy(p1);
    fixture_destroy(&f);
}

/* `new OffsetAndMetadata(-1)` is Java's IllegalArgumentException. On the
 * mock, `partitionResult` for a partition that was not requested cannot
 * report Java's IllegalArgumentException: the whole call failed first, so
 * the derived future carries that failure (as Java's `thenApply` chain
 * would). */
static void test_mock_admin_alter_consumer_group_offsets_rejects_bad_input(void) {
    kafka_consumer_OffsetAndMetadata_t *om = NULL;
    kafka_common_Error_t *err = kafka_consumer_OffsetAndMetadata_new(-1, &om);
    TEST_ASSERT_NULL(om);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "Invalid negative offset");

    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_TopicPartition_t *requested = tp_new("acg-topic", 0);
    kafka_common_TopicPartition_t *other = tp_new("acg-topic", 5);
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_new(1, &om));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, requested, om);
    kafka_admin_AlterConsumerGroupOffsetsResult_t *result = kafka_admin_Admin_alter_consumer_group_offsets(f.admin, "acg", offsets);
    kafka_common_KafkaFuture_t *future = kafka_admin_AlterConsumerGroupOffsetsResult_partition_result(result, other);
    TEST_ASSERT_NOT_NULL(future);
    expect_fails_with(future, "Not implement yet");
    kafka_admin_AlterConsumerGroupOffsetsResult_destroy(result);
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(om);
    kafka_common_TopicPartition_destroy(requested);
    kafka_common_TopicPartition_destroy(other);
    fixture_destroy(&f);
}

/* With no requested partition there is no per-key slot for the outcome, so
 * Java's `all()` is the only observable, and it carries the mock's
 * refusal. */
static void test_mock_admin_alter_consumer_group_offsets_with_no_partitions_fails_the_call(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_admin_AlterConsumerGroupOffsetsResult_t *result = kafka_admin_Admin_alter_consumer_group_offsets(f.admin, "acg", offsets);
    TEST_ASSERT_NOT_NULL(result);
    expect_fails_with(kafka_admin_AlterConsumerGroupOffsetsResult_all(result), "Not implement yet");
    kafka_admin_AlterConsumerGroupOffsetsResult_destroy(result);
    kafka_Map_destroy(offsets);
    fixture_destroy(&f);
}

static void test_mock_admin_alter_consumer_group_offsets_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_TopicPartition_t *p0 = tp_new("acg-async", 0);
    kafka_consumer_OffsetAndMetadata_t *om = NULL;
    TEST_ASSERT_NULL(kafka_consumer_OffsetAndMetadata_with_metadata(7, "m", &om));
    kafka_Map_t *offsets = kafka_Map_new();
    kafka_Map_put(offsets, p0, om);
    kafka_admin_AlterConsumerGroupOffsetsResult_t *result = kafka_admin_Admin_alter_consumer_group_offsets(f.admin, "acg", offsets);
    kafka_common_KafkaFuture_t *future = kafka_admin_AlterConsumerGroupOffsetsResult_partition_result(result, p0);
    cb_result_t r;
    get_cb_pumped(&f, future, &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implement yet", r.message);
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_AlterConsumerGroupOffsetsResult_destroy(result);
    kafka_Map_destroy(offsets);
    kafka_consumer_OffsetAndMetadata_destroy(om);
    kafka_common_TopicPartition_destroy(p0);
    fixture_destroy(&f);
}

/* `deleteConsumerGroupOffsets`: "Not implemented yet" per partition, through
 * `partitionResult(tp)`, which delivers the future through an out slot and
 * reports IllegalArgument for an unrequested partition. */
static void test_mock_admin_delete_consumer_group_offsets_reports_unsupported_per_partition(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_TopicPartition_t *p0 = tp_new("dcg-topic", 0);
    kafka_common_TopicPartition_t *other = tp_new("dcg-topic", 9);
    kafka_List_t *partitions = kafka_List_new();
    kafka_List_add(partitions, p0);
    kafka_admin_DeleteConsumerGroupOffsetsOptions_t *options = kafka_admin_DeleteConsumerGroupOffsetsOptions_new();
    kafka_admin_DeleteConsumerGroupOffsetsResult_t *result =
        kafka_admin_Admin_delete_consumer_group_offsets_with_options(f.admin, "dcg", partitions, options);
    kafka_admin_DeleteConsumerGroupOffsetsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result(result, p0, &future));
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_common_KafkaFuture_destroy(future);
    future = NULL;
    err = kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result(result, other, &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    expect_fails_with(kafka_admin_DeleteConsumerGroupOffsetsResult_all(result), "Not implemented yet");
    kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(result);
    kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(NULL);
    kafka_List_destroy(partitions);
    kafka_common_TopicPartition_destroy(p0);
    kafka_common_TopicPartition_destroy(other);
    fixture_destroy(&f);
}

static void test_mock_admin_delete_consumer_group_offsets_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_TopicPartition_t *p0 = tp_new("dcg-async", 0);
    kafka_List_t *partitions = kafka_List_new();
    kafka_List_add(partitions, p0);
    kafka_admin_DeleteConsumerGroupOffsetsResult_t *result = kafka_admin_Admin_delete_consumer_group_offsets(f.admin, "dcg", partitions);
    kafka_common_KafkaFuture_t *all = kafka_admin_DeleteConsumerGroupOffsetsResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(result);
    kafka_List_destroy(partitions);
    kafka_common_TopicPartition_destroy(p0);
    fixture_destroy(&f);
}

/* `deleteConsumerGroups`: "Not implemented yet" per group id, sorted. */
static void test_mock_admin_delete_consumer_groups_reports_unsupported_per_group(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *ids = string_list("del-g-b", "del-g-a", NULL);
    kafka_admin_DeleteConsumerGroupsOptions_t *options = kafka_admin_DeleteConsumerGroupsOptions_new();
    kafka_admin_DeleteConsumerGroupsResult_t *result = kafka_admin_Admin_delete_consumer_groups_with_options(f.admin, ids, options);
    kafka_admin_DeleteConsumerGroupsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *groups = kafka_admin_DeleteConsumerGroupsResult_deleted_groups(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(groups));
    TEST_ASSERT_EQUAL_STRING("del-g-a", (const char *)kafka_Map_key(groups, 0));
    TEST_ASSERT_EQUAL_STRING("del-g-b", (const char *)kafka_Map_key(groups, 1));
    assert_error_message(get_err(future_for(groups, "del-g-a")), "Not implemented yet");
    assert_error_message(get_err(future_for(groups, "del-g-b")), "Not implemented yet");
    kafka_Map_destroy(groups);
    expect_fails_with(kafka_admin_DeleteConsumerGroupsResult_all(result), "Not implemented yet");
    kafka_admin_DeleteConsumerGroupsResult_destroy(result);
    kafka_admin_DeleteConsumerGroupsResult_destroy(NULL);
    kafka_List_destroy(ids);
    fixture_destroy(&f);
}

static void test_mock_admin_delete_consumer_groups_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *ids = string_list("del-g-async", NULL);
    kafka_admin_DeleteConsumerGroupsResult_t *result = kafka_admin_Admin_delete_consumer_groups(f.admin, ids);
    kafka_Map_t *groups = kafka_admin_DeleteConsumerGroupsResult_deleted_groups(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(groups, "del-g-async"), &r);
    TEST_ASSERT_TRUE(r.had_error);
    kafka_Map_destroy(groups);
    kafka_admin_DeleteConsumerGroupsResult_destroy(result);
    kafka_List_destroy(ids);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// removeMembersFromConsumerGroup
// ---------------------------------------------------------------------------

/* `memberResult(member)` for each requested member fails "Not implemented
 * yet"; an unrequested member is Java's IllegalArgumentException. */
static void test_mock_admin_remove_members_reports_unsupported_per_member(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_MemberToRemove_t *b = kafka_admin_MemberToRemove_new("instance-b");
    kafka_admin_MemberToRemove_t *a = kafka_admin_MemberToRemove_new("instance-a");
    kafka_admin_MemberToRemove_t *other = kafka_admin_MemberToRemove_new("instance-z");
    TEST_ASSERT_EQUAL_STRING("instance-a", kafka_admin_MemberToRemove_group_instance_id(a));
    kafka_List_t *members = kafka_List_new();
    kafka_List_add(members, b);
    kafka_List_add(members, a);
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t *options = NULL;
    TEST_ASSERT_NULL(kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(members, &options));
    kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason(options, "rolling restart");
    TEST_ASSERT_EQUAL_STRING("rolling restart", kafka_admin_RemoveMembersFromConsumerGroupOptions_reason(options));
    TEST_ASSERT_FALSE(kafka_admin_RemoveMembersFromConsumerGroupOptions_remove_all(options));
    kafka_List_t *copy = kafka_admin_RemoveMembersFromConsumerGroupOptions_members(options);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(copy));
    kafka_List_destroy(copy);
    kafka_admin_RemoveMembersFromConsumerGroupResult_t *result =
        kafka_admin_Admin_remove_members_from_consumer_group_with_options(f.admin, "rm-group", options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(result, a, &future));
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_common_KafkaFuture_destroy(future);
    TEST_ASSERT_NULL(kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(result, b, &future));
    expect_fails_with(future, "Not implemented yet");
    future = NULL;
    err = kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(result, other, &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    expect_fails_with(kafka_admin_RemoveMembersFromConsumerGroupResult_all(result), "Not implemented yet");
    kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(result);
    kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(NULL);
    kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy(options);
    kafka_List_destroy(members);
    kafka_admin_MemberToRemove_destroy(a);
    kafka_admin_MemberToRemove_destroy(b);
    kafka_admin_MemberToRemove_destroy(other);
    fixture_destroy(&f);
}

/* `new RemoveMembersFromConsumerGroupOptions()` is Java's removeAll mode:
 * `removeAll()` is true, `members()` is empty, and `memberResult(member)` is
 * Java's IllegalArgumentException ("not applicable in 'removeAll' mode");
 * the only outcome is `all()`. */
static void test_mock_admin_remove_all_members_has_no_per_member_outcome(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t *options =
        kafka_admin_RemoveMembersFromConsumerGroupOptions_new();
    TEST_ASSERT_TRUE(kafka_admin_RemoveMembersFromConsumerGroupOptions_remove_all(options));
    kafka_List_t *none = kafka_admin_RemoveMembersFromConsumerGroupOptions_members(options);
    TEST_ASSERT_EQUAL_INT32(0, kafka_List_size(none));
    kafka_List_destroy(none);
    kafka_admin_RemoveMembersFromConsumerGroupResult_t *result =
        kafka_admin_Admin_remove_members_from_consumer_group_with_options(f.admin, "rm-group", options);
    kafka_admin_MemberToRemove_t *m = kafka_admin_MemberToRemove_new("instance-a");
    kafka_common_KafkaFuture_t *future = NULL;
    kafka_common_Error_t *err = kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(result, m, &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "The method: memberResult is not applicable in 'removeAll' mode");
    expect_fails_with(kafka_admin_RemoveMembersFromConsumerGroupResult_all(result), "Not implemented yet");
    kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(result);
    kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy(options);
    kafka_admin_MemberToRemove_destroy(m);
    fixture_destroy(&f);
}

/* Java's `RemoveMembersFromConsumerGroupOptions(Collection)` rejects an
 * empty collection. */
static void test_mock_admin_remove_members_rejects_an_empty_member_list(void) {
    kafka_List_t *empty = kafka_List_new();
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t *options = NULL;
    kafka_common_Error_t *err = kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(empty, &options);
    TEST_ASSERT_NULL(options);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "Invalid empty members has been provided");
    err = kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(NULL, &options);
    TEST_ASSERT_NULL(options);
    assert_error_message(err, "Invalid empty members has been provided");
    kafka_List_destroy(empty);
}

static void test_mock_admin_remove_members_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_MemberToRemove_t *m = kafka_admin_MemberToRemove_new("instance-async");
    kafka_List_t *members = kafka_List_new();
    kafka_List_add(members, m);
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t *options = NULL;
    TEST_ASSERT_NULL(kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(members, &options));
    kafka_admin_RemoveMembersFromConsumerGroupResult_t *result =
        kafka_admin_Admin_remove_members_from_consumer_group_with_options(f.admin, "rm-group", options);
    kafka_common_KafkaFuture_t *all = kafka_admin_RemoveMembersFromConsumerGroupResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(result);
    kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy(options);
    kafka_List_destroy(members);
    kafka_admin_MemberToRemove_destroy(m);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// ACLs
// ---------------------------------------------------------------------------

static kafka_common_resource_ResourcePattern_t *topic_pattern(const char *name) {
    kafka_common_resource_ResourcePattern_t *pattern = NULL;
    TEST_ASSERT_NULL(kafka_common_resource_ResourcePattern_new(
        kafka_common_resource_ResourceType_topic(), name, kafka_common_resource_PatternType_literal(), &pattern));
    TEST_ASSERT_NOT_NULL(pattern);
    return pattern;
}

static kafka_common_acl_AccessControlEntry_t *allow_read_entry(const char *principal) {
    kafka_common_acl_AccessControlEntry_t *entry = NULL;
    TEST_ASSERT_NULL(kafka_common_acl_AccessControlEntry_new(
        principal, "*", kafka_common_acl_AclOperation_read(), kafka_common_acl_AclPermissionType_allow(), &entry));
    TEST_ASSERT_NOT_NULL(entry);
    return entry;
}

/* Java's mock throws "Not implemented yet" for `createAcls`; each binding's
 * future fails with it, keyed by the binding, found by value. */
static void test_mock_admin_create_acls_reports_unsupported_per_binding(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_resource_ResourcePattern_t *pattern = topic_pattern("acl-topic");
    TEST_ASSERT_EQUAL_STRING("acl-topic", kafka_common_resource_ResourcePattern_name(pattern));
    TEST_ASSERT_TRUE(kafka_common_resource_ResourcePattern_resource_type(pattern) == kafka_common_resource_ResourceType_topic());
    TEST_ASSERT_EQUAL_INT(kafka_common_resource_PatternType_e_literal,
                          kafka_common_resource_PatternType__enum(kafka_common_resource_ResourcePattern_pattern_type(pattern)));
    kafka_common_acl_AccessControlEntry_t *entry = allow_read_entry("User:alice");
    TEST_ASSERT_EQUAL_STRING("User:alice", kafka_common_acl_AccessControlEntry_principal(entry));
    TEST_ASSERT_EQUAL_STRING("*", kafka_common_acl_AccessControlEntry_host(entry));
    TEST_ASSERT_EQUAL_INT(kafka_common_acl_AclOperation_e_read, kafka_common_acl_AclOperation__enum(kafka_common_acl_AccessControlEntry_operation(entry)));
    TEST_ASSERT_EQUAL_INT(kafka_common_acl_AclPermissionType_e_allow,
                          kafka_common_acl_AclPermissionType__enum(kafka_common_acl_AccessControlEntry_permission_type(entry)));
    kafka_common_acl_AclBinding_t *binding = kafka_common_acl_AclBinding_new(pattern, entry);
    TEST_ASSERT_NOT_NULL(binding);
    TEST_ASSERT_EQUAL_STRING("acl-topic", kafka_common_resource_ResourcePattern_name(kafka_common_acl_AclBinding_pattern(binding)));
    TEST_ASSERT_EQUAL_STRING("User:alice", kafka_common_acl_AccessControlEntry_principal(kafka_common_acl_AclBinding_entry(binding)));
    char *text = kafka_common_acl_AclBinding_to_string(binding);
    TEST_ASSERT_NOT_NULL(strstr(text, "acl-topic"));
    kafka_string_destroy(text);
    kafka_List_t *acls = kafka_List_new();
    kafka_List_add(acls, binding);
    kafka_admin_CreateAclsOptions_t *options = kafka_admin_CreateAclsOptions_new();
    kafka_admin_CreateAclsOptions_set_timeout_ms(options, 5000);
    kafka_admin_CreateAclsResult_t *result = kafka_admin_Admin_create_acls_with_options(f.admin, acls, options);
    kafka_admin_CreateAclsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_CreateAclsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    kafka_common_Error_t *err = get_err(future_for(values, binding));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_Map_destroy(values);
    expect_fails_with(kafka_admin_CreateAclsResult_all(result), "Not implemented yet");
    kafka_admin_CreateAclsResult_destroy(result);
    kafka_admin_CreateAclsResult_destroy(NULL);
    kafka_List_destroy(acls);
    kafka_common_acl_AclBinding_destroy(binding);
    kafka_common_acl_AccessControlEntry_destroy(entry);
    kafka_common_resource_ResourcePattern_destroy(pattern);
    fixture_destroy(&f);
}

/* The Java constructors reject the filter-only enum values (`ANY`, `MATCH`)
 * on a concrete pattern or entry; each is an IllegalArgumentException. */
static void test_mock_admin_acl_constructors_reject_filter_values(void) {
    kafka_common_resource_ResourcePattern_t *pattern = NULL;
    kafka_common_Error_t *err = kafka_common_resource_ResourcePattern_new(
        kafka_common_resource_ResourceType_any(), "t", kafka_common_resource_PatternType_literal(), &pattern);
    TEST_ASSERT_NULL(pattern);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "resourceType must not be ANY");
    err = kafka_common_resource_ResourcePattern_new(
        kafka_common_resource_ResourceType_topic(), "t", kafka_common_resource_PatternType_match(), &pattern);
    TEST_ASSERT_NULL(pattern);
    assert_error_message(err, "patternType must not be MATCH");
    err = kafka_common_resource_ResourcePattern_new(
        kafka_common_resource_ResourceType_topic(), "t", kafka_common_resource_PatternType_any(), &pattern);
    TEST_ASSERT_NULL(pattern);
    assert_error_message(err, "patternType must not be ANY");

    kafka_common_acl_AccessControlEntry_t *entry = NULL;
    err = kafka_common_acl_AccessControlEntry_new(
        "User:a", "*", kafka_common_acl_AclOperation_any(), kafka_common_acl_AclPermissionType_allow(), &entry);
    TEST_ASSERT_NULL(entry);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "operation must not be ANY");
    err = kafka_common_acl_AccessControlEntry_new(
        "User:a", "*", kafka_common_acl_AclOperation_read(), kafka_common_acl_AclPermissionType_any(), &entry);
    TEST_ASSERT_NULL(entry);
    assert_error_message(err, "permissionType must not be ANY");
}

static void test_mock_admin_create_acls_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_resource_ResourcePattern_t *pattern = topic_pattern("acl-async");
    kafka_common_acl_AccessControlEntry_t *entry = allow_read_entry("User:bob");
    kafka_common_acl_AclBinding_t *binding = kafka_common_acl_AclBinding_new(pattern, entry);
    kafka_List_t *acls = kafka_List_new();
    kafka_List_add(acls, binding);
    kafka_admin_CreateAclsResult_t *result = kafka_admin_Admin_create_acls(f.admin, acls);
    kafka_common_KafkaFuture_t *all = kafka_admin_CreateAclsResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_CreateAclsResult_destroy(result);
    kafka_List_destroy(acls);
    kafka_common_acl_AclBinding_destroy(binding);
    kafka_common_acl_AccessControlEntry_destroy(entry);
    kafka_common_resource_ResourcePattern_destroy(pattern);
    fixture_destroy(&f);
}

/* `describeAcls` is "Not implemented yet" on the mock whatever the filter;
 * `values()` is the result's one future, borrowed from the result. */
static void test_mock_admin_describe_acls_reports_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_acl_AclBindingFilter_t *any = kafka_common_acl_AclBindingFilter_any();
    TEST_ASSERT_NOT_NULL(any);
    TEST_ASSERT_EQUAL_INT(kafka_common_resource_ResourceType_e_any,
                          kafka_common_resource_ResourceType__enum(kafka_common_resource_ResourcePatternFilter_resource_type(
                              kafka_common_acl_AclBindingFilter_pattern_filter(any))));
    kafka_admin_DescribeAclsResult_t *result = kafka_admin_Admin_describe_acls(f.admin, any);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_Error_t *err = get_err(kafka_admin_DescribeAclsResult_values(result));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_admin_DescribeAclsResult_destroy(result);
    kafka_admin_DescribeAclsResult_destroy(NULL);

    kafka_common_resource_ResourcePatternFilter_t *pf = kafka_common_resource_ResourcePatternFilter_new(
        kafka_common_resource_ResourceType_topic(), "acl-topic", kafka_common_resource_PatternType_match());
    kafka_common_acl_AccessControlEntryFilter_t *ef = kafka_common_acl_AccessControlEntryFilter_new(
        "User:alice", NULL, kafka_common_acl_AclOperation_any(), kafka_common_acl_AclPermissionType_any());
    TEST_ASSERT_NULL(kafka_common_acl_AccessControlEntryFilter_host(ef));
    kafka_common_acl_AclBindingFilter_t *filter = kafka_common_acl_AclBindingFilter_new(pf, ef);
    TEST_ASSERT_EQUAL_STRING("acl-topic", kafka_common_resource_ResourcePatternFilter_name(kafka_common_acl_AclBindingFilter_pattern_filter(filter)));
    TEST_ASSERT_EQUAL_STRING("User:alice", kafka_common_acl_AccessControlEntryFilter_principal(kafka_common_acl_AclBindingFilter_entry_filter(filter)));
    kafka_admin_DescribeAclsOptions_t *options = kafka_admin_DescribeAclsOptions_new();
    result = kafka_admin_Admin_describe_acls_with_options(f.admin, filter, options);
    kafka_admin_DescribeAclsOptions_destroy(options);
    assert_error_message(get_err(kafka_admin_DescribeAclsResult_values(result)), "Not implemented yet");
    kafka_admin_DescribeAclsResult_destroy(result);
    kafka_common_acl_AclBindingFilter_destroy(filter);
    kafka_common_acl_AccessControlEntryFilter_destroy(ef);
    kafka_common_resource_ResourcePatternFilter_destroy(pf);
    kafka_common_acl_AclBindingFilter_destroy(any);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_acls_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_acl_AclBindingFilter_t *any = kafka_common_acl_AclBindingFilter_any();
    kafka_admin_DescribeAclsResult_t *result = kafka_admin_Admin_describe_acls(f.admin, any);
    cb_result_t r;
    get_cb_pumped(&f, kafka_admin_DescribeAclsResult_values(result), &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_admin_DescribeAclsResult_destroy(result);
    kafka_common_acl_AclBindingFilter_destroy(any);
    fixture_destroy(&f);
}

/* `deleteAcls`: "Not implemented yet" per filter, keyed by the filter. */
static void test_mock_admin_delete_acls_reports_unsupported_per_filter(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_acl_AclBindingFilter_t *any = kafka_common_acl_AclBindingFilter_any();
    kafka_common_resource_ResourcePatternFilter_t *pf = kafka_common_resource_ResourcePatternFilter_new(
        kafka_common_resource_ResourceType_group(), "g", kafka_common_resource_PatternType_literal());
    kafka_common_acl_AccessControlEntryFilter_t *ef = kafka_common_acl_AccessControlEntryFilter_any();
    kafka_common_acl_AclBindingFilter_t *specific = kafka_common_acl_AclBindingFilter_new(pf, ef);
    kafka_List_t *filters = kafka_List_new();
    kafka_List_add(filters, any);
    kafka_List_add(filters, specific);
    kafka_admin_DeleteAclsOptions_t *options = kafka_admin_DeleteAclsOptions_new();
    kafka_admin_DeleteAclsResult_t *result = kafka_admin_Admin_delete_acls_with_options(f.admin, filters, options);
    kafka_admin_DeleteAclsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_DeleteAclsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    kafka_common_Error_t *err = get_err(future_for(values, any));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    assert_error_message(get_err(future_for(values, specific)), "Not implemented yet");
    kafka_Map_destroy(values);
    expect_fails_with(kafka_admin_DeleteAclsResult_all(result), "Not implemented yet");
    kafka_admin_DeleteAclsResult_destroy(result);
    kafka_admin_DeleteAclsResult_destroy(NULL);
    kafka_List_destroy(filters);
    kafka_common_acl_AclBindingFilter_destroy(specific);
    kafka_common_acl_AccessControlEntryFilter_destroy(ef);
    kafka_common_resource_ResourcePatternFilter_destroy(pf);
    kafka_common_acl_AclBindingFilter_destroy(any);
    fixture_destroy(&f);
}

static void test_mock_admin_delete_acls_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_acl_AclBindingFilter_t *any = kafka_common_acl_AclBindingFilter_any();
    kafka_List_t *filters = kafka_List_new();
    kafka_List_add(filters, any);
    kafka_admin_DeleteAclsResult_t *result = kafka_admin_Admin_delete_acls(f.admin, filters);
    kafka_common_KafkaFuture_t *all = kafka_admin_DeleteAclsResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_TRUE(r.had_error);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DeleteAclsResult_destroy(result);
    kafka_List_destroy(filters);
    kafka_common_acl_AclBindingFilter_destroy(any);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// client quotas
// ---------------------------------------------------------------------------

/* `describeClientQuotas` is "Not implement yet" (sic) on the mock; `entities()`
 * is the result's one future, borrowed from the result. */
static void test_mock_admin_describe_client_quotas_reports_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_quota_ClientQuotaFilter_t *all = kafka_common_quota_ClientQuotaFilter_all();
    TEST_ASSERT_FALSE(kafka_common_quota_ClientQuotaFilter_strict(all));
    kafka_admin_DescribeClientQuotasResult_t *result = kafka_admin_Admin_describe_client_quotas(f.admin, all);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_Error_t *err = get_err(kafka_admin_DescribeClientQuotasResult_entities(result));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implement yet");
    kafka_admin_DescribeClientQuotasResult_destroy(result);
    kafka_admin_DescribeClientQuotasResult_destroy(NULL);
    kafka_common_quota_ClientQuotaFilter_destroy(all);

    /* A strict filter over a user, the default client-id and any ip. */
    kafka_common_quota_ClientQuotaFilterComponent_t *user = kafka_common_quota_ClientQuotaFilterComponent_of_entity("user", "alice");
    kafka_common_quota_ClientQuotaFilterComponent_t *client = kafka_common_quota_ClientQuotaFilterComponent_of_default_entity("client-id");
    kafka_common_quota_ClientQuotaFilterComponent_t *ip = kafka_common_quota_ClientQuotaFilterComponent_of_entity_type("ip");
    TEST_ASSERT_EQUAL_STRING("user", kafka_common_quota_ClientQuotaFilterComponent_entity_type(user));
    TEST_ASSERT_EQUAL_INT(kafka_common_quota_ClientQuotaMatch_e_exact,
                          kafka_common_quota_ClientQuotaMatch__enum(kafka_common_quota_ClientQuotaFilterComponent_match(user)));
    TEST_ASSERT_TRUE(kafka_common_quota_ClientQuotaFilterComponent_match(client) == kafka_common_quota_ClientQuotaMatch_default());
    TEST_ASSERT_TRUE(kafka_common_quota_ClientQuotaFilterComponent_match(ip) == kafka_common_quota_ClientQuotaMatch_any());
    kafka_List_t *components = kafka_List_new();
    kafka_List_add(components, user);
    kafka_List_add(components, client);
    kafka_List_add(components, ip);
    kafka_common_quota_ClientQuotaFilter_t *strict = kafka_common_quota_ClientQuotaFilter_contains_only(components);
    TEST_ASSERT_TRUE(kafka_common_quota_ClientQuotaFilter_strict(strict));
    kafka_List_t *copy = kafka_common_quota_ClientQuotaFilter_components(strict);
    TEST_ASSERT_EQUAL_INT32(3, kafka_List_size(copy));
    kafka_List_destroy(copy);
    kafka_admin_DescribeClientQuotasOptions_t *options = kafka_admin_DescribeClientQuotasOptions_new();
    result = kafka_admin_Admin_describe_client_quotas_with_options(f.admin, strict, options);
    kafka_admin_DescribeClientQuotasOptions_destroy(options);
    assert_error_message(get_err(kafka_admin_DescribeClientQuotasResult_entities(result)), "Not implement yet");
    kafka_admin_DescribeClientQuotasResult_destroy(result);
    kafka_common_quota_ClientQuotaFilter_destroy(strict);
    kafka_List_destroy(components);
    kafka_common_quota_ClientQuotaFilterComponent_destroy(user);
    kafka_common_quota_ClientQuotaFilterComponent_destroy(client);
    kafka_common_quota_ClientQuotaFilterComponent_destroy(ip);
    fixture_destroy(&f);
}

static void test_mock_admin_describe_client_quotas_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_quota_ClientQuotaFilter_t *all = kafka_common_quota_ClientQuotaFilter_all();
    kafka_admin_DescribeClientQuotasResult_t *result = kafka_admin_Admin_describe_client_quotas(f.admin, all);
    cb_result_t r;
    get_cb_pumped(&f, kafka_admin_DescribeClientQuotasResult_entities(result), &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implement yet", r.message);
    kafka_admin_DescribeClientQuotasResult_destroy(result);
    kafka_common_quota_ClientQuotaFilter_destroy(all);
    fixture_destroy(&f);
}

/* Two alterations: {user=alice, client-id=<default>} and {ip=10.0.0.1}.
 * `alterClientQuotas` is "Not implement yet" (sic) per entity; the result is
 * keyed by `ClientQuotaEntity`, found by value, and `entries()` round-trips
 * the default entity as a NULL name. */
static void test_mock_admin_alter_client_quotas_reports_unsupported_per_entity(void) {
    fixture_t f;
    fixture_init(&f, 1);
    TEST_ASSERT_TRUE(kafka_common_quota_ClientQuotaEntity_is_valid_entity_type("user"));
    TEST_ASSERT_TRUE(kafka_common_quota_ClientQuotaEntity_is_valid_entity_type("client-id"));
    TEST_ASSERT_TRUE(kafka_common_quota_ClientQuotaEntity_is_valid_entity_type("ip"));
    TEST_ASSERT_FALSE(kafka_common_quota_ClientQuotaEntity_is_valid_entity_type("group"));
    kafka_Map_t *user_entries = kafka_Map_new();
    kafka_Map_put(user_entries, (void *)"user", (void *)"alice");
    kafka_Map_put(user_entries, (void *)"client-id", NULL);
    kafka_common_quota_ClientQuotaEntity_t *user_entity = kafka_common_quota_ClientQuotaEntity_new(user_entries);
    kafka_Map_t *entries = kafka_common_quota_ClientQuotaEntity_entries(user_entity);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(entries));
    TEST_ASSERT_EQUAL_STRING("client-id", (const char *)kafka_Map_key(entries, 0));
    TEST_ASSERT_NULL(kafka_Map_value(entries, 0));
    TEST_ASSERT_EQUAL_STRING("user", (const char *)kafka_Map_key(entries, 1));
    TEST_ASSERT_EQUAL_STRING("alice", (const char *)kafka_Map_value(entries, 1));
    kafka_Map_destroy(entries);
    kafka_Map_t *ip_entries = kafka_Map_new();
    kafka_Map_put(ip_entries, (void *)"ip", (void *)"10.0.0.1");
    kafka_common_quota_ClientQuotaEntity_t *ip_entity = kafka_common_quota_ClientQuotaEntity_new(ip_entries);

    kafka_common_quota_ClientQuotaAlteration_Op_t *producer = kafka_common_quota_ClientQuotaAlteration_Op_new("producer_byte_rate", 1024.0);
    TEST_ASSERT_EQUAL_STRING("producer_byte_rate", kafka_common_quota_ClientQuotaAlteration_Op_key(producer));
    TEST_ASSERT_TRUE(kafka_common_quota_ClientQuotaAlteration_Op_value(producer) == 1024.0);
    kafka_common_quota_ClientQuotaAlteration_Op_t *consumer = kafka_common_quota_ClientQuotaAlteration_Op_new("consumer_byte_rate", 2048.0);
    kafka_common_quota_ClientQuotaAlteration_Op_t *request = kafka_common_quota_ClientQuotaAlteration_Op_new("request_percentage", 50.5);
    kafka_List_t *user_ops = kafka_List_new();
    kafka_List_add(user_ops, producer);
    kafka_List_t *ip_ops = kafka_List_new();
    kafka_List_add(ip_ops, consumer);
    kafka_List_add(ip_ops, request);
    kafka_common_quota_ClientQuotaAlteration_t *user_alt = kafka_common_quota_ClientQuotaAlteration_new(user_entity, user_ops);
    kafka_common_quota_ClientQuotaAlteration_t *ip_alt = kafka_common_quota_ClientQuotaAlteration_new(ip_entity, ip_ops);
    kafka_List_t *ops_copy = kafka_common_quota_ClientQuotaAlteration_ops(ip_alt);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(ops_copy));
    kafka_List_destroy(ops_copy);
    kafka_List_t *alterations = kafka_List_new();
    kafka_List_add(alterations, user_alt);
    kafka_List_add(alterations, ip_alt);
    kafka_admin_AlterClientQuotasOptions_t *options = kafka_admin_AlterClientQuotasOptions_new();
    kafka_admin_AlterClientQuotasOptions_set_validate_only(options, 1);
    TEST_ASSERT_TRUE(kafka_admin_AlterClientQuotasOptions_validate_only(options));
    kafka_admin_AlterClientQuotasResult_t *result = kafka_admin_Admin_alter_client_quotas_with_options(f.admin, alterations, options);
    kafka_admin_AlterClientQuotasOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterClientQuotasResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    kafka_common_Error_t *err = get_err(future_for(values, user_entity));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implement yet");
    assert_error_message(get_err(future_for(values, ip_entity)), "Not implement yet");
    /* The keys come back as entities; the default name is still NULL. */
    int saw_default = 0;
    for (int32_t i = 0; i < kafka_Map_size(values); i++) {
        kafka_Map_t *key_entries = kafka_common_quota_ClientQuotaEntity_entries((const kafka_common_quota_ClientQuotaEntity_t *)kafka_Map_key(values, i));
        for (int32_t j = 0; j < kafka_Map_size(key_entries); j++) {
            saw_default += kafka_Map_value(key_entries, j) == NULL;
        }
        kafka_Map_destroy(key_entries);
    }
    TEST_ASSERT_EQUAL_INT(1, saw_default);
    kafka_Map_destroy(values);
    expect_fails_with(kafka_admin_AlterClientQuotasResult_all(result), "Not implement yet");
    kafka_admin_AlterClientQuotasResult_destroy(result);
    kafka_admin_AlterClientQuotasResult_destroy(NULL);
    kafka_List_destroy(alterations);
    kafka_common_quota_ClientQuotaAlteration_destroy(user_alt);
    kafka_common_quota_ClientQuotaAlteration_destroy(ip_alt);
    kafka_List_destroy(user_ops);
    kafka_List_destroy(ip_ops);
    kafka_common_quota_ClientQuotaAlteration_Op_destroy(producer);
    kafka_common_quota_ClientQuotaAlteration_Op_destroy(consumer);
    kafka_common_quota_ClientQuotaAlteration_Op_destroy(request);
    kafka_common_quota_ClientQuotaEntity_destroy(user_entity);
    kafka_common_quota_ClientQuotaEntity_destroy(ip_entity);
    kafka_Map_destroy(user_entries);
    kafka_Map_destroy(ip_entries);
    fixture_destroy(&f);
}

static void test_mock_admin_alter_client_quotas_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_Map_t *ip_entries = kafka_Map_new();
    kafka_Map_put(ip_entries, (void *)"ip", (void *)"10.0.0.2");
    kafka_common_quota_ClientQuotaEntity_t *entity = kafka_common_quota_ClientQuotaEntity_new(ip_entries);
    kafka_common_quota_ClientQuotaAlteration_Op_t *op = kafka_common_quota_ClientQuotaAlteration_Op_new("connection_creation_rate", 5.0);
    kafka_List_t *ops = kafka_List_new();
    kafka_List_add(ops, op);
    kafka_common_quota_ClientQuotaAlteration_t *alt = kafka_common_quota_ClientQuotaAlteration_new(entity, ops);
    kafka_List_t *alterations = kafka_List_new();
    kafka_List_add(alterations, alt);
    kafka_admin_AlterClientQuotasResult_t *result = kafka_admin_Admin_alter_client_quotas(f.admin, alterations);
    kafka_Map_t *values = kafka_admin_AlterClientQuotasResult_values(result);
    cb_result_t r;
    get_cb_pumped(&f, future_for(values, entity), &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implement yet", r.message);
    kafka_Map_destroy(values);
    kafka_admin_AlterClientQuotasResult_destroy(result);
    kafka_List_destroy(alterations);
    kafka_common_quota_ClientQuotaAlteration_destroy(alt);
    kafka_List_destroy(ops);
    kafka_common_quota_ClientQuotaAlteration_Op_destroy(op);
    kafka_common_quota_ClientQuotaEntity_destroy(entity);
    kafka_Map_destroy(ip_entries);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// SCRAM credentials
// ---------------------------------------------------------------------------

static void test_mock_admin_scram_accessors(void) {
    const kafka_admin_ScramMechanism_t *sha256 = kafka_admin_ScramMechanism_scram_sha256();
    TEST_ASSERT_EQUAL_INT(kafka_admin_ScramMechanism_e_scram_sha256, kafka_admin_ScramMechanism__enum(sha256));
    TEST_ASSERT_EQUAL_STRING("SCRAM-SHA-256", kafka_admin_ScramMechanism_mechanism_name(sha256));
    TEST_ASSERT_EQUAL_INT8(1, kafka_admin_ScramMechanism_type(sha256));
    TEST_ASSERT_TRUE(kafka_admin_ScramMechanism_from_type(2) == kafka_admin_ScramMechanism_scram_sha512());
    TEST_ASSERT_TRUE(kafka_admin_ScramMechanism_from_type(99) == kafka_admin_ScramMechanism_unknown());
    TEST_ASSERT_TRUE(kafka_admin_ScramMechanism_from_mechanism_name("SCRAM-SHA-512") == kafka_admin_ScramMechanism_scram_sha512());
    TEST_ASSERT_TRUE(kafka_admin_ScramMechanism_from_mechanism_name("nope") == kafka_admin_ScramMechanism_unknown());

    kafka_admin_ScramCredentialInfo_t *info = kafka_admin_ScramCredentialInfo_new(sha256, 4096);
    TEST_ASSERT_TRUE(kafka_admin_ScramCredentialInfo_mechanism(info) == sha256);
    TEST_ASSERT_EQUAL_INT32(4096, kafka_admin_ScramCredentialInfo_iterations(info));
    char *text = kafka_admin_ScramCredentialInfo_to_string(info);
    TEST_ASSERT_NOT_NULL(strstr(text, "4096"));
    kafka_string_destroy(text);

    const uint8_t password[] = {'p', 'w'};
    const uint8_t salt[] = {1, 2, 3};
    kafka_Bytes_t password_bytes = {password, 2};
    kafka_Bytes_t salt_bytes = {salt, 3};
    kafka_admin_UserScramCredentialUpsertion_t *up = kafka_admin_UserScramCredentialUpsertion_with_salt("alice", info, password_bytes, salt_bytes);
    TEST_ASSERT_EQUAL_STRING("alice", kafka_admin_UserScramCredentialUpsertion_user(up));
    TEST_ASSERT_EQUAL_INT32(4096, kafka_admin_ScramCredentialInfo_iterations(kafka_admin_UserScramCredentialUpsertion_credential_info(up)));
    kafka_Bytes_t got = kafka_admin_UserScramCredentialUpsertion_password(up);
    TEST_ASSERT_EQUAL_INT32(2, got.len);
    TEST_ASSERT_EQUAL_UINT8_ARRAY(password, got.data, 2);
    got = kafka_admin_UserScramCredentialUpsertion_salt(up);
    TEST_ASSERT_EQUAL_INT32(3, got.len);
    TEST_ASSERT_EQUAL_UINT8_ARRAY(salt, got.data, 3);
    kafka_admin_UserScramCredentialUpsertion_t *up_str = kafka_admin_UserScramCredentialUpsertion_with_str("bob", info, "secret");
    TEST_ASSERT_EQUAL_INT32(6, kafka_admin_UserScramCredentialUpsertion_password(up_str).len);
    /* The one-argument form generates a random salt. */
    TEST_ASSERT_TRUE(kafka_admin_UserScramCredentialUpsertion_salt(up_str).len > 0);
    kafka_admin_UserScramCredentialDeletion_t *del = kafka_admin_UserScramCredentialDeletion_new("carol", kafka_admin_ScramMechanism_scram_sha512());
    TEST_ASSERT_EQUAL_STRING("carol", kafka_admin_UserScramCredentialDeletion_user(del));
    TEST_ASSERT_TRUE(kafka_admin_UserScramCredentialDeletion_mechanism(del) == kafka_admin_ScramMechanism_scram_sha512());
    kafka_admin_UserScramCredentialAlteration_t *alt_up = kafka_admin_UserScramCredentialAlteration_upsertion(up);
    kafka_admin_UserScramCredentialAlteration_t *alt_del = kafka_admin_UserScramCredentialAlteration_deletion(del);
    TEST_ASSERT_EQUAL_INT(kafka_admin_UserScramCredentialAlteration_e_upsertion, kafka_admin_UserScramCredentialAlteration__enum(alt_up));
    TEST_ASSERT_EQUAL_INT(kafka_admin_UserScramCredentialAlteration_e_deletion, kafka_admin_UserScramCredentialAlteration__enum(alt_del));
    TEST_ASSERT_EQUAL_STRING("alice", kafka_admin_UserScramCredentialAlteration_user(alt_up));
    TEST_ASSERT_EQUAL_STRING("carol", kafka_admin_UserScramCredentialAlteration_user(alt_del));
    kafka_admin_UserScramCredentialAlteration_destroy(alt_up);
    kafka_admin_UserScramCredentialAlteration_destroy(alt_del);
    kafka_admin_UserScramCredentialDeletion_destroy(del);
    kafka_admin_UserScramCredentialUpsertion_destroy(up_str);
    kafka_admin_UserScramCredentialUpsertion_destroy(up);
    kafka_admin_ScramCredentialInfo_destroy(info);
}

/* `alterUserScramCredentials` is "Not implemented yet" per user, keyed by
 * user name, sorted; an empty password still reaches the mock. */
static void test_mock_admin_alter_user_scram_credentials_reports_unsupported_per_user(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_ScramCredentialInfo_t *info = kafka_admin_ScramCredentialInfo_new(kafka_admin_ScramMechanism_scram_sha256(), 8192);
    kafka_admin_UserScramCredentialUpsertion_t *up = kafka_admin_UserScramCredentialUpsertion_with_str("scram-b", info, "");
    kafka_admin_UserScramCredentialDeletion_t *del = kafka_admin_UserScramCredentialDeletion_new("scram-a", kafka_admin_ScramMechanism_scram_sha512());
    kafka_admin_UserScramCredentialAlteration_t *alt_up = kafka_admin_UserScramCredentialAlteration_upsertion(up);
    kafka_admin_UserScramCredentialAlteration_t *alt_del = kafka_admin_UserScramCredentialAlteration_deletion(del);
    kafka_List_t *alterations = kafka_List_new();
    kafka_List_add(alterations, alt_up);
    kafka_List_add(alterations, alt_del);
    kafka_admin_AlterUserScramCredentialsOptions_t *options = kafka_admin_AlterUserScramCredentialsOptions_new();
    kafka_admin_AlterUserScramCredentialsResult_t *result = kafka_admin_Admin_alter_user_scram_credentials_with_options(f.admin, alterations, options);
    kafka_admin_AlterUserScramCredentialsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_AlterUserScramCredentialsResult_values(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(values));
    TEST_ASSERT_EQUAL_STRING("scram-a", (const char *)kafka_Map_key(values, 0));
    TEST_ASSERT_EQUAL_STRING("scram-b", (const char *)kafka_Map_key(values, 1));
    kafka_common_Error_t *err = get_err(future_for(values, "scram-a"));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    assert_error_message(get_err(future_for(values, "scram-b")), "Not implemented yet");
    kafka_Map_destroy(values);
    expect_fails_with(kafka_admin_AlterUserScramCredentialsResult_all(result), "Not implemented yet");
    kafka_admin_AlterUserScramCredentialsResult_destroy(result);
    kafka_admin_AlterUserScramCredentialsResult_destroy(NULL);
    kafka_List_destroy(alterations);
    kafka_admin_UserScramCredentialAlteration_destroy(alt_up);
    kafka_admin_UserScramCredentialAlteration_destroy(alt_del);
    kafka_admin_UserScramCredentialDeletion_destroy(del);
    kafka_admin_UserScramCredentialUpsertion_destroy(up);
    kafka_admin_ScramCredentialInfo_destroy(info);
    fixture_destroy(&f);
}

/* `describeUserScramCredentials` is "Not implemented yet": `all()`, `users()`
 * and `description(user)` all derive from the one failed future. */
static void test_mock_admin_describe_user_scram_credentials_reports_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_DescribeUserScramCredentialsResult_t *result = kafka_admin_Admin_describe_user_scram_credentials(f.admin);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *all = kafka_admin_DescribeUserScramCredentialsResult_all(result);
    kafka_common_Error_t *err = get_err(all);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_common_KafkaFuture_destroy(all);
    expect_fails_with(kafka_admin_DescribeUserScramCredentialsResult_users(result), "Not implemented yet");
    expect_fails_with(kafka_admin_DescribeUserScramCredentialsResult_description(result, "alice"), "Not implemented yet");
    kafka_admin_DescribeUserScramCredentialsResult_destroy(result);
    kafka_admin_DescribeUserScramCredentialsResult_destroy(NULL);

    kafka_List_t *users = string_list("alice", "bob", NULL);
    kafka_admin_DescribeUserScramCredentialsOptions_t *options = kafka_admin_DescribeUserScramCredentialsOptions_new();
    result = kafka_admin_Admin_describe_user_scram_credentials_with_users_options(f.admin, users, options);
    kafka_admin_DescribeUserScramCredentialsOptions_destroy(options);
    expect_fails_with(kafka_admin_DescribeUserScramCredentialsResult_all(result), "Not implemented yet");
    kafka_admin_DescribeUserScramCredentialsResult_destroy(result);
    result = kafka_admin_Admin_describe_user_scram_credentials_with_users(f.admin, users);
    expect_fails_with(kafka_admin_DescribeUserScramCredentialsResult_users(result), "Not implemented yet");
    kafka_admin_DescribeUserScramCredentialsResult_destroy(result);
    kafka_List_destroy(users);
    fixture_destroy(&f);
}

static void test_mock_admin_user_scram_credentials_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_DescribeUserScramCredentialsResult_t *result = kafka_admin_Admin_describe_user_scram_credentials(f.admin);
    kafka_common_KafkaFuture_t *all = kafka_admin_DescribeUserScramCredentialsResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_DescribeUserScramCredentialsResult_destroy(result);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// delegation tokens
// ---------------------------------------------------------------------------

static kafka_common_security_auth_KafkaPrincipal_t *user_principal(const char *name) {
    kafka_common_security_auth_KafkaPrincipal_t *p = kafka_common_security_auth_KafkaPrincipal_new("User", name);
    TEST_ASSERT_NOT_NULL(p);
    return p;
}

/* Creates a token owned by `owner` (the mock makes the first renewer the
 * owner) and copies its HMAC into `hmac_out` (capacity `cap`), returning
 * the HMAC length. */
static int32_t create_token_for(fixture_t *f, const char *owner, uint8_t *hmac_out, int32_t cap) {
    kafka_common_security_auth_KafkaPrincipal_t *p = user_principal(owner);
    kafka_List_t *renewers = kafka_List_new();
    kafka_List_add(renewers, p);
    kafka_admin_CreateDelegationTokenOptions_t *options = kafka_admin_CreateDelegationTokenOptions_new();
    kafka_admin_CreateDelegationTokenOptions_set_renewers(options, renewers);
    kafka_admin_CreateDelegationTokenResult_t *result = kafka_admin_Admin_create_delegation_token_with_options(f->admin, options);
    kafka_admin_CreateDelegationTokenOptions_destroy(options);
    kafka_List_destroy(renewers);
    kafka_common_security_auth_KafkaPrincipal_destroy(p);
    TEST_ASSERT_NOT_NULL(result);
    const kafka_common_security_token_delegation_DelegationToken_t *token =
        (const kafka_common_security_token_delegation_DelegationToken_t *)get_ok(kafka_admin_CreateDelegationTokenResult_delegation_token(result));
    kafka_Bytes_t hmac = kafka_common_security_token_delegation_DelegationToken_hmac(token);
    TEST_ASSERT_TRUE(hmac.len > 0 && hmac.len <= cap);
    memcpy(hmac_out, hmac.data, (size_t)hmac.len);
    kafka_admin_CreateDelegationTokenResult_destroy(result);
    return hmac.len;
}

static int32_t count_tokens(fixture_t *f, const kafka_admin_DescribeDelegationTokenOptions_t *options) {
    kafka_admin_DescribeDelegationTokenResult_t *result =
        options ? kafka_admin_Admin_describe_delegation_token_with_options(f->admin, options)
                : kafka_admin_Admin_describe_delegation_token(f->admin);
    TEST_ASSERT_NOT_NULL(result);
    const kafka_List_t *tokens = (const kafka_List_t *)get_ok(kafka_admin_DescribeDelegationTokenResult_delegation_tokens(result));
    int32_t count = kafka_List_size(tokens);
    kafka_admin_DescribeDelegationTokenResult_destroy(result);
    return count;
}

/* Mirrors Java's MockAdminClient: the token id doubles as the HMAC, the
 * first renewer is the owner, `renew` sets the expiry to the renew period,
 * `expire` to the expiry period and removes the token when negative, and an
 * unknown HMAC is DELEGATION_TOKEN_NOT_FOUND. */
static void test_mock_admin_delegation_token_lifecycle(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_security_auth_KafkaPrincipal_t *owner = user_principal("owner-principal");
    kafka_common_security_auth_KafkaPrincipal_t *second = user_principal("second-renewer");
    TEST_ASSERT_EQUAL_STRING("User", kafka_common_security_auth_KafkaPrincipal_principal_type(owner));
    TEST_ASSERT_EQUAL_STRING("owner-principal", kafka_common_security_auth_KafkaPrincipal_name(owner));
    kafka_List_t *renewers = kafka_List_new();
    kafka_List_add(renewers, owner);
    kafka_List_add(renewers, second);
    kafka_admin_CreateDelegationTokenOptions_t *options = kafka_admin_CreateDelegationTokenOptions_new();
    kafka_admin_CreateDelegationTokenOptions_set_renewers(options, renewers);
    kafka_admin_CreateDelegationTokenOptions_set_max_lifetime_ms(options, 86400000);
    TEST_ASSERT_EQUAL_INT64(86400000, kafka_admin_CreateDelegationTokenOptions_max_lifetime_ms(options));
    TEST_ASSERT_NULL(kafka_admin_CreateDelegationTokenOptions_owner(options));
    kafka_List_t *copy = kafka_admin_CreateDelegationTokenOptions_renewers(options);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(copy));
    kafka_List_destroy(copy);
    kafka_admin_CreateDelegationTokenResult_t *created = kafka_admin_Admin_create_delegation_token_with_options(f.admin, options);
    kafka_admin_CreateDelegationTokenOptions_destroy(options);
    kafka_List_destroy(renewers);
    TEST_ASSERT_NOT_NULL(created);
    const kafka_common_security_token_delegation_DelegationToken_t *token =
        (const kafka_common_security_token_delegation_DelegationToken_t *)get_ok(kafka_admin_CreateDelegationTokenResult_delegation_token(created));
    const kafka_common_security_token_delegation_TokenInformation_t *info = kafka_common_security_token_delegation_DelegationToken_token_info(token);
    TEST_ASSERT_EQUAL_STRING("owner-principal", kafka_common_security_auth_KafkaPrincipal_name(kafka_common_security_token_delegation_TokenInformation_owner(info)));
    kafka_List_t *token_renewers = kafka_common_security_token_delegation_TokenInformation_renewers(info);
    TEST_ASSERT_EQUAL_INT32(2, kafka_List_size(token_renewers));
    TEST_ASSERT_EQUAL_STRING("owner-principal", kafka_common_security_auth_KafkaPrincipal_name(
                                                    (const kafka_common_security_auth_KafkaPrincipal_t *)kafka_List_get(token_renewers, 0)));
    TEST_ASSERT_EQUAL_STRING("second-renewer", kafka_common_security_auth_KafkaPrincipal_name(
                                                   (const kafka_common_security_auth_KafkaPrincipal_t *)kafka_List_get(token_renewers, 1)));
    kafka_List_destroy(token_renewers);
    const char *token_id = kafka_common_security_token_delegation_TokenInformation_token_id(info);
    TEST_ASSERT_TRUE(strlen(token_id) > 0);
    kafka_Bytes_t hmac = kafka_common_security_token_delegation_DelegationToken_hmac(token);
    TEST_ASSERT_EQUAL_INT32((int32_t)strlen(token_id), hmac.len);
    TEST_ASSERT_EQUAL_MEMORY(token_id, hmac.data, (uint32_t)hmac.len);
    char *base64 = kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string(token);
    TEST_ASSERT_TRUE(strlen(base64) > 0);
    kafka_string_destroy(base64);
    TEST_ASSERT_EQUAL_INT64(-1, kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(info));
    uint8_t hmac_copy[256];
    TEST_ASSERT_TRUE(hmac.len <= (int32_t)sizeof(hmac_copy));
    memcpy(hmac_copy, hmac.data, (size_t)hmac.len);
    kafka_Bytes_t saved = {hmac_copy, hmac.len};
    kafka_admin_CreateDelegationTokenResult_destroy(created);
    kafka_admin_CreateDelegationTokenResult_destroy(NULL);

    TEST_ASSERT_EQUAL_INT32(1, count_tokens(&f, NULL));

    kafka_admin_RenewDelegationTokenOptions_t *renew_options = kafka_admin_RenewDelegationTokenOptions_new();
    kafka_admin_RenewDelegationTokenOptions_set_renew_time_period_ms(renew_options, 4242);
    TEST_ASSERT_EQUAL_INT64(4242, kafka_admin_RenewDelegationTokenOptions_renew_time_period_ms(renew_options));
    kafka_admin_RenewDelegationTokenResult_t *renewed = kafka_admin_Admin_renew_delegation_token_with_options(f.admin, saved, renew_options);
    kafka_admin_RenewDelegationTokenOptions_destroy(renew_options);
    TEST_ASSERT_NOT_NULL(renewed);
    TEST_ASSERT_EQUAL_INT64(4242, *(const int64_t *)get_ok(kafka_admin_RenewDelegationTokenResult_expiry_timestamp(renewed)));
    kafka_admin_RenewDelegationTokenResult_destroy(renewed);
    kafka_admin_RenewDelegationTokenResult_destroy(NULL);

    const uint8_t bogus_bytes[] = {'b', 'o', 'g', 'u', 's'};
    kafka_Bytes_t bogus = {bogus_bytes, 5};
    renewed = kafka_admin_Admin_renew_delegation_token(f.admin, bogus);
    kafka_common_Error_t *err = get_err(kafka_admin_RenewDelegationTokenResult_expiry_timestamp(renewed));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_DELEGATION_TOKEN_NOT_FOUND, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_admin_RenewDelegationTokenResult_destroy(renewed);
    kafka_admin_ExpireDelegationTokenResult_t *expired = kafka_admin_Admin_expire_delegation_token(f.admin, bogus);
    err = get_err(kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(expired));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_DELEGATION_TOKEN_NOT_FOUND, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_admin_ExpireDelegationTokenResult_destroy(expired);

    kafka_admin_ExpireDelegationTokenOptions_t *expire_options = kafka_admin_ExpireDelegationTokenOptions_new();
    kafka_admin_ExpireDelegationTokenOptions_set_expiry_time_period_ms(expire_options, -1);
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_ExpireDelegationTokenOptions_expiry_time_period_ms(expire_options));
    expired = kafka_admin_Admin_expire_delegation_token_with_options(f.admin, saved, expire_options);
    kafka_admin_ExpireDelegationTokenOptions_destroy(expire_options);
    TEST_ASSERT_NOT_NULL(expired);
    TEST_ASSERT_EQUAL_INT64(-1, *(const int64_t *)get_ok(kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(expired)));
    kafka_admin_ExpireDelegationTokenResult_destroy(expired);
    kafka_admin_ExpireDelegationTokenResult_destroy(NULL);
    TEST_ASSERT_EQUAL_INT32(0, count_tokens(&f, NULL));
    kafka_admin_DescribeDelegationTokenResult_destroy(NULL);
    kafka_common_security_auth_KafkaPrincipal_destroy(owner);
    kafka_common_security_auth_KafkaPrincipal_destroy(second);
    fixture_destroy(&f);
}

/* `describeDelegationToken` with an owners filter lists only those owners'
 * tokens; without one, every token. */
static void test_mock_admin_describe_delegation_token_filters_by_owner(void) {
    fixture_t f;
    fixture_init(&f, 1);
    uint8_t hmac[256];
    create_token_for(&f, "alice", hmac, (int32_t)sizeof(hmac));
    create_token_for(&f, "bob", hmac, (int32_t)sizeof(hmac));
    TEST_ASSERT_EQUAL_INT32(2, count_tokens(&f, NULL));

    kafka_common_security_auth_KafkaPrincipal_t *alice = user_principal("alice");
    kafka_List_t *owners = kafka_List_new();
    kafka_List_add(owners, alice);
    kafka_admin_DescribeDelegationTokenOptions_t *options = kafka_admin_DescribeDelegationTokenOptions_new();
    TEST_ASSERT_NULL(kafka_admin_DescribeDelegationTokenOptions_owners(options));
    kafka_admin_DescribeDelegationTokenOptions_set_owners(options, owners);
    kafka_List_t *copy = kafka_admin_DescribeDelegationTokenOptions_owners(options);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(copy));
    kafka_List_destroy(copy);
    kafka_admin_DescribeDelegationTokenResult_t *result = kafka_admin_Admin_describe_delegation_token_with_options(f.admin, options);
    const kafka_List_t *tokens = (const kafka_List_t *)get_ok(kafka_admin_DescribeDelegationTokenResult_delegation_tokens(result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(tokens));
    const kafka_common_security_token_delegation_DelegationToken_t *token =
        (const kafka_common_security_token_delegation_DelegationToken_t *)kafka_List_get(tokens, 0);
    TEST_ASSERT_EQUAL_STRING("alice", kafka_common_security_auth_KafkaPrincipal_name(kafka_common_security_token_delegation_TokenInformation_owner(
                                          kafka_common_security_token_delegation_DelegationToken_token_info(token))));
    kafka_admin_DescribeDelegationTokenResult_destroy(result);
    kafka_admin_DescribeDelegationTokenOptions_destroy(options);
    kafka_List_destroy(owners);
    kafka_common_security_auth_KafkaPrincipal_destroy(alice);
    fixture_destroy(&f);
}

/* A non-`User` renewer is refused by the mock (INVALID_PRINCIPAL_TYPE), and
 * with no renewer at all there is no owner to assign: the Rust mock fails
 * the future where Java's `renewers().get(0)` throws. */
static void test_mock_admin_create_delegation_token_rejects_bad_input(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_security_auth_KafkaPrincipal_t *group = kafka_common_security_auth_KafkaPrincipal_new("Group", "admins");
    kafka_List_t *renewers = kafka_List_new();
    kafka_List_add(renewers, group);
    kafka_admin_CreateDelegationTokenOptions_t *options = kafka_admin_CreateDelegationTokenOptions_new();
    kafka_admin_CreateDelegationTokenOptions_set_renewers(options, renewers);
    kafka_admin_CreateDelegationTokenResult_t *result = kafka_admin_Admin_create_delegation_token_with_options(f.admin, options);
    kafka_admin_CreateDelegationTokenOptions_destroy(options);
    kafka_common_Error_t *err = get_err(kafka_admin_CreateDelegationTokenResult_delegation_token(result));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_INVALID_PRINCIPAL_TYPE, kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
    kafka_admin_CreateDelegationTokenResult_destroy(result);

    result = kafka_admin_Admin_create_delegation_token(f.admin);
    err = get_err(kafka_admin_CreateDelegationTokenResult_delegation_token(result));
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "createDelegationToken requires at least one renewer: MockAdminClient makes the first renewer the owner");
    kafka_admin_CreateDelegationTokenResult_destroy(result);
    TEST_ASSERT_EQUAL_INT32(0, count_tokens(&f, NULL));
    kafka_List_destroy(renewers);
    kafka_common_security_auth_KafkaPrincipal_destroy(group);
    fixture_destroy(&f);
}

static void test_mock_admin_delegation_token_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    uint8_t hmac[256];
    int32_t len = create_token_for(&f, "async-owner", hmac, (int32_t)sizeof(hmac));
    kafka_Bytes_t saved = {hmac, len};
    kafka_admin_RenewDelegationTokenOptions_t *options = kafka_admin_RenewDelegationTokenOptions_new();
    kafka_admin_RenewDelegationTokenOptions_set_renew_time_period_ms(options, 99);
    kafka_admin_RenewDelegationTokenResult_t *renewed = kafka_admin_Admin_renew_delegation_token_with_options(f.admin, saved, options);
    kafka_admin_RenewDelegationTokenOptions_destroy(options);
    cb_result_t r;
    get_cb_pumped(&f, kafka_admin_RenewDelegationTokenResult_expiry_timestamp(renewed), &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT64(99, *(const int64_t *)r.value);
    kafka_admin_RenewDelegationTokenResult_destroy(renewed);
    kafka_admin_DescribeDelegationTokenResult_t *described = kafka_admin_Admin_describe_delegation_token(f.admin);
    get_cb_pumped(&f, kafka_admin_DescribeDelegationTokenResult_delegation_tokens(described), &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size((const kafka_List_t *)r.value));
    kafka_admin_DescribeDelegationTokenResult_destroy(described);
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// features
// ---------------------------------------------------------------------------

/* Seeds metadata.version 17 in [14, 21], transaction.version 2 in [1, 3]
 * and group.version 1 in [0, 4], at finalized epoch 123. */
static void seed_features(fixture_t *f) {
    int16_t levels[3] = {17, 2, 1};
    int16_t mins[3] = {14, 1, 0};
    int16_t maxs[3] = {21, 3, 4};
    const char *names[3] = {"metadata.version", "transaction.version", "group.version"};
    kafka_Map_t *level_map = kafka_Map_new();
    kafka_Map_t *min_map = kafka_Map_new();
    kafka_Map_t *max_map = kafka_Map_new();
    for (int i = 0; i < 3; i++) {
        kafka_Map_put(level_map, (void *)names[i], &levels[i]);
        kafka_Map_put(min_map, (void *)names[i], &mins[i]);
        kafka_Map_put(max_map, (void *)names[i], &maxs[i]);
    }
    kafka_admin_MockAdminClient_set_feature_levels(f->mock, level_map, min_map, max_map);
    kafka_Map_destroy(level_map);
    kafka_Map_destroy(min_map);
    kafka_Map_destroy(max_map);
}

static int16_t finalized_level(fixture_t *f, const char *feature) {
    kafka_admin_DescribeFeaturesResult_t *result = kafka_admin_Admin_describe_features(f->admin);
    kafka_common_KafkaFuture_t *future = kafka_admin_DescribeFeaturesResult_feature_metadata(result);
    const kafka_admin_FeatureMetadata_t *metadata = (const kafka_admin_FeatureMetadata_t *)get_ok(future);
    kafka_Map_t *finalized = kafka_admin_FeatureMetadata_finalized_features(metadata);
    const kafka_admin_FinalizedVersionRange_t *range = (const kafka_admin_FinalizedVersionRange_t *)kafka_Map_get(finalized, (void *)feature);
    TEST_ASSERT_NOT_NULL(range);
    int16_t level = kafka_admin_FinalizedVersionRange_max_version_level(range);
    kafka_Map_destroy(finalized);
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_DescribeFeaturesResult_destroy(result);
    return level;
}

static kafka_admin_FeatureUpdate_t *feature_update(int16_t level, const kafka_admin_FeatureUpdate_UpgradeType_t *type) {
    kafka_admin_FeatureUpdate_t *update = NULL;
    TEST_ASSERT_NULL(kafka_admin_FeatureUpdate_new(level, type, &update));
    TEST_ASSERT_NOT_NULL(update);
    return update;
}

/* The mock reports each seeded feature as a finalized range [level, level]
 * and a supported range [min, max], sorted by name; the finalized epoch is
 * fixed at 123 (MockAdminClient.java). */
static void test_mock_admin_describe_features(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_features(&f);
    kafka_admin_DescribeFeaturesOptions_t *options = kafka_admin_DescribeFeaturesOptions_new();
    kafka_admin_DescribeFeaturesOptions_set_node_id(options, 0);
    kafka_admin_DescribeFeaturesOptions_set_timeout_ms(options, 5000);
    kafka_admin_DescribeFeaturesResult_t *result = kafka_admin_Admin_describe_features_with_options(f.admin, options);
    kafka_admin_DescribeFeaturesOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = kafka_admin_DescribeFeaturesResult_feature_metadata(result);
    const kafka_admin_FeatureMetadata_t *metadata = (const kafka_admin_FeatureMetadata_t *)get_ok(future);
    TEST_ASSERT_EQUAL_INT64(123, kafka_admin_FeatureMetadata_finalized_features_epoch(metadata));
    kafka_Map_t *finalized = kafka_admin_FeatureMetadata_finalized_features(metadata);
    TEST_ASSERT_EQUAL_INT32(3, kafka_Map_size(finalized));
    TEST_ASSERT_EQUAL_STRING("group.version", (const char *)kafka_Map_key(finalized, 0));
    TEST_ASSERT_EQUAL_STRING("metadata.version", (const char *)kafka_Map_key(finalized, 1));
    TEST_ASSERT_EQUAL_STRING("transaction.version", (const char *)kafka_Map_key(finalized, 2));
    const kafka_admin_FinalizedVersionRange_t *range = (const kafka_admin_FinalizedVersionRange_t *)kafka_Map_value(finalized, 1);
    TEST_ASSERT_EQUAL_INT16(17, kafka_admin_FinalizedVersionRange_min_version_level(range));
    TEST_ASSERT_EQUAL_INT16(17, kafka_admin_FinalizedVersionRange_max_version_level(range));
    char *text = kafka_admin_FinalizedVersionRange_to_string(range);
    TEST_ASSERT_NOT_NULL(strstr(text, "17"));
    kafka_string_destroy(text);
    kafka_Map_destroy(finalized);
    kafka_Map_t *supported = kafka_admin_FeatureMetadata_supported_features(metadata);
    TEST_ASSERT_EQUAL_INT32(3, kafka_Map_size(supported));
    const kafka_admin_SupportedVersionRange_t *srange = (const kafka_admin_SupportedVersionRange_t *)kafka_Map_get(supported, (void *)"transaction.version");
    TEST_ASSERT_NOT_NULL(srange);
    TEST_ASSERT_EQUAL_INT16(1, kafka_admin_SupportedVersionRange_min_version(srange));
    TEST_ASSERT_EQUAL_INT16(3, kafka_admin_SupportedVersionRange_max_version(srange));
    kafka_Map_destroy(supported);
    text = kafka_admin_FeatureMetadata_to_string(metadata);
    TEST_ASSERT_NOT_NULL(strstr(text, "metadata.version"));
    kafka_string_destroy(text);
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_DescribeFeaturesResult_destroy(result);
    kafka_admin_DescribeFeaturesResult_destroy(NULL);
    /* Without a node id the answer is the same. */
    TEST_ASSERT_EQUAL_INT16(2, finalized_level(&f, "transaction.version"));
    fixture_destroy(&f);
}

/* `updateFeatures` with `validateOnly` leaves the level alone; the real
 * update applies it. */
static void test_mock_admin_update_features_applies_a_valid_upgrade(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_features(&f);
    kafka_admin_FeatureUpdate_t *update = feature_update(19, kafka_admin_FeatureUpdate_UpgradeType_upgrade());
    TEST_ASSERT_EQUAL_INT16(19, kafka_admin_FeatureUpdate_max_version_level(update));
    TEST_ASSERT_TRUE(kafka_admin_FeatureUpdate_upgrade_type(update) == kafka_admin_FeatureUpdate_UpgradeType_upgrade());
    TEST_ASSERT_EQUAL_INT(kafka_admin_FeatureUpdate_UpgradeType_e_upgrade,
                          kafka_admin_FeatureUpdate_UpgradeType__enum(kafka_admin_FeatureUpdate_upgrade_type(update)));
    TEST_ASSERT_EQUAL_INT8(1, kafka_admin_FeatureUpdate_UpgradeType_code(kafka_admin_FeatureUpdate_UpgradeType_upgrade()));
    TEST_ASSERT_TRUE(kafka_admin_FeatureUpdate_UpgradeType_from_code(2) == kafka_admin_FeatureUpdate_UpgradeType_safe_downgrade());
    TEST_ASSERT_TRUE(kafka_admin_FeatureUpdate_UpgradeType_from_code(3) == kafka_admin_FeatureUpdate_UpgradeType_unsafe_downgrade());
    TEST_ASSERT_TRUE(kafka_admin_FeatureUpdate_UpgradeType_from_code(42) == kafka_admin_FeatureUpdate_UpgradeType_unknown());
    char *text = kafka_admin_FeatureUpdate_to_string(update);
    TEST_ASSERT_NOT_NULL(strstr(text, "19"));
    kafka_string_destroy(text);
    kafka_Map_t *updates = kafka_Map_new();
    kafka_Map_put(updates, (void *)"metadata.version", update);

    kafka_admin_UpdateFeaturesOptions_t *options = kafka_admin_UpdateFeaturesOptions_new();
    kafka_admin_UpdateFeaturesOptions_set_validate_only(options, 1);
    kafka_admin_UpdateFeaturesResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_Admin_update_features_with_options(f.admin, updates, options, &result));
    kafka_admin_UpdateFeaturesOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *values = kafka_admin_UpdateFeaturesResult_values(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_Map_size(values));
    TEST_ASSERT_NULL(get_ok(future_for(values, "metadata.version")));
    kafka_Map_destroy(values);
    expect_void_ok(kafka_admin_UpdateFeaturesResult_all(result));
    kafka_admin_UpdateFeaturesResult_destroy(result);
    kafka_admin_UpdateFeaturesResult_destroy(NULL);
    TEST_ASSERT_EQUAL_INT16(17, finalized_level(&f, "metadata.version"));

    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_Admin_update_features(f.admin, updates, &result));
    expect_void_ok(kafka_admin_UpdateFeaturesResult_all(result));
    kafka_admin_UpdateFeaturesResult_destroy(result);
    TEST_ASSERT_EQUAL_INT16(19, finalized_level(&f, "metadata.version"));
    kafka_Map_destroy(updates);
    kafka_admin_FeatureUpdate_destroy(update);
    fixture_destroy(&f);
}

/* An out-of-range level fails every feature's future with the mock's
 * `invalidUpdateVersion` message — an InvalidRequestException wrapping the
 * InvalidUpdateVersionException's text (MockAdminClient.java:1333,1354); an UNKNOWN upgrade type is
 * rejected the same way; `FeatureUpdate`'s constructor refuses a level
 * below 1 with an UPGRADE type (Java's IllegalArgumentException); an empty
 * batch is accepted by the mock and yields an empty result. */
static void test_mock_admin_update_features_rejects_bad_updates(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_features(&f);
    kafka_admin_FeatureUpdate_t *too_high = feature_update(99, kafka_admin_FeatureUpdate_UpgradeType_upgrade());
    kafka_Map_t *updates = kafka_Map_new();
    kafka_Map_put(updates, (void *)"metadata.version", too_high);
    kafka_admin_UpdateFeaturesResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_Admin_update_features(f.admin, updates, &result));
    kafka_Map_t *values = kafka_admin_UpdateFeaturesResult_values(result);
    kafka_common_Error_t *err = get_err(future_for(values, "metadata.version"));
    TEST_ASSERT_TRUE(kafka_common_Error_is_api_error(err));
    assert_error_message(err, "Invalid update version 99 for feature metadata.version. Can't upgrade above 21");
    kafka_Map_destroy(values);
    expect_fails_with(kafka_admin_UpdateFeaturesResult_all(result), "Invalid update version 99 for feature metadata.version. Can't upgrade above 21");
    kafka_admin_UpdateFeaturesResult_destroy(result);
    kafka_Map_destroy(updates);
    kafka_admin_FeatureUpdate_destroy(too_high);
    TEST_ASSERT_EQUAL_INT16(17, finalized_level(&f, "metadata.version"));

    kafka_admin_FeatureUpdate_t *unknown = feature_update(18, kafka_admin_FeatureUpdate_UpgradeType_unknown());
    updates = kafka_Map_new();
    kafka_Map_put(updates, (void *)"metadata.version", unknown);
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_Admin_update_features(f.admin, updates, &result));
    values = kafka_admin_UpdateFeaturesResult_values(result);
    assert_error_message(get_err(future_for(values, "metadata.version")),
                         "Invalid update version 18 for feature metadata.version. Invalid upgrade type.");
    kafka_Map_destroy(values);
    kafka_admin_UpdateFeaturesResult_destroy(result);
    kafka_Map_destroy(updates);
    kafka_admin_FeatureUpdate_destroy(unknown);

    kafka_admin_FeatureUpdate_t *bad = NULL;
    err = kafka_admin_FeatureUpdate_new(0, kafka_admin_FeatureUpdate_UpgradeType_upgrade(), &bad);
    TEST_ASSERT_NULL(bad);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    assert_error_message(err, "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:0 is < 1.");
    kafka_admin_FeatureUpdate_t *downgrade = feature_update(0, kafka_admin_FeatureUpdate_UpgradeType_safe_downgrade());
    kafka_admin_FeatureUpdate_destroy(downgrade);

    kafka_Map_t *empty = kafka_Map_new();
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_Admin_update_features(f.admin, empty, &result));
    TEST_ASSERT_NOT_NULL(result);
    values = kafka_admin_UpdateFeaturesResult_values(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(values));
    kafka_Map_destroy(values);
    expect_void_ok(kafka_admin_UpdateFeaturesResult_all(result));
    kafka_admin_UpdateFeaturesResult_destroy(result);
    kafka_Map_destroy(empty);
    fixture_destroy(&f);
}

static void test_mock_admin_features_async(void) {
    fixture_t f;
    fixture_init(&f, 1);
    seed_features(&f);
    kafka_admin_DescribeFeaturesResult_t *described = kafka_admin_Admin_describe_features(f.admin);
    kafka_common_KafkaFuture_t *future = kafka_admin_DescribeFeaturesResult_feature_metadata(described);
    cb_result_t r;
    get_cb_pumped(&f, future, &r);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT64(123, kafka_admin_FeatureMetadata_finalized_features_epoch((const kafka_admin_FeatureMetadata_t *)r.value));
    kafka_common_KafkaFuture_destroy(future);
    kafka_admin_DescribeFeaturesResult_destroy(described);

    kafka_admin_FeatureUpdate_t *update = feature_update(3, kafka_admin_FeatureUpdate_UpgradeType_upgrade());
    kafka_Map_t *updates = kafka_Map_new();
    kafka_Map_put(updates, (void *)"transaction.version", update);
    kafka_admin_UpdateFeaturesResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_Admin_update_features(f.admin, updates, &result));
    kafka_common_KafkaFuture_t *all = kafka_admin_UpdateFeaturesResult_all(result);
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_FALSE(r.had_error);
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_UpdateFeaturesResult_destroy(result);
    kafka_Map_destroy(updates);
    kafka_admin_FeatureUpdate_destroy(update);
    TEST_ASSERT_EQUAL_INT16(3, finalized_level(&f, "transaction.version"));
    fixture_destroy(&f);
}

// ---------------------------------------------------------------------------
// transactions
// ---------------------------------------------------------------------------

/* `describeProducers`: "Not implemented yet" per partition through
 * `partitionResult(tp)` (out slot; IllegalArgument for an unrequested one). */
static void test_mock_admin_describe_producers_reports_unsupported_per_partition(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_TopicPartition_t *p0 = tp_new("txn-topic", 0);
    kafka_common_TopicPartition_t *other = tp_new("txn-topic", 7);
    kafka_List_t *partitions = kafka_List_new();
    kafka_List_add(partitions, p0);
    kafka_admin_DescribeProducersOptions_t *options = kafka_admin_DescribeProducersOptions_new();
    kafka_admin_DescribeProducersOptions_set_broker_id(options, 0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeProducersOptions_broker_id(options));
    kafka_admin_DescribeProducersResult_t *result = kafka_admin_Admin_describe_producers_with_options(f.admin, partitions, options);
    kafka_admin_DescribeProducersOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_DescribeProducersResult_partition_result(result, p0, &future));
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_common_KafkaFuture_destroy(future);
    future = NULL;
    err = kafka_admin_DescribeProducersResult_partition_result(result, other, &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    expect_fails_with(kafka_admin_DescribeProducersResult_all(result), "Not implemented yet");
    kafka_admin_DescribeProducersResult_destroy(result);
    kafka_admin_DescribeProducersResult_destroy(NULL);
    kafka_List_destroy(partitions);
    kafka_common_TopicPartition_destroy(p0);
    kafka_common_TopicPartition_destroy(other);
    fixture_destroy(&f);
}

/* `describeTransactions`: "Not implemented yet" per transactional id. */
static void test_mock_admin_describe_transactions_reports_unsupported_per_id(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *ids = string_list("txn-a", NULL);
    kafka_admin_DescribeTransactionsOptions_t *options = kafka_admin_DescribeTransactionsOptions_new();
    kafka_admin_DescribeTransactionsResult_t *result = kafka_admin_Admin_describe_transactions_with_options(f.admin, ids, options);
    kafka_admin_DescribeTransactionsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_DescribeTransactionsResult_description(result, "txn-a", &future));
    kafka_common_Error_t *err = get_err(future);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_common_KafkaFuture_destroy(future);
    future = NULL;
    err = kafka_admin_DescribeTransactionsResult_description(result, "txn-unknown", &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    expect_fails_with(kafka_admin_DescribeTransactionsResult_all(result), "Not implemented yet");
    kafka_admin_DescribeTransactionsResult_destroy(result);
    kafka_admin_DescribeTransactionsResult_destroy(NULL);
    kafka_List_destroy(ids);
    fixture_destroy(&f);
}

/* `fenceProducers`: "Not implemented yet" per id, sorted; `producerId` and
 * `epochId` derive from the same per-id future; an empty batch is an empty
 * result whose `all()` succeeds. */
static void test_mock_admin_fence_producers_reports_unsupported_per_id(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_List_t *ids = string_list("txn-y", "txn-x", NULL);
    kafka_admin_FenceProducersOptions_t *options = kafka_admin_FenceProducersOptions_new();
    kafka_admin_FenceProducersResult_t *result = kafka_admin_Admin_fence_producers_with_options(f.admin, ids, options);
    kafka_admin_FenceProducersOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_Map_t *fenced = kafka_admin_FenceProducersResult_fenced_producers(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_Map_size(fenced));
    TEST_ASSERT_EQUAL_STRING("txn-x", (const char *)kafka_Map_key(fenced, 0));
    TEST_ASSERT_EQUAL_STRING("txn-y", (const char *)kafka_Map_key(fenced, 1));
    kafka_common_Error_t *err = get_err(future_for(fenced, "txn-x"));
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_Map_destroy(fenced);
    kafka_common_KafkaFuture_t *future = NULL;
    TEST_ASSERT_NULL(kafka_admin_FenceProducersResult_producer_id(result, "txn-y", &future));
    expect_fails_with(future, "Not implemented yet");
    TEST_ASSERT_NULL(kafka_admin_FenceProducersResult_epoch_id(result, "txn-y", &future));
    expect_fails_with(future, "Not implemented yet");
    future = NULL;
    err = kafka_admin_FenceProducersResult_producer_id(result, "txn-z", &future);
    TEST_ASSERT_NULL(future);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(err));
    kafka_common_Error_destroy(err);
    expect_fails_with(kafka_admin_FenceProducersResult_all(result), "Not implemented yet");
    kafka_admin_FenceProducersResult_destroy(result);
    kafka_admin_FenceProducersResult_destroy(NULL);
    kafka_List_destroy(ids);

    kafka_List_t *none = kafka_List_new();
    result = kafka_admin_Admin_fence_producers(f.admin, none);
    fenced = kafka_admin_FenceProducersResult_fenced_producers(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_Map_size(fenced));
    kafka_Map_destroy(fenced);
    expect_void_ok(kafka_admin_FenceProducersResult_all(result));
    kafka_admin_FenceProducersResult_destroy(result);
    kafka_List_destroy(none);
    fixture_destroy(&f);
}

/* `listTransactions`: the one future fails "Not implemented yet", and so do
 * its `byBrokerId` / `allByBrokerId` views. */
static void test_mock_admin_list_transactions_reports_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_ListTransactionsOptions_t *options = kafka_admin_ListTransactionsOptions_new();
    kafka_admin_ListTransactionsOptions_filter_on_duration(options, 1000);
    TEST_ASSERT_EQUAL_INT64(1000, kafka_admin_ListTransactionsOptions_filtered_duration(options));
    kafka_admin_ListTransactionsResult_t *result = kafka_admin_Admin_list_transactions_with_options(f.admin, options);
    kafka_admin_ListTransactionsOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(result);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListTransactionsResult_all(result);
    kafka_common_Error_t *err = get_err(all);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_common_KafkaFuture_destroy(all);
    expect_fails_with(kafka_admin_ListTransactionsResult_by_broker_id(result), "Not implemented yet");
    expect_fails_with(kafka_admin_ListTransactionsResult_all_by_broker_id(result), "Not implemented yet");
    kafka_admin_ListTransactionsResult_destroy(result);
    kafka_admin_ListTransactionsResult_destroy(NULL);
    result = kafka_admin_Admin_list_transactions(f.admin);
    expect_fails_with(kafka_admin_ListTransactionsResult_all(result), "Not implemented yet");
    kafka_admin_ListTransactionsResult_destroy(result);
    fixture_destroy(&f);
}

/* `abortTransaction` and `forceTerminateTransaction`: single-future results
 * that fail "Not implemented yet" on the mock. */
static void test_mock_admin_abort_and_terminate_transaction_report_unsupported(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_common_TopicPartition_t *tp = tp_new("txn-topic", 3);
    kafka_admin_AbortTransactionSpec_t *spec = kafka_admin_AbortTransactionSpec_new(tp, 91234, 7, 42);
    TEST_ASSERT_EQUAL_INT32(3, kafka_common_TopicPartition_partition(kafka_admin_AbortTransactionSpec_topic_partition(spec)));
    TEST_ASSERT_EQUAL_INT64(91234, kafka_admin_AbortTransactionSpec_producer_id(spec));
    TEST_ASSERT_EQUAL_INT16(7, kafka_admin_AbortTransactionSpec_producer_epoch(spec));
    TEST_ASSERT_EQUAL_INT32(42, kafka_admin_AbortTransactionSpec_coordinator_epoch(spec));
    char *text = kafka_admin_AbortTransactionSpec_to_string(spec);
    TEST_ASSERT_NOT_NULL(strstr(text, "91234"));
    kafka_string_destroy(text);
    kafka_admin_AbortTransactionOptions_t *options = kafka_admin_AbortTransactionOptions_new();
    kafka_admin_AbortTransactionResult_t *aborted = kafka_admin_Admin_abort_transaction_with_options(f.admin, spec, options);
    kafka_admin_AbortTransactionOptions_destroy(options);
    TEST_ASSERT_NOT_NULL(aborted);
    kafka_common_KafkaFuture_t *all = kafka_admin_AbortTransactionResult_all(aborted);
    kafka_common_Error_t *err = get_err(all);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_e_UNSUPPORTED_VERSION, kafka_common_Error_code(err));
    assert_error_message(err, "Not implemented yet");
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_AbortTransactionResult_destroy(aborted);
    kafka_admin_AbortTransactionResult_destroy(NULL);
    aborted = kafka_admin_Admin_abort_transaction(f.admin, spec);
    expect_fails_with(kafka_admin_AbortTransactionResult_all(aborted), "Not implemented yet");
    kafka_admin_AbortTransactionResult_destroy(aborted);

    kafka_admin_TerminateTransactionOptions_t *t_options = kafka_admin_TerminateTransactionOptions_new();
    kafka_admin_TerminateTransactionResult_t *terminated = kafka_admin_Admin_force_terminate_transaction_with_options(f.admin, "txn-a", t_options);
    kafka_admin_TerminateTransactionOptions_destroy(t_options);
    TEST_ASSERT_NOT_NULL(terminated);
    expect_fails_with(kafka_admin_TerminateTransactionResult_result(terminated), "Not implemented yet");
    kafka_admin_TerminateTransactionResult_destroy(terminated);
    kafka_admin_TerminateTransactionResult_destroy(NULL);
    terminated = kafka_admin_Admin_force_terminate_transaction(f.admin, "txn-a");
    expect_fails_with(kafka_admin_TerminateTransactionResult_result(terminated), "Not implemented yet");
    kafka_admin_TerminateTransactionResult_destroy(terminated);
    kafka_admin_AbortTransactionSpec_destroy(spec);
    kafka_common_TopicPartition_destroy(tp);
    fixture_destroy(&f);
}

/* A `_cb` registration fires exactly once, through the pump, and a second
 * pump run finds nothing left. */
static void test_mock_admin_transactions_async_fires_once(void) {
    fixture_t f;
    fixture_init(&f, 1);
    kafka_admin_ListTransactionsResult_t *result = kafka_admin_Admin_list_transactions(f.admin);
    kafka_common_KafkaFuture_t *all = kafka_admin_ListTransactionsResult_all(result);
    cb_result_t r;
    get_cb_pumped(&f, all, &r);
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT32(0, callback_pump_execute(&f.pump));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    kafka_common_KafkaFuture_destroy(all);
    kafka_admin_ListTransactionsResult_destroy(result);
    kafka_List_t *ids = string_list("txn-cb", NULL);
    kafka_admin_FenceProducersResult_t *fenced = kafka_admin_Admin_fence_producers(f.admin, ids);
    kafka_Map_t *values = kafka_admin_FenceProducersResult_fenced_producers(fenced);
    get_cb_pumped(&f, future_for(values, "txn-cb"), &r);
    TEST_ASSERT_TRUE(r.had_error);
    kafka_Map_destroy(values);
    kafka_admin_FenceProducersResult_destroy(fenced);
    kafka_List_destroy(ids);
    fixture_destroy(&f);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_mock_admin_create_close_destroy);
    RUN_TEST(test_mock_admin_rejects_zero_brokers);
    RUN_TEST(test_mock_admin_close_async);
    RUN_TEST(test_mock_admin_destroy_runs_pending_close_callback);
    RUN_TEST(test_admin_client_config_lifecycle);
    RUN_TEST(test_admin_client_create_rejects_empty_bootstrap);
    RUN_TEST(test_mock_admin_create_topics_sync);
    RUN_TEST(test_mock_admin_create_topics_partial_failure);
    RUN_TEST(test_mock_admin_per_key_futures_resolve_independently);
    RUN_TEST(test_mock_admin_create_topics_broker_defaults);
    RUN_TEST(test_mock_admin_create_topics_replicas_assignment);
    RUN_TEST(test_mock_admin_new_topic_configs);
    RUN_TEST(test_mock_admin_create_topics_async_partial_failure);
    RUN_TEST(test_mock_admin_list_topics_sync);
    RUN_TEST(test_mock_admin_list_topics_async);
    RUN_TEST(test_mock_admin_list_topics_call_error);
    RUN_TEST(test_mock_admin_describe_topics_by_names);
    RUN_TEST(test_mock_admin_describe_topics_by_ids);
    RUN_TEST(test_mock_admin_describe_topics_async);
    RUN_TEST(test_mock_admin_uuid_from_string_rejects_bad_id);
    RUN_TEST(test_mock_admin_delete_topics_by_names);
    RUN_TEST(test_mock_admin_delete_topics_by_ids);
    RUN_TEST(test_mock_admin_delete_topics_async);
    RUN_TEST(test_mock_admin_delete_topics_by_ids_async);
    RUN_TEST(test_mock_admin_create_partitions_reports_unsupported_per_topic);
    RUN_TEST(test_mock_admin_create_partitions_with_assignments_and_sorting);
    RUN_TEST(test_mock_admin_create_partitions_empty_request);
    RUN_TEST(test_mock_admin_create_partitions_async);
    RUN_TEST(test_mock_admin_delete_records_reports_unsupported_per_partition);
    RUN_TEST(test_mock_admin_delete_records_empty_request);
    RUN_TEST(test_mock_admin_delete_records_async);
    RUN_TEST(test_mock_admin_describe_cluster_sync);
    RUN_TEST(test_mock_admin_describe_cluster_call_error);
    RUN_TEST(test_mock_admin_describe_cluster_async);
    RUN_TEST(test_mock_admin_describe_configs_partial_failure);
    RUN_TEST(test_mock_admin_describe_configs_empty_batch);
    RUN_TEST(test_mock_admin_describe_configs_async_partial_failure);
    RUN_TEST(test_mock_admin_incremental_alter_configs_set_then_delete);
    RUN_TEST(test_mock_admin_incremental_alter_configs_partial_failure);
    RUN_TEST(test_mock_admin_alter_config_op_type_for_id);
    RUN_TEST(test_mock_admin_incremental_alter_configs_async);
    RUN_TEST(test_mock_admin_list_config_resources);
    RUN_TEST(test_mock_admin_list_config_resources_async);
    RUN_TEST(test_mock_admin_describe_log_dirs);
    RUN_TEST(test_mock_admin_describe_log_dirs_async);
    RUN_TEST(test_mock_admin_alter_replica_log_dirs_partial_failure);
    RUN_TEST(test_mock_admin_alter_replica_log_dirs_async);
    RUN_TEST(test_mock_admin_describe_replica_log_dirs);
    RUN_TEST(test_mock_admin_describe_replica_log_dirs_async);
    RUN_TEST(test_mock_admin_elect_leaders_reports_unsupported);
    RUN_TEST(test_mock_admin_election_type_value_of);
    RUN_TEST(test_mock_admin_elect_leaders_async);
    RUN_TEST(test_mock_admin_alter_partition_reassignments_partial_failure);
    RUN_TEST(test_mock_admin_new_partition_reassignment_rejects_empty_replicas);
    RUN_TEST(test_mock_admin_list_partition_reassignments_round_trip);
    RUN_TEST(test_mock_admin_list_partition_reassignments_after_delete_returns_error);
    RUN_TEST(test_mock_admin_alter_partition_reassignments_async);
    RUN_TEST(test_mock_admin_list_partition_reassignments_async);
    RUN_TEST(test_mock_admin_list_offsets_earliest_and_latest);
    RUN_TEST(test_mock_admin_list_offsets_for_timestamp_is_unsupported);
    RUN_TEST(test_mock_admin_list_offsets_rejects_bad_inputs);
    RUN_TEST(test_mock_admin_list_offsets_async);
    RUN_TEST(test_mock_admin_list_groups_reports_seeded_groups);
    RUN_TEST(test_mock_admin_list_groups_with_no_groups_is_empty);
    RUN_TEST(test_mock_admin_list_groups_async);
    RUN_TEST(test_mock_admin_describe_consumer_groups_reports_unsupported_per_group);
    RUN_TEST(test_mock_admin_describe_classic_groups_reports_unsupported_per_group);
    RUN_TEST(test_mock_admin_describe_consumer_groups_async);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_round_trip);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_filters_by_spec_partitions);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_rejects_a_negative_seeded_offset);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_two_groups_are_unsupported);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_async);
    RUN_TEST(test_mock_admin_alter_consumer_group_offsets_reports_unsupported_per_partition);
    RUN_TEST(test_mock_admin_alter_consumer_group_offsets_rejects_bad_input);
    RUN_TEST(test_mock_admin_alter_consumer_group_offsets_with_no_partitions_fails_the_call);
    RUN_TEST(test_mock_admin_alter_consumer_group_offsets_async);
    RUN_TEST(test_mock_admin_delete_consumer_group_offsets_reports_unsupported_per_partition);
    RUN_TEST(test_mock_admin_delete_consumer_group_offsets_async);
    RUN_TEST(test_mock_admin_delete_consumer_groups_reports_unsupported_per_group);
    RUN_TEST(test_mock_admin_delete_consumer_groups_async);
    RUN_TEST(test_mock_admin_remove_members_reports_unsupported_per_member);
    RUN_TEST(test_mock_admin_remove_all_members_has_no_per_member_outcome);
    RUN_TEST(test_mock_admin_remove_members_rejects_an_empty_member_list);
    RUN_TEST(test_mock_admin_remove_members_async);
    RUN_TEST(test_mock_admin_create_acls_reports_unsupported_per_binding);
    RUN_TEST(test_mock_admin_acl_constructors_reject_filter_values);
    RUN_TEST(test_mock_admin_create_acls_async);
    RUN_TEST(test_mock_admin_describe_acls_reports_unsupported);
    RUN_TEST(test_mock_admin_describe_acls_async);
    RUN_TEST(test_mock_admin_delete_acls_reports_unsupported_per_filter);
    RUN_TEST(test_mock_admin_delete_acls_async);
    RUN_TEST(test_mock_admin_describe_client_quotas_reports_unsupported);
    RUN_TEST(test_mock_admin_describe_client_quotas_async);
    RUN_TEST(test_mock_admin_alter_client_quotas_reports_unsupported_per_entity);
    RUN_TEST(test_mock_admin_alter_client_quotas_async);
    RUN_TEST(test_mock_admin_scram_accessors);
    RUN_TEST(test_mock_admin_alter_user_scram_credentials_reports_unsupported_per_user);
    RUN_TEST(test_mock_admin_describe_user_scram_credentials_reports_unsupported);
    RUN_TEST(test_mock_admin_user_scram_credentials_async);
    RUN_TEST(test_mock_admin_delegation_token_lifecycle);
    RUN_TEST(test_mock_admin_describe_delegation_token_filters_by_owner);
    RUN_TEST(test_mock_admin_create_delegation_token_rejects_bad_input);
    RUN_TEST(test_mock_admin_delegation_token_async);
    RUN_TEST(test_mock_admin_describe_features);
    RUN_TEST(test_mock_admin_update_features_applies_a_valid_upgrade);
    RUN_TEST(test_mock_admin_update_features_rejects_bad_updates);
    RUN_TEST(test_mock_admin_features_async);
    RUN_TEST(test_mock_admin_describe_producers_reports_unsupported_per_partition);
    RUN_TEST(test_mock_admin_describe_transactions_reports_unsupported_per_id);
    RUN_TEST(test_mock_admin_fence_producers_reports_unsupported_per_id);
    RUN_TEST(test_mock_admin_list_transactions_reports_unsupported);
    RUN_TEST(test_mock_admin_abort_and_terminate_transaction_report_unsupported);
    RUN_TEST(test_mock_admin_transactions_async_fires_once);
    return UNITY_END();
}
