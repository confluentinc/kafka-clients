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

#include <confluent_kafka.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// Numeric `Errors` codes asserted below (src/common/protocol/errors.rs).
#define UNKNOWN_TOPIC_OR_PARTITION_CODE (3)
#define UNSUPPORTED_VERSION_CODE (35)
#define TOPIC_ALREADY_EXISTS_CODE (36)
#define INVALID_REPLICATION_FACTOR_CODE (38)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/* Spins up to ~5s for `*flag` to reach `expected`. Returns 1 on success. */
static int wait_for(atomic_int *flag, int expected) {
    for (int i = 0; i < 5000; i++) {
        if (atomic_load(flag) >= expected) {
            return 1;
        }
        struct timespec ts = {0, 1000000}; /* 1ms */
        nanosleep(&ts, NULL);
    }
    return atomic_load(flag) >= expected;
}

/* Returns the index of `key` in a createTopics result, or -1. */
static int32_t find_create_key(const kafka_admin_CreateTopicsResult_t *result,
                               const char *key) {
    int32_t n = kafka_admin_CreateTopicsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *k = kafka_admin_CreateTopicsResult_get_key(result, i);
        if (k != NULL && strcmp(k, key) == 0) {
            return i;
        }
    }
    return -1;
}

/* Returns the index of `key` in a describeTopics result, or -1. */
static int32_t find_describe_key(const kafka_admin_DescribeTopicsResult_t *result,
                                 const char *key) {
    int32_t n = kafka_admin_DescribeTopicsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *k = kafka_admin_DescribeTopicsResult_get_key(result, i);
        if (k != NULL && strcmp(k, key) == 0) {
            return i;
        }
    }
    return -1;
}

/* Returns the index of `key` in a deleteTopics result, or -1. */
static int32_t find_delete_key(const kafka_admin_DeleteTopicsResult_t *result,
                               const char *key) {
    int32_t n = kafka_admin_DeleteTopicsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *k = kafka_admin_DeleteTopicsResult_get_key(result, i);
        if (k != NULL && strcmp(k, key) == 0) {
            return i;
        }
    }
    return -1;
}

/* Returns the index of `key` in a listTopics result, or -1. */
static int32_t find_list_key(const kafka_admin_ListTopicsResult_t *result,
                             const char *key) {
    int32_t n = kafka_admin_ListTopicsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *k = kafka_admin_ListTopicsResult_get_key(result, i);
        if (k != NULL && strcmp(k, key) == 0) {
            return i;
        }
    }
    return -1;
}

/* Creates one topic synchronously and asserts it succeeded. */
static void create_one(kafka_admin_AdminClient_t *admin, const char *name,
                       int32_t num_partitions, int16_t replication_factor) {
    kafka_admin_NewTopic_t *topic =
        kafka_admin_NewTopic_new(name, num_partitions, replication_factor);
    TEST_ASSERT_NOT_NULL(topic);
    const kafka_admin_NewTopic_t *topics[1] = {topic};

    kafka_admin_CreateTopicsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_create_topics(
        admin, topics, 1, -1, false, false, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    int32_t i = find_create_key(result, name);
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_error(result, i));

    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_admin_NewTopic_destroy(topic);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

static void test_mock_admin_create_close_destroy(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    TEST_ASSERT_NOT_NULL(admin);
    kafka_admin_AdminClient_close(admin, 1000);
    kafka_admin_AdminClient_destroy(admin);
}

/* At least one broker is required: the mock puts the controller and every
 * partition leader on broker 0. Java throws IndexOutOfBoundsException there
 * (MockAdminClient.java:210/:412); the FFI returns NULL instead, because a Rust
 * panic must not unwind across the C boundary. */
static void test_mock_admin_rejects_zero_brokers(void) {
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_new(0));
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_new(-1));
}

/* Async close: the callback fires exactly once, with a null error (Java's
 * Admin.close(Duration) is void). */
typedef struct {
    atomic_int fired;
    int had_error;
} op_result_t;

static void on_close(kafka_common_Error_t *error, void *user_data) {
    op_result_t *r = (op_result_t *)user_data;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_close_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    op_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_close_async(admin, 1000, on_close, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_FALSE(r.had_error);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_close_async_null_handle_fires_error(void) {
    op_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_close_async(NULL, 0, on_close, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
}

// ---------------------------------------------------------------------------
// Properties handle
// ---------------------------------------------------------------------------

static void test_admin_properties_lifecycle(void) {
    kafka_admin_AdminClientProperties_t *props = kafka_admin_AdminClientProperties_new();
    TEST_ASSERT_NOT_NULL(props);
    kafka_admin_AdminClientProperties_put(props, "bootstrap.servers", "localhost:9092");
    kafka_admin_AdminClientProperties_put(props, "client.id", "c-admin-test");
    /* Null key / value / handle are no-ops, not crashes. */
    kafka_admin_AdminClientProperties_put(props, NULL, "x");
    kafka_admin_AdminClientProperties_put(props, "x", NULL);
    kafka_admin_AdminClientProperties_put(NULL, "x", "y");
    kafka_admin_AdminClientProperties_destroy(props);
    kafka_admin_AdminClientProperties_destroy(NULL);
}

static void test_admin_properties_from_configs(void) {
    const char *configs[] = {"bootstrap.servers", "localhost:9092", NULL};
    kafka_admin_AdminClientProperties_t *props =
        kafka_admin_AdminClientProperties_from_configs(configs);
    TEST_ASSERT_NOT_NULL(props);
    kafka_admin_AdminClientProperties_destroy(props);

    /* NULL input, and an odd number of entries, both yield NULL. */
    TEST_ASSERT_NULL(kafka_admin_AdminClientProperties_from_configs(NULL));
    const char *odd[] = {"bootstrap.servers", NULL};
    TEST_ASSERT_NULL(kafka_admin_AdminClientProperties_from_configs(odd));
}

/* A production client with an unreachable broker still constructs (the address
 * is only parsed, not connected); an empty bootstrap list is rejected. */
static void test_admin_client_new_rejects_empty_bootstrap(void) {
    kafka_admin_AdminClientProperties_t *props = kafka_admin_AdminClientProperties_new();
    kafka_common_Error_t *err = NULL;
    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, &err);
    TEST_ASSERT_NULL(admin);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_destroy(err);
    kafka_admin_AdminClientProperties_destroy(props);
}

static void test_admin_client_new_null_props(void) {
    kafka_common_Error_t *err = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_new(NULL, &err));
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_destroy(err);
}

// ---------------------------------------------------------------------------
// createTopics (sync)
// ---------------------------------------------------------------------------

static void test_mock_admin_create_topics_sync(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);

    kafka_admin_NewTopic_t *t = kafka_admin_NewTopic_new("topic-a", 4, 2);
    kafka_admin_NewTopic_put_config(t, "cleanup.policy", "compact");
    const kafka_admin_NewTopic_t *topics[1] = {t};

    kafka_admin_CreateTopicsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_create_topics(
        admin, topics, 1, 5000, false, false, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_CreateTopicsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("topic-a", kafka_admin_CreateTopicsResult_get_key(result, 0));
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_error(result, 0));

    const kafka_admin_TopicMetadataAndConfig_t *mc =
        kafka_admin_CreateTopicsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(mc);
    TEST_ASSERT_NULL(kafka_admin_TopicMetadataAndConfig_error(mc));
    TEST_ASSERT_EQUAL_INT32(4, kafka_admin_TopicMetadataAndConfig_num_partitions(mc));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_TopicMetadataAndConfig_replication_factor(mc));
    /* Topic id is a non-empty base64 string (Java's Uuid.toString()). */
    const char *topic_id = kafka_admin_TopicMetadataAndConfig_topic_id(mc);
    TEST_ASSERT_NOT_NULL(topic_id);
    TEST_ASSERT_TRUE(strlen(topic_id) > 0);
    /* The config set on the NewTopic comes back on the metadata. */
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_TopicMetadataAndConfig_config_count(mc));
    TEST_ASSERT_EQUAL_STRING("cleanup.policy",
                             kafka_admin_TopicMetadataAndConfig_config_name(mc, 0));
    TEST_ASSERT_EQUAL_STRING("compact",
                             kafka_admin_TopicMetadataAndConfig_config_value(mc, 0));
    /* Out-of-range config index is null / false, not a crash. */
    TEST_ASSERT_NULL(kafka_admin_TopicMetadataAndConfig_config_name(mc, 1));
    TEST_ASSERT_NULL(kafka_admin_TopicMetadataAndConfig_config_name(mc, -1));
    TEST_ASSERT_FALSE(kafka_admin_TopicMetadataAndConfig_config_is_sensitive(mc, 7));

    /* Out-of-range result index is null, not a crash. */
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_key(result, 1));
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_value(result, 1));
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_error(result, -1));

    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_admin_NewTopic_destroy(t);
    kafka_admin_AdminClient_destroy(admin);
}

/* The whole point of per-key errors: one topic succeeds, one fails, and both
 * outcomes come back in the same result handle. */
static void test_mock_admin_create_topics_partial_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "existing", 1, 1);

    /* "existing" already exists -> TOPIC_ALREADY_EXISTS.
     * "too-many-replicas" asks for more replicas than brokers ->
     * INVALID_REPLICATION_FACTOR.
     * "fresh" succeeds. */
    kafka_admin_NewTopic_t *a = kafka_admin_NewTopic_new("existing", 1, 1);
    kafka_admin_NewTopic_t *b = kafka_admin_NewTopic_new("fresh", 2, 1);
    kafka_admin_NewTopic_t *c = kafka_admin_NewTopic_new("too-many-replicas", 1, 9);
    const kafka_admin_NewTopic_t *topics[3] = {a, b, c};

    kafka_admin_CreateTopicsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_create_topics(
        admin, topics, 3, -1, false, false, &result);
    /* A per-key failure is NOT a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_CreateTopicsResult_count(result));

    int32_t i_existing = find_create_key(result, "existing");
    int32_t i_fresh = find_create_key(result, "fresh");
    int32_t i_many = find_create_key(result, "too-many-replicas");
    TEST_ASSERT_TRUE(i_existing >= 0 && i_fresh >= 0 && i_many >= 0);

    const kafka_common_Error_t *e_existing =
        kafka_admin_CreateTopicsResult_get_error(result, i_existing);
    TEST_ASSERT_NOT_NULL(e_existing);
    TEST_ASSERT_EQUAL_INT32(TOPIC_ALREADY_EXISTS_CODE,
                            kafka_common_Error_code(e_existing));
    TEST_ASSERT_EQUAL_STRING("Topic existing exists already.",
                             kafka_common_Error_message(e_existing));
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_value(result, i_existing));

    const kafka_common_Error_t *e_many =
        kafka_admin_CreateTopicsResult_get_error(result, i_many);
    TEST_ASSERT_NOT_NULL(e_many);
    TEST_ASSERT_EQUAL_INT32(INVALID_REPLICATION_FACTOR_CODE,
                            kafka_common_Error_code(e_many));

    /* The successful key in the same batch is unaffected. */
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_error(result, i_fresh));
    const kafka_admin_TopicMetadataAndConfig_t *mc =
        kafka_admin_CreateTopicsResult_get_value(result, i_fresh);
    TEST_ASSERT_NOT_NULL(mc);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_TopicMetadataAndConfig_num_partitions(mc));

    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_admin_NewTopic_destroy(a);
    kafka_admin_NewTopic_destroy(b);
    kafka_admin_NewTopic_destroy(c);
    kafka_admin_AdminClient_destroy(admin);
}

/* Negative num_partitions / replication_factor mean "broker default"
 * (Java's NewTopic(name, Optional.empty(), Optional.empty())). The mock's
 * defaults are 1 partition and min(numBrokers, 3) replicas. */
static void test_mock_admin_create_topics_broker_defaults(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(2);
    kafka_admin_NewTopic_t *t = kafka_admin_NewTopic_new("defaults", -1, -1);
    const kafka_admin_NewTopic_t *topics[1] = {t};

    kafka_admin_CreateTopicsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_topics(admin, topics, 1, -1, false,
                                                           false, &result));
    const kafka_admin_TopicMetadataAndConfig_t *mc =
        kafka_admin_CreateTopicsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(mc);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_TopicMetadataAndConfig_num_partitions(mc));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_TopicMetadataAndConfig_replication_factor(mc));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicMetadataAndConfig_config_count(mc));

    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_admin_NewTopic_destroy(t);
    kafka_admin_AdminClient_destroy(admin);
}

/* A replica assignment switches the request to Java's
 * NewTopic(name, Map<Integer, List<Integer>>) form, in which numPartitions and
 * replicationFactor are left unset (-1).
 *
 * The mock then applies its DEFAULTS rather than honoring the assignment:
 * MockAdminClient.createTopics reads only newTopic.replicationFactor() and
 * newTopic.numPartitions(), substituting defaultReplicationFactor /
 * defaultPartitions for -1, and never looks at replicasAssignments
 * (MockAdminClient.java:388-411 at kafka a18251bae0b8). So with 3 brokers the
 * topic comes back with 1 partition and replication factor 3, and this test
 * pins the marshaling path (the assignment is accepted, the call succeeds)
 * rather than an assignment the mock does not implement. */
static void test_mock_admin_create_topics_replicas_assignment(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    kafka_admin_NewTopic_t *t = kafka_admin_NewTopic_new("assigned", -1, -1);
    int32_t brokers0[] = {0, 1};
    int32_t brokers1[] = {1, 2};
    kafka_admin_NewTopic_set_replicas_assignment(t, 0, brokers0, 2);
    kafka_admin_NewTopic_set_replicas_assignment(t, 1, brokers1, 2);
    const kafka_admin_NewTopic_t *topics[1] = {t};

    kafka_admin_CreateTopicsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_topics(admin, topics, 1, -1, false,
                                                           false, &result));
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_error(result, 0));
    const kafka_admin_TopicMetadataAndConfig_t *mc =
        kafka_admin_CreateTopicsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(mc);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_TopicMetadataAndConfig_num_partitions(mc));
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_TopicMetadataAndConfig_replication_factor(mc));

    /* describeTopics agrees with the metadata the create call reported. */
    const char *names[1] = {"assigned"};
    kafka_admin_DescribeTopicsResult_t *described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_topics(admin, names, 1, -1, false,
                                                             -1, &described));
    const kafka_admin_TopicDescription_t *d =
        kafka_admin_DescribeTopicsResult_get_value(described, 0);
    TEST_ASSERT_NOT_NULL(d);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_TopicDescription_partition_count(d));
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_TopicPartitionInfo_replica_count(
        kafka_admin_TopicDescription_partition(d, 0)));

    kafka_admin_DescribeTopicsResult_destroy(described);
    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_admin_NewTopic_destroy(t);
    kafka_admin_AdminClient_destroy(admin);
}

/* NewTopic_new(NULL, ..) returns NULL; NULL entries in the topics array are
 * skipped rather than dereferenced. */
static void test_mock_admin_new_topic_null_handling(void) {
    TEST_ASSERT_NULL(kafka_admin_NewTopic_new(NULL, 1, 1));
    kafka_admin_NewTopic_destroy(NULL);
    kafka_admin_NewTopic_put_config(NULL, "k", "v");
    kafka_admin_NewTopic_set_replicas_assignment(NULL, 0, NULL, 0);

    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    kafka_admin_NewTopic_t *t = kafka_admin_NewTopic_new("only-one", 1, 1);
    const kafka_admin_NewTopic_t *topics[2] = {NULL, t};
    kafka_admin_CreateTopicsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_topics(admin, topics, 2, -1, false,
                                                           false, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_CreateTopicsResult_count(result));
    kafka_admin_CreateTopicsResult_destroy(result);
    kafka_admin_NewTopic_destroy(t);
    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// createTopics (async)
// ---------------------------------------------------------------------------

/* Phase A: create_topics_async fires once PER KEY, independently, as that
 * key's own future resolves - not once for the whole batch. `error_count` /
 * `value_count` are generic (any key); the `_for_existing` / `_for_fresh`
 * fields pin the two specific keys this test cares about. */
typedef struct {
    atomic_int fired;
    atomic_int error_count;
    atomic_int value_count;
    int had_error_for_existing;
    int32_t error_code_for_existing;
    int had_value_for_fresh;
    int32_t partitions_for_fresh;
} create_async_result_t;

static void on_create(const char *key, kafka_admin_TopicMetadataAndConfig_t *value,
                      kafka_common_Error_t *error, void *user_data) {
    create_async_result_t *r = (create_async_result_t *)user_data;
    if (error != NULL) {
        atomic_fetch_add(&r->error_count, 1);
        if (key != NULL && strcmp(key, "existing") == 0) {
            r->had_error_for_existing = 1;
            r->error_code_for_existing = kafka_common_Error_code(error);
        }
        kafka_common_Error_destroy(error);
    }
    if (value != NULL) {
        atomic_fetch_add(&r->value_count, 1);
        if (key != NULL && strcmp(key, "fresh-async") == 0) {
            r->had_value_for_fresh = 1;
            r->partitions_for_fresh = kafka_admin_TopicMetadataAndConfig_num_partitions(value);
        }
        kafka_admin_TopicMetadataAndConfig_destroy(value);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_create_topics_async_partial_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "existing", 1, 1);

    kafka_admin_NewTopic_t *a = kafka_admin_NewTopic_new("existing", 1, 1);
    kafka_admin_NewTopic_t *b = kafka_admin_NewTopic_new("fresh-async", 3, 1);
    const kafka_admin_NewTopic_t *topics[2] = {a, b};

    create_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    atomic_init(&r.value_count, 0);
    kafka_admin_AdminClient_create_topics_async(admin, topics, 2, -1, false, false,
                                                on_create, &r);
    /* Two keys -> two independent callback invocations. */
    TEST_ASSERT_TRUE(wait_for(&r.fired, 2));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.error_count));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.value_count));
    TEST_ASSERT_TRUE(r.had_error_for_existing);
    TEST_ASSERT_EQUAL_INT32(TOPIC_ALREADY_EXISTS_CODE, r.error_code_for_existing);
    TEST_ASSERT_TRUE(r.had_value_for_fresh);
    TEST_ASSERT_EQUAL_INT32(3, r.partitions_for_fresh);

    kafka_admin_NewTopic_destroy(a);
    kafka_admin_NewTopic_destroy(b);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle fans the same error out over every requested key, one
 * callback per key - the same cardinality as a successful submission, so a
 * caller that already built one Future per key still gets each one resolved. */
static void test_mock_admin_create_topics_async_null_handle(void) {
    kafka_admin_NewTopic_t *a = kafka_admin_NewTopic_new("a", 1, 1);
    kafka_admin_NewTopic_t *b = kafka_admin_NewTopic_new("b", 1, 1);
    const kafka_admin_NewTopic_t *topics[2] = {a, b};

    create_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    atomic_init(&r.value_count, 0);
    kafka_admin_AdminClient_create_topics_async(NULL, topics, 2, -1, false, false,
                                                on_create, &r);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.error_count));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&r.value_count));

    kafka_admin_NewTopic_destroy(a);
    kafka_admin_NewTopic_destroy(b);
}

// ---------------------------------------------------------------------------
// listTopics
// ---------------------------------------------------------------------------

static void test_mock_admin_list_topics_sync(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "beta", 1, 1);
    create_one(admin, "alpha", 1, 1);

    kafka_admin_ListTopicsResult_t *result = NULL;
    kafka_common_Error_t *err =
        kafka_admin_AdminClient_list_topics(admin, -1, true, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_ListTopicsResult_count(result));
    /* Entries are sorted by name, so indexing is reproducible. */
    TEST_ASSERT_EQUAL_STRING("alpha", kafka_admin_ListTopicsResult_get_key(result, 0));
    TEST_ASSERT_EQUAL_STRING("beta", kafka_admin_ListTopicsResult_get_key(result, 1));

    const kafka_admin_TopicListing_t *listing =
        kafka_admin_ListTopicsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(listing);
    TEST_ASSERT_EQUAL_STRING("alpha", kafka_admin_TopicListing_name(listing));
    TEST_ASSERT_FALSE(kafka_admin_TopicListing_is_internal(listing));
    TEST_ASSERT_TRUE(strlen(kafka_admin_TopicListing_topic_id(listing)) > 0);

    TEST_ASSERT_NULL(kafka_admin_ListTopicsResult_get_value(result, 2));
    TEST_ASSERT_NULL(kafka_admin_ListTopicsResult_get_key(result, -1));

    kafka_admin_ListTopicsResult_destroy(result);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} list_async_result_t;

static void on_list(kafka_admin_ListTopicsResult_t *result,
                    kafka_common_Error_t *error, void *user_data) {
    list_async_result_t *r = (list_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListTopicsResult_count(result);
        kafka_admin_ListTopicsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_topics_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "one", 1, 1);

    list_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_topics_async(admin, -1, true, on_list, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    kafka_admin_AdminClient_destroy(admin);
}

/* `timeout_next_request` makes the mock fail the next RPC. For listTopics that
 * is a whole-call failure (Java has one future), so it surfaces as the call's
 * error rather than a per-key error. */
static void test_mock_admin_list_topics_call_error(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    kafka_admin_MockAdminClient_timeout_next_request(admin, 1);

    kafka_admin_ListTopicsResult_t *result = NULL;
    kafka_common_Error_t *err =
        kafka_admin_AdminClient_list_topics(admin, -1, true, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("The mock timed out the request.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// describeTopics
// ---------------------------------------------------------------------------

static void test_mock_admin_describe_topics_by_names(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "described", 2, 2);

    const char *names[2] = {"described", "missing"};
    kafka_admin_DescribeTopicsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_describe_topics(
        admin, names, 2, -1, false, -1, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DescribeTopicsResult_count(result));

    int32_t i_ok = find_describe_key(result, "described");
    int32_t i_bad = find_describe_key(result, "missing");
    TEST_ASSERT_TRUE(i_ok >= 0 && i_bad >= 0);

    /* Partial failure: one description, one per-key error. */
    const kafka_common_Error_t *e =
        kafka_admin_DescribeTopicsResult_get_error(result, i_bad);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE,
                            kafka_common_Error_code(e));
    TEST_ASSERT_EQUAL_STRING("Topic missing not found.", kafka_common_Error_message(e));
    TEST_ASSERT_NULL(kafka_admin_DescribeTopicsResult_get_value(result, i_bad));

    TEST_ASSERT_NULL(kafka_admin_DescribeTopicsResult_get_error(result, i_ok));
    const kafka_admin_TopicDescription_t *d =
        kafka_admin_DescribeTopicsResult_get_value(result, i_ok);
    TEST_ASSERT_NOT_NULL(d);
    TEST_ASSERT_EQUAL_STRING("described", kafka_admin_TopicDescription_name(d));
    TEST_ASSERT_FALSE(kafka_admin_TopicDescription_is_internal(d));
    TEST_ASSERT_TRUE(strlen(kafka_admin_TopicDescription_topic_id(d)) > 0);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_TopicDescription_partition_count(d));
    /* The mock passes Collections.emptySet(), so the operations were *reported*
     * and merely empty. The count is 0 either way -- it is never negative -- so
     * the presence bit is what separates this from Java's null. */
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicDescription_authorized_operation_count(d));
    TEST_ASSERT_TRUE(kafka_admin_TopicDescription_has_authorized_operations(d));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_TopicDescription_authorized_operation(d, 0));

    const kafka_admin_TopicPartitionInfo_t *p0 =
        kafka_admin_TopicDescription_partition(d, 0);
    TEST_ASSERT_NOT_NULL(p0);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicPartitionInfo_partition(p0));
    /* The mock puts every partition's leader on broker 0 and its replicas on
     * the first `replicationFactor` brokers. */
    const kafka_common_Node_t *leader = kafka_admin_TopicPartitionInfo_leader(p0);
    TEST_ASSERT_NOT_NULL(leader);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Node_id(leader));
    TEST_ASSERT_EQUAL_INT32(1000, kafka_common_Node_port(leader));
    int32_t host_len = 0;
    const char *host = kafka_common_Node_host(leader, &host_len);
    TEST_ASSERT_EQUAL_INT32(9, host_len);
    TEST_ASSERT_EQUAL_INT(0, strncmp(host, "localhost", 9));

    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_TopicPartitionInfo_replica_count(p0));
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_Node_id(
        kafka_admin_TopicPartitionInfo_replica(p0, 1)));
    TEST_ASSERT_NULL(kafka_admin_TopicPartitionInfo_replica(p0, 2));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicPartitionInfo_isr_count(p0));
    /* The mock reports an empty (not absent) ELR set: count 0 with the presence
     * bit set. An absent set would also count 0, with the bit clear. */
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicPartitionInfo_elr_count(p0));
    TEST_ASSERT_TRUE(kafka_admin_TopicPartitionInfo_has_elr(p0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicPartitionInfo_last_known_elr_count(p0));
    TEST_ASSERT_TRUE(kafka_admin_TopicPartitionInfo_has_last_known_elr(p0));
    TEST_ASSERT_NULL(kafka_admin_TopicPartitionInfo_elr(p0, 0));

    TEST_ASSERT_NULL(kafka_admin_TopicDescription_partition(d, 2));
    TEST_ASSERT_NULL(kafka_admin_TopicDescription_partition(d, -1));

    kafka_admin_DescribeTopicsResult_destroy(result);
    kafka_admin_AdminClient_destroy(admin);
}

/* describeTopics by id: the keys are the base64 topic ids that listTopics
 * reported, and an unknown id yields a per-key error. */
static void test_mock_admin_describe_topics_by_ids(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "by-id", 1, 1);

    kafka_admin_ListTopicsResult_t *listed = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_topics(admin, -1, true, &listed));
    int32_t i = find_list_key(listed, "by-id");
    TEST_ASSERT_TRUE(i >= 0);
    const char *topic_id = kafka_admin_TopicListing_topic_id(
        kafka_admin_ListTopicsResult_get_value(listed, i));
    /* Copy: the borrowed string dies with `listed`. */
    char id_copy[64];
    strncpy(id_copy, topic_id, sizeof(id_copy) - 1);
    id_copy[sizeof(id_copy) - 1] = '\0';
    kafka_admin_ListTopicsResult_destroy(listed);

    const char *ids[1] = {id_copy};
    kafka_admin_DescribeTopicsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_topics_by_ids(admin, ids, 1, -1,
                                                                    false, -1, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeTopicsResult_count(result));
    TEST_ASSERT_EQUAL_STRING(id_copy, kafka_admin_DescribeTopicsResult_get_key(result, 0));
    TEST_ASSERT_NULL(kafka_admin_DescribeTopicsResult_get_error(result, 0));
    const kafka_admin_TopicDescription_t *d =
        kafka_admin_DescribeTopicsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(d);
    TEST_ASSERT_EQUAL_STRING("by-id", kafka_admin_TopicDescription_name(d));
    TEST_ASSERT_EQUAL_STRING(id_copy, kafka_admin_TopicDescription_topic_id(d));
    kafka_admin_DescribeTopicsResult_destroy(result);

    /* An unparseable id fails the whole call (Java's Uuid.fromString throws). */
    const char *bad_ids[1] = {"not-a-valid-base64-uuid-at-all-really"};
    kafka_admin_DescribeTopicsResult_t *bad_result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_describe_topics_by_ids(
        admin, bad_ids, 1, -1, false, -1, &bad_result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(bad_result);
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* Phase A: describe_topics_async / describe_topics_by_ids_async fire once
 * per key, independently. */
typedef struct {
    atomic_int fired;
    atomic_int error_count;
    atomic_int value_count;
    int32_t partition_count;
} describe_async_result_t;

static void on_describe(const char *key, kafka_admin_TopicDescription_t *value,
                        kafka_common_Error_t *error, void *user_data) {
    describe_async_result_t *r = (describe_async_result_t *)user_data;
    (void)key;
    if (error != NULL) {
        atomic_fetch_add(&r->error_count, 1);
        kafka_common_Error_destroy(error);
    }
    if (value != NULL) {
        atomic_fetch_add(&r->value_count, 1);
        r->partition_count = kafka_admin_TopicDescription_partition_count(value);
        kafka_admin_TopicDescription_destroy(value);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_topics_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "async-described", 5, 1);

    const char *names[1] = {"async-described"};
    describe_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    atomic_init(&r.value_count, 0);
    kafka_admin_AdminClient_describe_topics_async(admin, names, 1, -1, false, -1,
                                                  on_describe, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.value_count));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&r.error_count));
    TEST_ASSERT_EQUAL_INT32(5, r.partition_count);
    kafka_admin_AdminClient_destroy(admin);
}

/* An unparseable id in the async by-ids path fires the callback inline with the
 * error and never submits the RPC. */
static void test_mock_admin_describe_topics_by_ids_async_bad_id(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *bad_ids[1] = {"###"};
    describe_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    atomic_init(&r.value_count, 0);
    kafka_admin_AdminClient_describe_topics_by_ids_async(admin, bad_ids, 1, -1, false,
                                                         -1, on_describe, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.error_count));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&r.value_count));
    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// deleteTopics
// ---------------------------------------------------------------------------

static void test_mock_admin_delete_topics_by_names(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "doomed", 1, 1);

    const char *names[2] = {"doomed", "never-existed"};
    kafka_admin_DeleteTopicsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_delete_topics(
        admin, names, 2, -1, false, &result);
    /* Partial failure is not a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DeleteTopicsResult_count(result));

    int32_t i_ok = find_delete_key(result, "doomed");
    int32_t i_bad = find_delete_key(result, "never-existed");
    TEST_ASSERT_TRUE(i_ok >= 0 && i_bad >= 0);
    TEST_ASSERT_NULL(kafka_admin_DeleteTopicsResult_get_error(result, i_ok));

    const kafka_common_Error_t *e =
        kafka_admin_DeleteTopicsResult_get_error(result, i_bad);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE,
                            kafka_common_Error_code(e));
    TEST_ASSERT_EQUAL_STRING("Topic never-existed does not exist.",
                             kafka_common_Error_message(e));
    TEST_ASSERT_NULL(kafka_admin_DeleteTopicsResult_get_error(result, 5));
    kafka_admin_DeleteTopicsResult_destroy(result);

    /* The deleted topic is gone from listTopics. */
    kafka_admin_ListTopicsResult_t *listed = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_topics(admin, -1, true, &listed));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListTopicsResult_count(listed));
    kafka_admin_ListTopicsResult_destroy(listed);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_delete_topics_by_ids(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "id-doomed", 1, 1);

    kafka_admin_ListTopicsResult_t *listed = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_topics(admin, -1, true, &listed));
    const char *topic_id = kafka_admin_TopicListing_topic_id(
        kafka_admin_ListTopicsResult_get_value(listed, 0));
    char id_copy[64];
    strncpy(id_copy, topic_id, sizeof(id_copy) - 1);
    id_copy[sizeof(id_copy) - 1] = '\0';
    kafka_admin_ListTopicsResult_destroy(listed);

    const char *ids[1] = {id_copy};
    kafka_admin_DeleteTopicsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_topics_by_ids(admin, ids, 1, -1,
                                                                  false, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DeleteTopicsResult_count(result));
    TEST_ASSERT_EQUAL_STRING(id_copy, kafka_admin_DeleteTopicsResult_get_key(result, 0));
    TEST_ASSERT_NULL(kafka_admin_DeleteTopicsResult_get_error(result, 0));
    kafka_admin_DeleteTopicsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

/* Phase A: delete_topics_async / delete_topics_by_ids_async fire once per
 * key, independently. Java's per-key future is KafkaFuture<Void>, so the
 * callback carries no value parameter - a null error is the success signal. */
typedef struct {
    atomic_int fired;
    atomic_int error_count;
    int had_error_for_missing;
    int32_t error_code_for_missing;
} delete_async_result_t;

static void on_delete(const char *key, kafka_common_Error_t *error, void *user_data) {
    delete_async_result_t *r = (delete_async_result_t *)user_data;
    if (error != NULL) {
        atomic_fetch_add(&r->error_count, 1);
        if (key != NULL && strcmp(key, "nope") == 0) {
            r->had_error_for_missing = 1;
            r->error_code_for_missing = kafka_common_Error_code(error);
        }
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_delete_topics_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "gone-async", 1, 1);

    const char *names[2] = {"gone-async", "nope"};
    delete_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    kafka_admin_AdminClient_delete_topics_async(admin, names, 2, -1, false, on_delete, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 2));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.error_count));
    TEST_ASSERT_TRUE(r.had_error_for_missing);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, r.error_code_for_missing);
    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_delete_topics_by_ids_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *bad_ids[1] = {"@@@@"};
    delete_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    kafka_admin_AdminClient_delete_topics_by_ids_async(admin, bad_ids, 1, -1, false,
                                                        on_delete, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.error_count));
    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// createPartitions
//
// Java's MockAdminClient.createPartitions throws
// UnsupportedOperationException("Not implemented yet")
// (MockAdminClient.java:626-628 at kafka a18251bae0b8). Per
// .claude/rules/admin-client.md §9 the Rust mock represents that as a per-key
// KafkaError::unsupported_version("Not implemented yet") rather than a panic,
// which must not unwind across the C boundary. So the mock can exercise the
// full marshaling + result-flattening path, and the outcome asserted below is
// that faithful "unsupported" error rather than a created partition.
// ---------------------------------------------------------------------------

static void test_mock_admin_create_partitions_reports_unsupported_per_topic(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "grow-me", 1, 1);

    kafka_admin_NewPartitions_t *np = kafka_admin_NewPartitions_new(4, false);
    TEST_ASSERT_NOT_NULL(np);
    const char *topics[1] = {"grow-me"};
    const kafka_admin_NewPartitions_t *specs[1] = {np};

    kafka_admin_CreatePartitionsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_create_partitions(
        admin, topics, specs, 1, 5000, false, true, &result);
    /* A per-topic failure is NOT a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_CreatePartitionsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("grow-me",
                             kafka_admin_CreatePartitionsResult_get_key(result, 0));

    const kafka_common_Error_t *e =
        kafka_admin_CreatePartitionsResult_get_error(result, 0);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_Error_code(e));
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));

    /* Out-of-range indices are null / no crash. */
    TEST_ASSERT_NULL(kafka_admin_CreatePartitionsResult_get_key(result, 1));
    TEST_ASSERT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, 1));
    TEST_ASSERT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, -1));

    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_admin_CreatePartitionsResult_destroy(NULL);
    kafka_admin_NewPartitions_destroy(np);
    kafka_admin_AdminClient_destroy(admin);
}

/* The `has_assignments` flag picks between Java's
 * NewPartitions.increaseTo(totalCount) and
 * increaseTo(totalCount, newAssignments) forms. The mock rejects the RPC either
 * way, so this pins the marshaling path: all three shapes -- no list, a
 * populated list and a present-but-EMPTY list -- are accepted, entries stay
 * sorted by topic name, and every key gets an outcome. Which Java factory each
 * one selected is not observable through the mock; that is pinned by
 * `new_partitions_builder_distinguishes_an_absent_assignment_list_from_an_empty_one`
 * in src/ffi/admin.rs, and end to end by the `create_partitions_with_an_empty_assignment_list_is_rejected`
 * harness scenario against a real broker. */
static void test_mock_admin_create_partitions_with_assignments_and_sorting(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);

    kafka_admin_NewPartitions_t *plain = kafka_admin_NewPartitions_new(2, false);
    kafka_admin_NewPartitions_t *assigned = kafka_admin_NewPartitions_new(3, true);
    /* has_assignments with zero appended lists: Java's legal
     * increaseTo(n, emptyList()), which `is_empty()` could not express. */
    kafka_admin_NewPartitions_t *empty_list = kafka_admin_NewPartitions_new(4, true);
    int32_t brokers0[] = {0, 1};
    int32_t brokers1[] = {1, 2};
    kafka_admin_NewPartitions_add_assignment(assigned, brokers0, 2);
    kafka_admin_NewPartitions_add_assignment(assigned, brokers1, 2);
    /* Null handle / null broker array are no-ops, not crashes -- and in
     * particular the null-broker-array call must not turn `empty_list` into a
     * one-element list. */
    kafka_admin_NewPartitions_add_assignment(NULL, brokers0, 2);
    kafka_admin_NewPartitions_add_assignment(assigned, NULL, 2);
    kafka_admin_NewPartitions_add_assignment(empty_list, NULL, 2);

    /* Deliberately unsorted input; the result is sorted by topic name. */
    const char *topics[3] = {"zeta", "alpha", "mu"};
    const kafka_admin_NewPartitions_t *specs[3] = {plain, assigned, empty_list};

    kafka_admin_CreatePartitionsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_partitions(admin, topics, specs, 3,
                                                               -1, false, true, &result));
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_CreatePartitionsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("alpha", kafka_admin_CreatePartitionsResult_get_key(result, 0));
    TEST_ASSERT_EQUAL_STRING("mu", kafka_admin_CreatePartitionsResult_get_key(result, 1));
    TEST_ASSERT_EQUAL_STRING("zeta", kafka_admin_CreatePartitionsResult_get_key(result, 2));
    TEST_ASSERT_NOT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, 0));
    TEST_ASSERT_NOT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, 1));
    TEST_ASSERT_NOT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, 2));

    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_admin_NewPartitions_destroy(plain);
    kafka_admin_NewPartitions_destroy(assigned);
    kafka_admin_NewPartitions_destroy(empty_list);
    kafka_admin_NewPartitions_destroy(NULL);
    kafka_admin_AdminClient_destroy(admin);
}

/* An empty batch, and pairs where either side is NULL: the pair is skipped as a
 * unit so the two parallel arrays cannot drift out of step. */
static void test_mock_admin_create_partitions_null_and_empty_handling(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    kafka_admin_CreatePartitionsResult_t *empty = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_partitions(admin, NULL, NULL, 0, -1,
                                                               false, true, &empty));
    TEST_ASSERT_NOT_NULL(empty);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_CreatePartitionsResult_count(empty));
    kafka_admin_CreatePartitionsResult_destroy(empty);

    kafka_admin_NewPartitions_t *np = kafka_admin_NewPartitions_new(2, false);
    /* Entry 0 has a NULL spec, entry 1 a NULL name: both pairs are dropped,
     * leaving only entry 2. */
    const char *topics[3] = {"dropped-spec", NULL, "kept"};
    const kafka_admin_NewPartitions_t *specs[3] = {NULL, np, np};

    kafka_admin_CreatePartitionsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_partitions(admin, topics, specs, 3,
                                                               -1, false, true, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_CreatePartitionsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("kept", kafka_admin_CreatePartitionsResult_get_key(result, 0));

    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_admin_NewPartitions_destroy(np);
    kafka_admin_AdminClient_destroy(admin);
}

/* Phase A: create_partitions_async fires once per key, independently. Java's
 * per-key future is KafkaFuture<Void>, so the callback carries no value
 * parameter. */
typedef struct {
    atomic_int fired;
    atomic_int error_count;
    int32_t error_code_for_first;
} create_partitions_async_result_t;

static void on_create_partitions(const char *key, kafka_common_Error_t *error,
                                 void *user_data) {
    create_partitions_async_result_t *r = (create_partitions_async_result_t *)user_data;
    (void)key;
    if (error != NULL) {
        atomic_fetch_add(&r->error_count, 1);
        r->error_code_for_first = kafka_common_Error_code(error);
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_create_partitions_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    kafka_admin_NewPartitions_t *np = kafka_admin_NewPartitions_new(6, false);
    const char *topics[1] = {"async-grow"};
    const kafka_admin_NewPartitions_t *specs[1] = {np};

    create_partitions_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    kafka_admin_AdminClient_create_partitions_async(admin, topics, specs, 1, -1, false,
                                                    true, on_create_partitions, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.error_count));
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, r.error_code_for_first);

    kafka_admin_NewPartitions_destroy(np);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error -
 * fanned out over every requested key (Phase A), not just once for the call. */
static void test_mock_admin_create_partitions_async_null_handle(void) {
    kafka_admin_NewPartitions_t *np1 = kafka_admin_NewPartitions_new(2, false);
    kafka_admin_NewPartitions_t *np2 = kafka_admin_NewPartitions_new(3, false);
    const char *topics[2] = {"a", "b"};
    const kafka_admin_NewPartitions_t *specs[2] = {np1, np2};

    create_partitions_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    kafka_admin_AdminClient_create_partitions_async(NULL, topics, specs, 2, -1, false, true,
                                                    on_create_partitions, &r);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.error_count));

    kafka_admin_NewPartitions_destroy(np1);
    kafka_admin_NewPartitions_destroy(np2);
}

// ---------------------------------------------------------------------------
// deleteRecords
//
// Java's MockAdminClient.deleteRecords returns an empty result for an empty
// request and otherwise throws UnsupportedOperationException("Not implemented
// yet") (MockAdminClient.java:630-638 at kafka a18251bae0b8). The Rust mock
// mirrors both halves: an empty map yields an empty result, and each requested
// partition otherwise gets an "unsupported" per-key error.
// ---------------------------------------------------------------------------

static void test_mock_admin_delete_records_reports_unsupported_per_partition(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "trimmed", 2, 1);

    /* Deliberately unsorted; entries come back sorted by (topic, partition).
     * -1 is Java's documented "truncate to the high watermark". */
    const char *topics[3] = {"trimmed", "another", "trimmed"};
    const int32_t partitions[3] = {1, 0, 0};
    const int64_t offsets[3] = {5, -1, 10};

    kafka_admin_DeleteRecordsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_delete_records(
        admin, topics, partitions, offsets, 3, 5000, &result);
    /* A per-partition failure is NOT a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_DeleteRecordsResult_count(result));

    TEST_ASSERT_EQUAL_STRING("another", kafka_admin_DeleteRecordsResult_get_topic(result, 0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteRecordsResult_get_partition(result, 0));
    TEST_ASSERT_EQUAL_STRING("trimmed", kafka_admin_DeleteRecordsResult_get_topic(result, 1));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteRecordsResult_get_partition(result, 1));
    TEST_ASSERT_EQUAL_STRING("trimmed", kafka_admin_DeleteRecordsResult_get_topic(result, 2));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DeleteRecordsResult_get_partition(result, 2));

    for (int32_t i = 0; i < 3; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_DeleteRecordsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_Error_code(e));
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
        /* A failed partition has no low watermark. */
        TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_DeleteRecordsResult_get_low_watermark(result, i));
    }

    /* Out-of-range indices: null / -1, no crash. */
    TEST_ASSERT_NULL(kafka_admin_DeleteRecordsResult_get_topic(result, 3));
    TEST_ASSERT_NULL(kafka_admin_DeleteRecordsResult_get_topic(result, -1));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_DeleteRecordsResult_get_partition(result, 3));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_DeleteRecordsResult_get_partition(result, -1));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_DeleteRecordsResult_get_low_watermark(result, 3));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_DeleteRecordsResult_get_low_watermark(result, -1));
    TEST_ASSERT_NULL(kafka_admin_DeleteRecordsResult_get_error(result, 3));
    TEST_ASSERT_NULL(kafka_admin_DeleteRecordsResult_get_error(result, -1));

    kafka_admin_DeleteRecordsResult_destroy(result);
    kafka_admin_DeleteRecordsResult_destroy(NULL);
    kafka_admin_AdminClient_destroy(admin);
}

/* Java returns an empty DeleteRecordsResult for an empty request instead of
 * throwing (MockAdminClient.java:632-635), so the empty call succeeds with no
 * per-key errors at all. NULL topic entries are skipped. */
static void test_mock_admin_delete_records_empty_and_null_handling(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    kafka_admin_DeleteRecordsResult_t *empty = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_records(admin, NULL, NULL, NULL, 0,
                                                            -1, &empty));
    TEST_ASSERT_NOT_NULL(empty);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteRecordsResult_count(empty));
    kafka_admin_DeleteRecordsResult_destroy(empty);

    const char *topics[2] = {NULL, "kept"};
    const int32_t partitions[2] = {0, 3};
    const int64_t offsets[2] = {1, 2};
    kafka_admin_DeleteRecordsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_records(admin, topics, partitions,
                                                            offsets, 2, -1, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DeleteRecordsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("kept", kafka_admin_DeleteRecordsResult_get_topic(result, 0));
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_DeleteRecordsResult_get_partition(result, 0));
    kafka_admin_DeleteRecordsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

/* Phase A: delete_records_async fires once per key, independently. The key is
 * a TopicPartition, delivered as a topic name plus a partition id rather than
 * through a single opaque key handle. */
typedef struct {
    atomic_int fired;
    atomic_int error_count;
    atomic_int value_count;
    int32_t error_code_for_first;
    char topic_for_first[64];
    int32_t partition_for_first;
} delete_records_async_result_t;

static void on_delete_records(const char *topic, int32_t partition,
                              kafka_admin_DeletedRecords_t *value,
                              kafka_common_Error_t *error, void *user_data) {
    delete_records_async_result_t *r = (delete_records_async_result_t *)user_data;
    if (topic != NULL) {
        strncpy(r->topic_for_first, topic, sizeof(r->topic_for_first) - 1);
        r->topic_for_first[sizeof(r->topic_for_first) - 1] = '\0';
    }
    r->partition_for_first = partition;
    if (error != NULL) {
        atomic_fetch_add(&r->error_count, 1);
        r->error_code_for_first = kafka_common_Error_code(error);
        kafka_common_Error_destroy(error);
    }
    if (value != NULL) {
        atomic_fetch_add(&r->value_count, 1);
        kafka_admin_DeletedRecords_destroy(value);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_delete_records_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *topics[1] = {"async-trim"};
    const int32_t partitions[1] = {0};
    const int64_t offsets[1] = {7};

    delete_records_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    atomic_init(&r.value_count, 0);
    kafka_admin_AdminClient_delete_records_async(admin, topics, partitions, offsets, 1,
                                                 -1, on_delete_records, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.error_count));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&r.value_count));
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, r.error_code_for_first);
    TEST_ASSERT_EQUAL_STRING("async-trim", r.topic_for_first);
    TEST_ASSERT_EQUAL_INT32(0, r.partition_for_first);

    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error -
 * fanned out over every requested key (Phase A), not just once for the call. */
static void test_mock_admin_delete_records_async_null_handle(void) {
    const char *topics[2] = {"a", "b"};
    const int32_t partitions[2] = {0, 1};
    const int64_t offsets[2] = {1, 2};

    delete_records_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    atomic_init(&r.error_count, 0);
    atomic_init(&r.value_count, 0);
    kafka_admin_AdminClient_delete_records_async(NULL, topics, partitions, offsets, 2, -1,
                                                 on_delete_records, &r);
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(2, atomic_load(&r.error_count));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&r.value_count));
}

// ---------------------------------------------------------------------------
// A NULL out_result means the caller does not want the result, so the handle is
// never built (see finish_sync). It must not leak or crash.
// ---------------------------------------------------------------------------

static void test_mock_admin_null_out_result(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    kafka_admin_NewTopic_t *t = kafka_admin_NewTopic_new("ignored-result", 1, 1);
    const kafka_admin_NewTopic_t *topics[1] = {t};
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_create_topics(admin, topics, 1, -1, false, false, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_topics(admin, -1, true, NULL));
    kafka_admin_NewTopic_destroy(t);
    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// B2 — cluster, configs and log dirs
//
// `ConfigResource.Type.id()` codes (src/common/config/config_resource.rs, which
// mirrors Java's `ConfigResource.Type`).
// ---------------------------------------------------------------------------

#define RESOURCE_TYPE_TOPIC (2)
#define RESOURCE_TYPE_BROKER (4)
#define RESOURCE_TYPE_BROKER_LOGGER (8)
#define RESOURCE_TYPE_CLIENT_METRICS (16)
#define RESOURCE_TYPE_GROUP (32)

/* `AlterConfigOp.OpType.id()` codes (src/admin/alter_config_op.rs). */
#define OP_TYPE_SET (0)
#define OP_TYPE_DELETE (1)
#define OP_TYPE_APPEND (2)

/* More numeric `Errors` codes asserted below (src/common/protocol/errors.rs). */
#define REPLICA_NOT_AVAILABLE_CODE (9)
#define INVALID_REQUEST_CODE (42)
#define KAFKA_STORAGE_ERROR_CODE (56)

/* Returns the index of the resource `(type, name)` in a describeConfigs result,
 * or -1. */
static int32_t find_describe_configs_key(const kafka_admin_DescribeConfigsResult_t *result,
                                         int32_t type_code, const char *name) {
    int32_t n = kafka_admin_DescribeConfigsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *k = kafka_admin_DescribeConfigsResult_get_key_name(result, i);
        if (k != NULL && strcmp(k, name) == 0 &&
            kafka_admin_DescribeConfigsResult_get_key_type(result, i) == type_code) {
            return i;
        }
    }
    return -1;
}

/* Returns the index of the resource `(type, name)` in an alterConfigs result,
 * or -1. */
static int32_t find_alter_configs_key(const kafka_admin_AlterConfigsResult_t *result,
                                      int32_t type_code, const char *name) {
    int32_t n = kafka_admin_AlterConfigsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *k = kafka_admin_AlterConfigsResult_get_key_name(result, i);
        if (k != NULL && strcmp(k, name) == 0 &&
            kafka_admin_AlterConfigsResult_get_key_type(result, i) == type_code) {
            return i;
        }
    }
    return -1;
}

/* Returns the index of the replica in an alterReplicaLogDirs result, or -1. */
static int32_t find_alter_replica_key(const kafka_admin_AlterReplicaLogDirsResult_t *result,
                                      const char *topic, int32_t partition, int32_t broker) {
    int32_t n = kafka_admin_AlterReplicaLogDirsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *t = kafka_admin_AlterReplicaLogDirsResult_get_topic(result, i);
        if (t != NULL && strcmp(t, topic) == 0 &&
            kafka_admin_AlterReplicaLogDirsResult_get_partition(result, i) == partition &&
            kafka_admin_AlterReplicaLogDirsResult_get_broker_id(result, i) == broker) {
            return i;
        }
    }
    return -1;
}

/* Applies one SET/DELETE config op and asserts the resource succeeded. */
static void alter_one_config(kafka_admin_AdminClient_t *admin, int32_t type_code,
                             const char *resource, const char *key, const char *value,
                             int32_t op_type) {
    const int32_t types[1] = {type_code};
    const char *resources[1] = {resource};
    const char *keys[1] = {key};
    const char *values[1] = {value};
    const int32_t ops[1] = {op_type};

    kafka_admin_AlterConfigsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_incremental_alter_configs(
        admin, types, resources, keys, values, ops, 1, -1, false, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    int32_t i = find_alter_configs_key(result, type_code, resource);
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_NULL(kafka_admin_AlterConfigsResult_get_error(result, i));
    kafka_admin_AlterConfigsResult_destroy(result);
}

// ---- describeCluster -------------------------------------------------------

static void test_mock_admin_describe_cluster_sync(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);

    kafka_admin_DescribeClusterResult_t *result = NULL;
    /* `include_authorized_operations` is passed as false on purpose: Java's
     * MockAdminClient.describeCluster ignores its options entirely
     * (MockAdminClient.java:340-360) and always completes the operations future
     * with an *empty* set, never null. */
    kafka_common_Error_t *err =
        kafka_admin_AdminClient_describe_cluster(admin, -1, false, false, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);

    TEST_ASSERT_EQUAL_STRING("4A5xz_QZTB2CtL4wc0X0Jw",
                             kafka_admin_DescribeClusterResult_cluster_id(result));
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_DescribeClusterResult_node_count(result));

    const kafka_common_Node_t *node = kafka_admin_DescribeClusterResult_get_node(result, 0);
    TEST_ASSERT_NOT_NULL(node);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Node_id(node));
    int32_t host_len = 0;
    const char *host = kafka_common_Node_host(node, &host_len);
    TEST_ASSERT_EQUAL_INT32(9, host_len);
    TEST_ASSERT_EQUAL_INT(0, strncmp(host, "localhost", 9));
    TEST_ASSERT_EQUAL_INT32(1000, kafka_common_Node_port(node));
    TEST_ASSERT_NULL(kafka_admin_DescribeClusterResult_get_node(result, 3));
    TEST_ASSERT_NULL(kafka_admin_DescribeClusterResult_get_node(result, -1));

    const kafka_common_Node_t *controller = kafka_admin_DescribeClusterResult_controller(result);
    TEST_ASSERT_NOT_NULL(controller);
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_Node_id(controller));

    /* Empty set, not absent. This accessor used to answer -1 for "absent"; it
     * now matches its TopicDescription / group siblings -- a non-negative count
     * plus a presence bit -- so no count can be fed to malloc as a negative. */
    TEST_ASSERT_EQUAL_INT32(
        0, kafka_admin_DescribeClusterResult_authorized_operation_count(result));
    TEST_ASSERT_TRUE(kafka_admin_DescribeClusterResult_has_authorized_operations(result));
    TEST_ASSERT_EQUAL_INT32(-1,
                            kafka_admin_DescribeClusterResult_authorized_operation(result, 0));

    kafka_admin_DescribeClusterResult_destroy(result);
    kafka_admin_DescribeClusterResult_destroy(NULL);
    kafka_admin_AdminClient_destroy(admin);
}

/* All four attribute futures fail together, so the call fails. The next call
 * succeeds: timeoutNextRequest only affects the requests it counted. */
static void test_mock_admin_describe_cluster_call_error(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_timeout_next_request(admin, 1));

    kafka_admin_DescribeClusterResult_t *result = NULL;
    kafka_common_Error_t *err =
        kafka_admin_AdminClient_describe_cluster(admin, -1, false, false, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_Error_destroy(err);

    err = kafka_admin_AdminClient_describe_cluster(admin, -1, false, false, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    kafka_admin_DescribeClusterResult_destroy(result);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t node_count;
    int32_t controller_id;
} describe_cluster_async_result_t;

static void on_describe_cluster(kafka_admin_DescribeClusterResult_t *result,
                                kafka_common_Error_t *error, void *user_data) {
    describe_cluster_async_result_t *r = (describe_cluster_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->node_count = kafka_admin_DescribeClusterResult_node_count(result);
        const kafka_common_Node_t *c = kafka_admin_DescribeClusterResult_controller(result);
        r->controller_id = c ? kafka_common_Node_id(c) : -1;
        kafka_admin_DescribeClusterResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_cluster_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(2);
    describe_cluster_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_cluster_async(admin, -1, true, false,
                                                   on_describe_cluster, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(2, r.node_count);
    TEST_ASSERT_EQUAL_INT32(0, r.controller_id);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_describe_cluster_async_null_handle(void) {
    describe_cluster_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_cluster_async(NULL, -1, false, false,
                                                   on_describe_cluster, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- describeConfigs -------------------------------------------------------

static void test_mock_admin_describe_configs_partial_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "cfg-topic", 1, 1);
    alter_one_config(admin, RESOURCE_TYPE_TOPIC, "cfg-topic", "retention.ms", "60000",
                     OP_TYPE_SET);

    const int32_t types[4] = {RESOURCE_TYPE_TOPIC, RESOURCE_TYPE_TOPIC, RESOURCE_TYPE_BROKER,
                              RESOURCE_TYPE_BROKER_LOGGER};
    const char *names[4] = {"cfg-topic", "missing-cfg-topic", "0", "0"};

    kafka_admin_DescribeConfigsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_describe_configs(
        admin, types, names, 4, -1, true, true, &result);
    TEST_ASSERT_NULL(err); /* per-resource failures are not call failures */
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(4, kafka_admin_DescribeConfigsResult_count(result));

    /* Entries are sorted by (type id, name): TOPIC(2) < BROKER(4) < BROKER_LOGGER(8). */
    TEST_ASSERT_EQUAL_INT32(RESOURCE_TYPE_TOPIC,
                            kafka_admin_DescribeConfigsResult_get_key_type(result, 0));
    TEST_ASSERT_EQUAL_STRING("cfg-topic",
                             kafka_admin_DescribeConfigsResult_get_key_name(result, 0));
    TEST_ASSERT_EQUAL_INT32(RESOURCE_TYPE_BROKER_LOGGER,
                            kafka_admin_DescribeConfigsResult_get_key_type(result, 3));

    /* 1. the topic we altered: value present, no error. */
    int32_t i = find_describe_configs_key(result, RESOURCE_TYPE_TOPIC, "cfg-topic");
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_NULL(kafka_admin_DescribeConfigsResult_get_error(result, i));
    const kafka_admin_Config_t *config = kafka_admin_DescribeConfigsResult_get_value(result, i);
    TEST_ASSERT_NOT_NULL(config);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_Config_entry_count(config));
    const kafka_admin_ConfigEntry_t *entry = kafka_admin_Config_get_entry(config, 0);
    TEST_ASSERT_NOT_NULL(entry);
    TEST_ASSERT_EQUAL_STRING("retention.ms", kafka_admin_ConfigEntry_name(entry));
    TEST_ASSERT_EQUAL_STRING("60000", kafka_admin_ConfigEntry_value(entry));
    /* The mock builds entries with `new ConfigEntry(name, value)`
     * (MockAdminClient.java `toConfigObject`), which leaves source UNKNOWN,
     * type UNKNOWN, no documentation and no synonyms — even though this request
     * asked for synonyms and documentation. */
    TEST_ASSERT_EQUAL_STRING("UNKNOWN", kafka_admin_ConfigEntry_source(entry));
    TEST_ASSERT_EQUAL_STRING("UNKNOWN", kafka_admin_ConfigEntry_type(entry));
    TEST_ASSERT_FALSE(kafka_admin_ConfigEntry_is_default(entry));
    TEST_ASSERT_FALSE(kafka_admin_ConfigEntry_is_sensitive(entry));
    TEST_ASSERT_FALSE(kafka_admin_ConfigEntry_is_read_only(entry));
    TEST_ASSERT_NULL(kafka_admin_ConfigEntry_documentation(entry));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ConfigEntry_synonym_count(entry));
    TEST_ASSERT_NULL(kafka_admin_ConfigEntry_synonym_name(entry, 0));
    TEST_ASSERT_NULL(kafka_admin_ConfigEntry_synonym_value(entry, 0));
    TEST_ASSERT_NULL(kafka_admin_ConfigEntry_synonym_source(entry, 0));
    /* Java's Config.get(name). */
    TEST_ASSERT_NOT_NULL(kafka_admin_Config_find_entry(config, "retention.ms"));
    TEST_ASSERT_NULL(kafka_admin_Config_find_entry(config, "no.such.config"));
    TEST_ASSERT_NULL(kafka_admin_Config_find_entry(config, NULL));
    TEST_ASSERT_NULL(kafka_admin_Config_get_entry(config, 1));
    TEST_ASSERT_NULL(kafka_admin_Config_get_entry(config, -1));

    /* 2. unknown topic -> UNKNOWN_TOPIC_OR_PARTITION, and no value. */
    i = find_describe_configs_key(result, RESOURCE_TYPE_TOPIC, "missing-cfg-topic");
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_NULL(kafka_admin_DescribeConfigsResult_get_value(result, i));
    const kafka_common_Error_t *e = kafka_admin_DescribeConfigsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, kafka_common_Error_code(e));

    /* 3. broker 0 carries the seeded default.replication.factor. */
    i = find_describe_configs_key(result, RESOURCE_TYPE_BROKER, "0");
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_NULL(kafka_admin_DescribeConfigsResult_get_error(result, i));
    config = kafka_admin_DescribeConfigsResult_get_value(result, i);
    TEST_ASSERT_NOT_NULL(config);
    entry = kafka_admin_Config_find_entry(config, "default.replication.factor");
    TEST_ASSERT_NOT_NULL(entry);
    TEST_ASSERT_EQUAL_STRING("1", kafka_admin_ConfigEntry_value(entry));

    /* 4. BROKER_LOGGER hits `getResourceDescription`'s default branch, which
     * throws UnsupportedOperationException("Not implemented yet")
     * (MockAdminClient.java:885 at kafka a18251bae0b8). */
    i = find_describe_configs_key(result, RESOURCE_TYPE_BROKER_LOGGER, "0");
    TEST_ASSERT_TRUE(i >= 0);
    e = kafka_admin_DescribeConfigsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_Error_code(e));

    /* Out-of-range indices are null / -1, never a crash. */
    TEST_ASSERT_NULL(kafka_admin_DescribeConfigsResult_get_key_name(result, 4));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_DescribeConfigsResult_get_key_type(result, 4));
    TEST_ASSERT_NULL(kafka_admin_DescribeConfigsResult_get_value(result, -1));
    TEST_ASSERT_NULL(kafka_admin_DescribeConfigsResult_get_error(result, 4));

    kafka_admin_DescribeConfigsResult_destroy(result);
    kafka_admin_DescribeConfigsResult_destroy(NULL);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL resource name skips that row rather than drifting the two arrays. */
static void test_mock_admin_describe_configs_null_row_skipped(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t types[2] = {RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER};
    const char *names[2] = {NULL, "0"};

    kafka_admin_DescribeConfigsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_configs(admin, types, names, 2, -1,
                                                              false, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeConfigsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("0", kafka_admin_DescribeConfigsResult_get_key_name(result, 0));
    kafka_admin_DescribeConfigsResult_destroy(result);

    /* An empty batch is legal and yields an empty result. */
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_configs(admin, NULL, NULL, 0, -1,
                                                              false, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeConfigsResult_count(result));
    kafka_admin_DescribeConfigsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t error_code_for_missing;
    int has_value_for_broker;
} describe_configs_async_result_t;

static void on_describe_configs(kafka_admin_DescribeConfigsResult_t *result,
                                kafka_common_Error_t *error, void *user_data) {
    describe_configs_async_result_t *r = (describe_configs_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_DescribeConfigsResult_count(result);
        int32_t i = find_describe_configs_key(result, RESOURCE_TYPE_TOPIC, "async-missing");
        const kafka_common_Error_t *e =
            i >= 0 ? kafka_admin_DescribeConfigsResult_get_error(result, i) : NULL;
        r->error_code_for_missing = e ? kafka_common_Error_code(e) : 0;
        i = find_describe_configs_key(result, RESOURCE_TYPE_BROKER, "0");
        r->has_value_for_broker =
            i >= 0 && kafka_admin_DescribeConfigsResult_get_value(result, i) != NULL;
        kafka_admin_DescribeConfigsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_configs_async_partial_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t types[2] = {RESOURCE_TYPE_TOPIC, RESOURCE_TYPE_BROKER};
    const char *names[2] = {"async-missing", "0"};

    describe_configs_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_configs_async(admin, types, names, 2, -1, false, false,
                                                   on_describe_configs, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(2, r.count);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, r.error_code_for_missing);
    TEST_ASSERT_TRUE(r.has_value_for_broker);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_describe_configs_async_null_handle(void) {
    describe_configs_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_configs_async(NULL, NULL, NULL, 0, -1, false, false,
                                                   on_describe_configs, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- incrementalAlterConfigs ----------------------------------------------

/* Two ops on one resource in one batch, then a second batch that deletes. */
static void test_mock_admin_incremental_alter_configs_set_then_delete(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "alter-topic", 1, 1);

    const int32_t types[2] = {RESOURCE_TYPE_TOPIC, RESOURCE_TYPE_TOPIC};
    const char *resources[2] = {"alter-topic", "alter-topic"};
    const char *keys[2] = {"retention.ms", "segment.ms"};
    const char *values[2] = {"1000", "2000"};
    const int32_t ops[2] = {OP_TYPE_SET, OP_TYPE_SET};

    kafka_admin_AlterConfigsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_incremental_alter_configs(
        admin, types, resources, keys, values, ops, 2, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    /* Both rows name the same resource, so the result has one key. */
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_AlterConfigsResult_count(result));
    TEST_ASSERT_EQUAL_INT32(RESOURCE_TYPE_TOPIC,
                            kafka_admin_AlterConfigsResult_get_key_type(result, 0));
    TEST_ASSERT_EQUAL_STRING("alter-topic",
                             kafka_admin_AlterConfigsResult_get_key_name(result, 0));
    TEST_ASSERT_NULL(kafka_admin_AlterConfigsResult_get_error(result, 0));
    kafka_admin_AlterConfigsResult_destroy(result);

    /* Both keys landed. */
    const int32_t d_types[1] = {RESOURCE_TYPE_TOPIC};
    const char *d_names[1] = {"alter-topic"};
    kafka_admin_DescribeConfigsResult_t *described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_configs(admin, d_types, d_names, 1, -1,
                                                              false, false, &described));
    const kafka_admin_Config_t *config =
        kafka_admin_DescribeConfigsResult_get_value(described, 0);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_Config_entry_count(config));
    TEST_ASSERT_EQUAL_STRING(
        "1000", kafka_admin_ConfigEntry_value(kafka_admin_Config_find_entry(config, "retention.ms")));
    kafka_admin_DescribeConfigsResult_destroy(described);

    /* DELETE takes a NULL value, which is what Java sends for a removal. */
    alter_one_config(admin, RESOURCE_TYPE_TOPIC, "alter-topic", "retention.ms", NULL,
                     OP_TYPE_DELETE);
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_configs(admin, d_types, d_names, 1, -1,
                                                              false, false, &described));
    config = kafka_admin_DescribeConfigsResult_get_value(described, 0);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_Config_entry_count(config));
    TEST_ASSERT_NULL(kafka_admin_Config_find_entry(config, "retention.ms"));
    kafka_admin_DescribeConfigsResult_destroy(described);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_incremental_alter_configs_partial_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "alter-ok", 1, 1);

    /* Row 3 uses APPEND, which the mock rejects as an unsupported op type
     * (MockAdminClient.handleIncrementalResourceAlteration's default branch
     * throws InvalidRequestException). */
    const int32_t types[3] = {RESOURCE_TYPE_TOPIC, RESOURCE_TYPE_TOPIC, RESOURCE_TYPE_TOPIC};
    const char *resources[3] = {"alter-ok", "alter-missing", "alter-ok"};
    const char *keys[3] = {"retention.ms", "retention.ms", "cleanup.policy"};
    const char *values[3] = {"1000", "1000", "compact"};
    const int32_t ops[3] = {OP_TYPE_SET, OP_TYPE_SET, OP_TYPE_APPEND};

    kafka_admin_AlterConfigsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_incremental_alter_configs(
        admin, types, resources, keys, values, ops, 3, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_AlterConfigsResult_count(result));

    /* "alter-ok" carries both its SET and its APPEND, and the APPEND fails the
     * whole resource. */
    int32_t i = find_alter_configs_key(result, RESOURCE_TYPE_TOPIC, "alter-ok");
    TEST_ASSERT_TRUE(i >= 0);
    const kafka_common_Error_t *e = kafka_admin_AlterConfigsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(INVALID_REQUEST_CODE, kafka_common_Error_code(e));

    i = find_alter_configs_key(result, RESOURCE_TYPE_TOPIC, "alter-missing");
    TEST_ASSERT_TRUE(i >= 0);
    e = kafka_admin_AlterConfigsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, kafka_common_Error_code(e));

    TEST_ASSERT_NULL(kafka_admin_AlterConfigsResult_get_key_name(result, 2));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_AlterConfigsResult_get_key_type(result, -1));
    kafka_admin_AlterConfigsResult_destroy(result);
    kafka_admin_AlterConfigsResult_destroy(NULL);
    kafka_admin_AdminClient_destroy(admin);
}

/* An unknown op-type code is a marshaling failure: the whole call fails and no
 * result handle is written. */
static void test_mock_admin_incremental_alter_configs_bad_op_type(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t types[1] = {RESOURCE_TYPE_TOPIC};
    const char *resources[1] = {"whatever"};
    const char *keys[1] = {"retention.ms"};
    const char *values[1] = {"1"};
    const int32_t ops[1] = {99};

    kafka_admin_AlterConfigsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_incremental_alter_configs(
        admin, types, resources, keys, values, ops, 1, -1, false, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_Error_destroy(err);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t error_code_for_first;
} alter_configs_async_result_t;

static void on_alter_configs(kafka_admin_AlterConfigsResult_t *result,
                             kafka_common_Error_t *error, void *user_data) {
    alter_configs_async_result_t *r = (alter_configs_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_AlterConfigsResult_count(result);
        const kafka_common_Error_t *e = kafka_admin_AlterConfigsResult_get_error(result, 0);
        r->error_code_for_first = e ? kafka_common_Error_code(e) : 0;
        kafka_admin_AlterConfigsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_incremental_alter_configs_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t types[1] = {RESOURCE_TYPE_TOPIC};
    const char *resources[1] = {"async-missing-topic"};
    const char *keys[1] = {"retention.ms"};
    const char *values[1] = {"1000"};
    const int32_t ops[1] = {OP_TYPE_SET};

    alter_configs_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_incremental_alter_configs_async(
        admin, types, resources, keys, values, ops, 1, -1, false, on_alter_configs, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, r.error_code_for_first);
    kafka_admin_AdminClient_destroy(admin);
}

/* Marshaling failures fire the callback inline, on the calling thread, before
 * the entry point returns — and never reach the mock. */
static void test_mock_admin_incremental_alter_configs_async_bad_op_type(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t types[1] = {RESOURCE_TYPE_TOPIC};
    const char *resources[1] = {"whatever"};
    const char *keys[1] = {"retention.ms"};
    const char *values[1] = {"1"};
    const int32_t ops[1] = {-7};

    alter_configs_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_incremental_alter_configs_async(
        admin, types, resources, keys, values, ops, 1, -1, false, on_alter_configs, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired)); /* already fired, inline */
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_incremental_alter_configs_async_null_handle(void) {
    alter_configs_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_incremental_alter_configs_async(NULL, NULL, NULL, NULL, NULL, NULL,
                                                            0, -1, false, on_alter_configs, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- listConfigResources ---------------------------------------------------

/* Returns the number of entries of type `type_code` in a list result. */
static int32_t count_config_resources_of_type(
    const kafka_admin_ListConfigResourcesResult_t *result, int32_t type_code) {
    int32_t n = kafka_admin_ListConfigResourcesResult_count(result);
    int32_t found = 0;
    for (int32_t i = 0; i < n; i++) {
        if (kafka_admin_ListConfigResourcesResult_get_type(result, i) == type_code) {
            found++;
        }
    }
    return found;
}

static void test_mock_admin_list_config_resources(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(2);
    create_one(admin, "lcr-b", 1, 1);
    create_one(admin, "lcr-a", 1, 1);

    /* Filtered to TOPIC. */
    const int32_t topic_only[1] = {RESOURCE_TYPE_TOPIC};
    kafka_admin_ListConfigResourcesResult_t *result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_config_resources(admin, topic_only, 1, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_ListConfigResourcesResult_count(result));
    /* Sorted by (type id, name). */
    TEST_ASSERT_EQUAL_STRING("lcr-a", kafka_admin_ListConfigResourcesResult_get_name(result, 0));
    TEST_ASSERT_EQUAL_STRING("lcr-b", kafka_admin_ListConfigResourcesResult_get_name(result, 1));
    TEST_ASSERT_EQUAL_INT32(RESOURCE_TYPE_TOPIC,
                            kafka_admin_ListConfigResourcesResult_get_type(result, 0));
    TEST_ASSERT_NULL(kafka_admin_ListConfigResourcesResult_get_name(result, 2));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_ListConfigResourcesResult_get_type(result, -1));
    kafka_admin_ListConfigResourcesResult_destroy(result);

    /* An empty type set means every supported type: the 2 topics plus one
     * BROKER and one BROKER_LOGGER per broker. */
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_config_resources(admin, NULL, 0, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, count_config_resources_of_type(result, RESOURCE_TYPE_TOPIC));
    TEST_ASSERT_EQUAL_INT32(2, count_config_resources_of_type(result, RESOURCE_TYPE_BROKER));
    TEST_ASSERT_EQUAL_INT32(2,
                            count_config_resources_of_type(result, RESOURCE_TYPE_BROKER_LOGGER));
    TEST_ASSERT_EQUAL_INT32(0,
                            count_config_resources_of_type(result, RESOURCE_TYPE_CLIENT_METRICS));
    kafka_admin_ListConfigResourcesResult_destroy(result);
    kafka_admin_ListConfigResourcesResult_destroy(NULL);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} list_config_resources_async_result_t;

static void on_list_config_resources(kafka_admin_ListConfigResourcesResult_t *result,
                                     kafka_common_Error_t *error, void *user_data) {
    list_config_resources_async_result_t *r = (list_config_resources_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListConfigResourcesResult_count(result);
        kafka_admin_ListConfigResourcesResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_config_resources_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t group_only[1] = {RESOURCE_TYPE_GROUP};
    list_config_resources_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_config_resources_async(admin, group_only, 1, -1,
                                                        on_list_config_resources, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(0, r.count); /* no group configs were seeded */
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_list_config_resources_async_null_handle(void) {
    list_config_resources_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_config_resources_async(NULL, NULL, 0, -1,
                                                        on_list_config_resources, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- listClientMetricsResources -------------------------------------------

static void test_mock_admin_list_client_metrics_resources(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    kafka_admin_ListClientMetricsResourcesResult_t *result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_client_metrics_resources(admin, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListClientMetricsResourcesResult_count(result));
    kafka_admin_ListClientMetricsResourcesResult_destroy(result);

    /* Altering a CLIENT_METRICS resource creates it, which is how Java's mock
     * seeds `clientMetricsConfigs` (MockAdminClient
     * handleIncrementalResourceAlteration, CLIENT_METRICS branch). */
    alter_one_config(admin, RESOURCE_TYPE_CLIENT_METRICS, "cm-b", "interval.ms", "1000",
                     OP_TYPE_SET);
    alter_one_config(admin, RESOURCE_TYPE_CLIENT_METRICS, "cm-a", "interval.ms", "2000",
                     OP_TYPE_SET);

    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_client_metrics_resources(admin, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_ListClientMetricsResourcesResult_count(result));
    /* Sorted by name. */
    TEST_ASSERT_EQUAL_STRING("cm-a",
                             kafka_admin_ListClientMetricsResourcesResult_get_name(result, 0));
    TEST_ASSERT_EQUAL_STRING("cm-b",
                             kafka_admin_ListClientMetricsResourcesResult_get_name(result, 1));
    TEST_ASSERT_NULL(kafka_admin_ListClientMetricsResourcesResult_get_name(result, 2));
    TEST_ASSERT_NULL(kafka_admin_ListClientMetricsResourcesResult_get_name(result, -1));
    kafka_admin_ListClientMetricsResourcesResult_destroy(result);
    kafka_admin_ListClientMetricsResourcesResult_destroy(NULL);

    /* The same resources show up through listConfigResources, the API that
     * supersedes this deprecated one. */
    const int32_t cm_only[1] = {RESOURCE_TYPE_CLIENT_METRICS};
    kafka_admin_ListConfigResourcesResult_t *listed = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_config_resources(admin, cm_only, 1, -1, &listed));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_ListConfigResourcesResult_count(listed));
    kafka_admin_ListConfigResourcesResult_destroy(listed);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} list_client_metrics_async_result_t;

static void on_list_client_metrics(kafka_admin_ListClientMetricsResourcesResult_t *result,
                                   kafka_common_Error_t *error, void *user_data) {
    list_client_metrics_async_result_t *r = (list_client_metrics_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListClientMetricsResourcesResult_count(result);
        kafka_admin_ListClientMetricsResourcesResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_client_metrics_resources_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    alter_one_config(admin, RESOURCE_TYPE_CLIENT_METRICS, "cm-async", "interval.ms", "1000",
                     OP_TYPE_SET);

    list_client_metrics_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_client_metrics_resources_async(admin, -1,
                                                                on_list_client_metrics, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_list_client_metrics_resources_async_null_handle(void) {
    list_client_metrics_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_client_metrics_resources_async(NULL, -1,
                                                                on_list_client_metrics, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- describeLogDirs -------------------------------------------------------

static void test_mock_admin_describe_log_dirs(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "ld-topic", 2, 1);

    /* Broker 7 does not exist. Java's `describeLogDirs` still puts an entry in
     * the result for every requested broker (`unwrappedResults.putIfAbsent`),
     * so it comes back with an empty log-dir map rather than an error. */
    const int32_t brokers[2] = {0, 7};
    kafka_admin_DescribeLogDirsResult_t *result = NULL;
    kafka_common_Error_t *err =
        kafka_admin_AdminClient_describe_log_dirs(admin, brokers, 2, -1, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DescribeLogDirsResult_count(result));

    /* Entries are sorted by broker id. */
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeLogDirsResult_get_broker(result, 0));
    TEST_ASSERT_EQUAL_INT32(7, kafka_admin_DescribeLogDirsResult_get_broker(result, 1));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_DescribeLogDirsResult_get_broker(result, 2));
    TEST_ASSERT_NULL(kafka_admin_DescribeLogDirsResult_get_error(result, 0));
    TEST_ASSERT_NULL(kafka_admin_DescribeLogDirsResult_get_error(result, 1));

    const kafka_admin_LogDirDescriptionMap_t *map =
        kafka_admin_DescribeLogDirsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_LogDirDescriptionMap_count(map));
    TEST_ASSERT_EQUAL_STRING("/tmp/kafka-logs",
                             kafka_admin_LogDirDescriptionMap_get_key(map, 0));
    TEST_ASSERT_NULL(kafka_admin_LogDirDescriptionMap_get_key(map, 1));
    TEST_ASSERT_NULL(kafka_admin_LogDirDescriptionMap_get_value(map, -1));

    const kafka_admin_LogDirDescription_t *dir =
        kafka_admin_LogDirDescriptionMap_get_value(map, 0);
    TEST_ASSERT_NOT_NULL(dir);
    TEST_ASSERT_NULL(kafka_admin_LogDirDescription_error(dir));
    /* The mock reports no volume sizes, i.e. Java's empty OptionalLong. */
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_LogDirDescription_total_bytes(dir));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_LogDirDescription_usable_bytes(dir));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_LogDirDescription_replica_count(dir));
    /* Replicas are sorted by (topic, partition). */
    TEST_ASSERT_EQUAL_STRING("ld-topic", kafka_admin_LogDirDescription_replica_topic(dir, 0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_LogDirDescription_replica_partition(dir, 0));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_LogDirDescription_replica_partition(dir, 1));
    TEST_ASSERT_EQUAL_INT64(0, kafka_admin_LogDirDescription_replica_size(dir, 0));
    TEST_ASSERT_EQUAL_INT64(0, kafka_admin_LogDirDescription_replica_offset_lag(dir, 0));
    TEST_ASSERT_FALSE(kafka_admin_LogDirDescription_replica_is_future(dir, 0));
    TEST_ASSERT_NULL(kafka_admin_LogDirDescription_replica_topic(dir, 2));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_LogDirDescription_replica_partition(dir, -1));
    TEST_ASSERT_FALSE(kafka_admin_LogDirDescription_replica_is_future(dir, 9));

    /* The unknown broker has an entry, but no log dirs. */
    const kafka_admin_LogDirDescriptionMap_t *empty =
        kafka_admin_DescribeLogDirsResult_get_value(result, 1);
    TEST_ASSERT_NOT_NULL(empty);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_LogDirDescriptionMap_count(empty));

    kafka_admin_DescribeLogDirsResult_destroy(result);
    kafka_admin_DescribeLogDirsResult_destroy(NULL);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t log_dir_count;
} describe_log_dirs_async_result_t;

static void on_describe_log_dirs(kafka_admin_DescribeLogDirsResult_t *result,
                                 kafka_common_Error_t *error, void *user_data) {
    describe_log_dirs_async_result_t *r = (describe_log_dirs_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_DescribeLogDirsResult_count(result);
        const kafka_admin_LogDirDescriptionMap_t *map =
            kafka_admin_DescribeLogDirsResult_get_value(result, 0);
        r->log_dir_count = map ? kafka_admin_LogDirDescriptionMap_count(map) : -1;
        kafka_admin_DescribeLogDirsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_log_dirs_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "ld-async", 1, 1);

    const int32_t brokers[1] = {0};
    describe_log_dirs_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_log_dirs_async(admin, brokers, 1, -1,
                                                    on_describe_log_dirs, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT32(1, r.log_dir_count);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_describe_log_dirs_async_null_handle(void) {
    describe_log_dirs_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_log_dirs_async(NULL, NULL, 0, -1, on_describe_log_dirs, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- alterReplicaLogDirs / describeReplicaLogDirs --------------------------

static void test_mock_admin_alter_replica_log_dirs_partial_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "mv-topic", 2, 1);

    /* Four replicas, three distinct failure modes:
     *  - (mv-topic, 0, 0) -> the broker's only log dir: accepted.
     *  - (mv-topic, 1, 0) -> a dir the broker does not have: KafkaStorageError.
     *  - (mv-missing, 0, 0) -> unknown topic: ReplicaNotAvailable.
     *  - (mv-topic, 0, 9) -> unknown broker: ReplicaNotAvailable.
     * (MockAdminClient.alterReplicaLogDirs, MockAdminClient.java:1032-1061.) */
    const char *topics[4] = {"mv-topic", "mv-topic", "mv-missing", "mv-topic"};
    const int32_t partitions[4] = {0, 1, 0, 0};
    const int32_t broker_ids[4] = {0, 0, 0, 9};
    const char *log_dirs[4] = {"/tmp/kafka-logs", "/data/other", "/tmp/kafka-logs",
                               "/tmp/kafka-logs"};

    kafka_admin_AlterReplicaLogDirsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_alter_replica_log_dirs(
        admin, topics, partitions, broker_ids, log_dirs, 4, -1, &result);
    TEST_ASSERT_NULL(err); /* per-replica failures are not call failures */
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(4, kafka_admin_AlterReplicaLogDirsResult_count(result));

    int32_t i = find_alter_replica_key(result, "mv-topic", 0, 0);
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_NULL(kafka_admin_AlterReplicaLogDirsResult_get_error(result, i));

    i = find_alter_replica_key(result, "mv-topic", 1, 0);
    TEST_ASSERT_TRUE(i >= 0);
    const kafka_common_Error_t *e =
        kafka_admin_AlterReplicaLogDirsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(KAFKA_STORAGE_ERROR_CODE, kafka_common_Error_code(e));

    i = find_alter_replica_key(result, "mv-missing", 0, 0);
    TEST_ASSERT_TRUE(i >= 0);
    e = kafka_admin_AlterReplicaLogDirsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(REPLICA_NOT_AVAILABLE_CODE, kafka_common_Error_code(e));

    i = find_alter_replica_key(result, "mv-topic", 0, 9);
    TEST_ASSERT_TRUE(i >= 0);
    e = kafka_admin_AlterReplicaLogDirsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(REPLICA_NOT_AVAILABLE_CODE, kafka_common_Error_code(e));

    /* Entries are sorted by (topic, partition, broker id). */
    TEST_ASSERT_EQUAL_STRING("mv-missing",
                             kafka_admin_AlterReplicaLogDirsResult_get_topic(result, 0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterReplicaLogDirsResult_get_partition(result, 1));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterReplicaLogDirsResult_get_broker_id(result, 1));
    TEST_ASSERT_EQUAL_INT32(9, kafka_admin_AlterReplicaLogDirsResult_get_broker_id(result, 2));
    TEST_ASSERT_NULL(kafka_admin_AlterReplicaLogDirsResult_get_topic(result, 4));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_AlterReplicaLogDirsResult_get_partition(result, -1));
    TEST_ASSERT_NULL(kafka_admin_AlterReplicaLogDirsResult_get_error(result, 4));
    kafka_admin_AlterReplicaLogDirsResult_destroy(result);
    kafka_admin_AlterReplicaLogDirsResult_destroy(NULL);

    /* The accepted move is now visible as a pending move on that replica. */
    const char *d_topics[1] = {"mv-topic"};
    const int32_t d_partitions[1] = {0};
    const int32_t d_brokers[1] = {0};
    kafka_admin_DescribeReplicaLogDirsResult_t *described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_replica_log_dirs(
        admin, d_topics, d_partitions, d_brokers, 1, -1, &described));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeReplicaLogDirsResult_count(described));
    const kafka_admin_ReplicaLogDirInfo_t *info =
        kafka_admin_DescribeReplicaLogDirsResult_get_value(described, 0);
    TEST_ASSERT_NOT_NULL(info);
    TEST_ASSERT_EQUAL_STRING("/tmp/kafka-logs",
                             kafka_admin_ReplicaLogDirInfo_future_replica_log_dir(info));
    kafka_admin_DescribeReplicaLogDirsResult_destroy(described);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_replica_log_dirs(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "drld-topic", 1, 1);

    /* The second replica names a topic the mock does not know.
     * `MockAdminClient.describeReplicaLogDirs` skips it entirely
     * (`if (topicMetadata != null)`, MockAdminClient.java:1112) rather than
     * reporting an error for it, so the result is *shorter* than the request.
     *
     * This is mock-only. `KafkaAdminClient` seeds one future per requested
     * replica (KafkaAdminClient.java:3066-3068) and completes all of them
     * (:3141-3145), so against a real broker an unknown topic comes back
     * *present*, with a null current replica log dir. */
    const char *topics[2] = {"drld-topic", "drld-missing"};
    const int32_t partitions[2] = {0, 0};
    const int32_t broker_ids[2] = {0, 0};

    kafka_admin_DescribeReplicaLogDirsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_describe_replica_log_dirs(
        admin, topics, partitions, broker_ids, 2, -1, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeReplicaLogDirsResult_count(result));

    TEST_ASSERT_EQUAL_STRING("drld-topic",
                             kafka_admin_DescribeReplicaLogDirsResult_get_topic(result, 0));
    TEST_ASSERT_EQUAL_INT32(0,
                            kafka_admin_DescribeReplicaLogDirsResult_get_partition(result, 0));
    TEST_ASSERT_EQUAL_INT32(0,
                            kafka_admin_DescribeReplicaLogDirsResult_get_broker_id(result, 0));
    TEST_ASSERT_NULL(kafka_admin_DescribeReplicaLogDirsResult_get_error(result, 0));

    const kafka_admin_ReplicaLogDirInfo_t *info =
        kafka_admin_DescribeReplicaLogDirsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(info);
    TEST_ASSERT_EQUAL_STRING("/tmp/kafka-logs",
                             kafka_admin_ReplicaLogDirInfo_current_replica_log_dir(info));
    TEST_ASSERT_EQUAL_INT64(0,
                            kafka_admin_ReplicaLogDirInfo_current_replica_offset_lag(info));
    /* No move is pending, so Java's future log dir is null. */
    TEST_ASSERT_NULL(kafka_admin_ReplicaLogDirInfo_future_replica_log_dir(info));
    TEST_ASSERT_EQUAL_INT64(0, kafka_admin_ReplicaLogDirInfo_future_replica_offset_lag(info));

    TEST_ASSERT_NULL(kafka_admin_DescribeReplicaLogDirsResult_get_topic(result, 1));
    TEST_ASSERT_EQUAL_INT32(-1,
                            kafka_admin_DescribeReplicaLogDirsResult_get_partition(result, 1));
    TEST_ASSERT_EQUAL_INT32(-1,
                            kafka_admin_DescribeReplicaLogDirsResult_get_broker_id(result, -1));
    TEST_ASSERT_NULL(kafka_admin_DescribeReplicaLogDirsResult_get_value(result, 1));
    TEST_ASSERT_NULL(kafka_admin_DescribeReplicaLogDirsResult_get_error(result, 1));

    kafka_admin_DescribeReplicaLogDirsResult_destroy(result);
    kafka_admin_DescribeReplicaLogDirsResult_destroy(NULL);
    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} alter_replica_async_result_t;

static void on_alter_replica_log_dirs(kafka_admin_AlterReplicaLogDirsResult_t *result,
                                      kafka_common_Error_t *error, void *user_data) {
    alter_replica_async_result_t *r = (alter_replica_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_AlterReplicaLogDirsResult_count(result);
        kafka_admin_AlterReplicaLogDirsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_alter_replica_log_dirs_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "mv-async", 1, 1);

    const char *topics[1] = {"mv-async"};
    const int32_t partitions[1] = {0};
    const int32_t broker_ids[1] = {0};
    const char *log_dirs[1] = {"/tmp/kafka-logs"};

    alter_replica_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_replica_log_dirs_async(
        admin, topics, partitions, broker_ids, log_dirs, 1, -1, on_alter_replica_log_dirs, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_alter_replica_log_dirs_async_null_handle(void) {
    alter_replica_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_replica_log_dirs_async(NULL, NULL, NULL, NULL, NULL, 0, -1,
                                                         on_alter_replica_log_dirs, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int has_current_dir;
} describe_replica_async_result_t;

static void on_describe_replica_log_dirs(kafka_admin_DescribeReplicaLogDirsResult_t *result,
                                         kafka_common_Error_t *error, void *user_data) {
    describe_replica_async_result_t *r = (describe_replica_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_DescribeReplicaLogDirsResult_count(result);
        const kafka_admin_ReplicaLogDirInfo_t *info =
            kafka_admin_DescribeReplicaLogDirsResult_get_value(result, 0);
        r->has_current_dir =
            info != NULL &&
            kafka_admin_ReplicaLogDirInfo_current_replica_log_dir(info) != NULL;
        kafka_admin_DescribeReplicaLogDirsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_replica_log_dirs_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "drld-async", 1, 1);

    const char *topics[1] = {"drld-async"};
    const int32_t partitions[1] = {0};
    const int32_t broker_ids[1] = {0};

    describe_replica_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_replica_log_dirs_async(
        admin, topics, partitions, broker_ids, 1, -1, on_describe_replica_log_dirs, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_TRUE(r.has_current_dir);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_describe_replica_log_dirs_async_null_handle(void) {
    describe_replica_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_replica_log_dirs_async(NULL, NULL, NULL, NULL, 0, -1,
                                                            on_describe_replica_log_dirs, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

/* A NULL out_result means the caller does not want the result, so the handle is
 * never built (see finish_sync). None of the B2 sync entry points may leak or
 * crash on that path. */
static void test_mock_admin_b2_null_out_result(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t types[1] = {RESOURCE_TYPE_BROKER};
    const char *names[1] = {"0"};
    const char *keys[1] = {"k"};
    const char *values[1] = {"v"};
    const int32_t ops[1] = {OP_TYPE_SET};
    const int32_t brokers[1] = {0};
    const char *topics[1] = {"none"};
    const int32_t partitions[1] = {0};
    const char *log_dirs[1] = {"/tmp/kafka-logs"};

    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_cluster(admin, -1, false, false, NULL));
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_configs(admin, types, names, 1, -1, false, false, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_incremental_alter_configs(
        admin, types, names, keys, values, ops, 1, -1, false, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_config_resources(admin, NULL, 0, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_client_metrics_resources(admin, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_log_dirs(admin, brokers, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_replica_log_dirs(
        admin, topics, partitions, brokers, log_dirs, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_replica_log_dirs(
        admin, topics, partitions, brokers, 1, -1, NULL));

    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// B3 — elections, reassignments, offsets
//
// Every RPC here is partition-keyed, so keys cross as parallel `topics[]` /
// `partitions[]` arrays and come back as `_get_topic(i)` / `_get_partition(i)`,
// as in deleteRecords.
// ---------------------------------------------------------------------------

/* `ElectionType.value` (src/common/election_type.rs, Java's ElectionType). */
#define ELECTION_TYPE_PREFERRED (0)
#define ELECTION_TYPE_UNCLEAN (1)

/* `IsolationLevel.id()` (Java's IsolationLevel). */
#define ISOLATION_READ_UNCOMMITTED (0)
#define ISOLATION_READ_COMMITTED (1)

/* `ListOffsetsRequest` timestamp sentinels, i.e. the values Java's
 * `KafkaAdminClient.getOffsetFromSpec` emits for the no-argument OffsetSpec
 * factories (KafkaAdminClient.java:5142-5156). */
#define OFFSET_SPEC_LATEST ((int64_t)-1)
#define OFFSET_SPEC_EARLIEST ((int64_t)-2)
#define OFFSET_SPEC_MAX_TIMESTAMP ((int64_t)-3)

/* Numeric `Errors` codes asserted below (src/common/protocol/errors.rs). */
#define INVALID_ARG_CODE (-1)

/* Returns the index of (topic, partition) in a listOffsets result, or -1. */
static int32_t find_list_offsets_key(const kafka_admin_ListOffsetsResult_t *result,
                                     const char *topic, int32_t partition) {
    int32_t n = kafka_admin_ListOffsetsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *t = kafka_admin_ListOffsetsResult_get_topic(result, i);
        if (t != NULL && strcmp(t, topic) == 0 &&
            kafka_admin_ListOffsetsResult_get_partition(result, i) == partition) {
            return i;
        }
    }
    return -1;
}

/* Reassigns `partition` of `topic` to {1, 2} and asserts it succeeded. */
static void reassign_one(kafka_admin_AdminClient_t *admin, const char *topic, int32_t partition) {
    const char *topics[1] = {topic};
    const int32_t partitions[1] = {partition};
    const bool cancel[1] = {false};
    const int32_t replicas[2] = {1, 2};
    const int32_t *replica_ptrs[1] = {replicas};
    const int32_t replica_counts[1] = {2};

    kafka_admin_AlterPartitionReassignmentsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, true, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_AlterPartitionReassignmentsResult_count(result));
    TEST_ASSERT_NULL(kafka_admin_AlterPartitionReassignmentsResult_get_error(result, 0));
    kafka_admin_AlterPartitionReassignmentsResult_destroy(result);
}

// ---- electLeaders ----------------------------------------------------------

/* `MockAdminClient.electLeaders` throws UnsupportedOperationException("Not
 * implemented yet") (MockAdminClient.java:792-798). Java exposes one future for
 * the whole election, so that failure is a *call* failure here, not a per-
 * partition error — the return value is non-null and no result is written. */
static void test_mock_admin_elect_leaders_reports_unsupported(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "el-topic", 2, 1);

    const char *topics[2] = {"el-topic", "el-topic"};
    const int32_t partitions[2] = {0, 1};

    kafka_admin_ElectLeadersResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_elect_leaders(
        admin, ELECTION_TYPE_PREFERRED, false, topics, partitions, 2, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_Error_code(err));
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* `all_partitions = true` is Java's null Set: the arrays are not read, so
     * passing NULL for them is fine and the mock still refuses. */
    result = NULL;
    err = kafka_admin_AdminClient_elect_leaders(admin, ELECTION_TYPE_UNCLEAN, true, NULL, NULL, 0, -1,
                                                &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* An election type outside Java's {0, 1} is rejected before the RPC is issued,
 * with `ElectionType.valueOf(byte)`'s own message. */
static void test_mock_admin_elect_leaders_rejects_bad_election_type(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    kafka_admin_ElectLeadersResult_t *result = NULL;
    kafka_common_Error_t *err =
        kafka_admin_AdminClient_elect_leaders(admin, 7, true, NULL, NULL, 0, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Value 7 must be one of [PREFERRED, UNCLEAN]",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t error_code;
} elect_leaders_async_result_t;

static void on_elect_leaders(kafka_admin_ElectLeadersResult_t *result,
                             kafka_common_Error_t *error, void *user_data) {
    elect_leaders_async_result_t *r = (elect_leaders_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        kafka_admin_ElectLeadersResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_Error_code(error);
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_elect_leaders_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "el-async", 1, 1);

    const char *topics[1] = {"el-async"};
    const int32_t partitions[1] = {0};

    elect_leaders_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_elect_leaders_async(admin, ELECTION_TYPE_PREFERRED, false, topics,
                                                partitions, 1, -1, on_elect_leaders, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    /* Same whole-call failure as the sync path. */
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, r.error_code);
    kafka_admin_AdminClient_destroy(admin);
}

/* A bad election type is a pre-submission marshaling failure, so the callback
 * fires inline on this thread, before the call returns — no wait_for needed. */
static void test_mock_admin_elect_leaders_async_bad_election_type(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    elect_leaders_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_elect_leaders_async(admin, -5, true, NULL, NULL, 0, -1, on_elect_leaders,
                                                &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_elect_leaders_async_null_handle(void) {
    elect_leaders_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_elect_leaders_async(NULL, ELECTION_TYPE_PREFERRED, true, NULL, NULL, 0,
                                                -1, on_elect_leaders, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- alterPartitionReassignments / listPartitionReassignments --------------

/* The mock implements both RPCs against its in-memory `reassignments` map
 * (MockAdminClient.java:1141-1180). A partition it does not know fails with
 * UNKNOWN_TOPIC_OR_PARTITION *per partition*, while the accepted one succeeds —
 * the partial-batch shape. */
static void test_mock_admin_alter_partition_reassignments_partial_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "ra-topic", 1, 3);

    const char *topics[2] = {"ra-topic", "ra-missing"};
    const int32_t partitions[2] = {0, 0};
    const bool cancel[2] = {false, false};
    const int32_t replicas[2] = {1, 2};
    const int32_t *replica_ptrs[2] = {replicas, replicas};
    const int32_t replica_counts[2] = {2, 2};

    kafka_admin_AlterPartitionReassignmentsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 2, -1, true, &result);
    /* A per-partition failure is not a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_AlterPartitionReassignmentsResult_count(result));

    /* Sorted by topic then partition, so "ra-missing" precedes "ra-topic". */
    TEST_ASSERT_EQUAL_STRING("ra-missing",
                             kafka_admin_AlterPartitionReassignmentsResult_get_topic(result, 0));
    const kafka_common_Error_t *e =
        kafka_admin_AlterPartitionReassignmentsResult_get_error(result, 0);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, kafka_common_Error_code(e));

    TEST_ASSERT_EQUAL_STRING("ra-topic",
                             kafka_admin_AlterPartitionReassignmentsResult_get_topic(result, 1));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterPartitionReassignmentsResult_get_partition(result, 1));
    TEST_ASSERT_NULL(kafka_admin_AlterPartitionReassignmentsResult_get_error(result, 1));

    /* Out-of-range indices are null / -1, never a crash. */
    TEST_ASSERT_NULL(kafka_admin_AlterPartitionReassignmentsResult_get_topic(result, 2));
    TEST_ASSERT_EQUAL_INT32(-1,
                            kafka_admin_AlterPartitionReassignmentsResult_get_partition(result, -1));
    TEST_ASSERT_NULL(kafka_admin_AlterPartitionReassignmentsResult_get_error(result, 2));
    kafka_admin_AlterPartitionReassignmentsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

/* An empty target-replica list is rejected before the RPC is issued, exactly as
 * Java's `NewPartitionReassignment(List<Integer>)` throws
 * IllegalArgumentException — it must NOT be silently read as a cancellation. */
static void test_mock_admin_alter_partition_reassignments_rejects_empty_replicas(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "ra-empty", 1, 3);

    const char *topics[1] = {"ra-empty"};
    const int32_t partitions[1] = {0};
    const bool cancel[1] = {false};
    const int32_t replicas[1] = {0};
    const int32_t *replica_ptrs[1] = {replicas};
    const int32_t replica_counts[1] = {0};

    kafka_admin_AlterPartitionReassignmentsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, true, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING(
        "reassignment for ra-empty-0 at index 0: Cannot create a new partition reassignment without "
        "any replicas",
        kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* Nothing was submitted, so nothing is listed. */
    kafka_admin_ListPartitionReassignmentsResult_t *listed = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_partition_reassignments(admin, true, NULL, NULL, 0, -1, &listed));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListPartitionReassignmentsResult_count(listed));
    kafka_admin_ListPartitionReassignmentsResult_destroy(listed);

    kafka_admin_AdminClient_destroy(admin);
}

/* The full round trip: reassign, list, cancel, list again. The mock seeds every
 * partition with all brokers as replicas (MockAdminClient.java:412-420), so
 * targeting {1, 2} on a 3-broker mock removes broker 0 and adds nothing. */
static void test_mock_admin_list_partition_reassignments_round_trip(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "lr-topic", 1, 3);
    reassign_one(admin, "lr-topic", 0);

    /* `all_partitions = true` is Java's Optional.empty(): list everything. */
    kafka_admin_ListPartitionReassignmentsResult_t *listed = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_partition_reassignments(admin, true, NULL, NULL, 0, -1, &listed));
    TEST_ASSERT_NOT_NULL(listed);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_ListPartitionReassignmentsResult_count(listed));
    TEST_ASSERT_EQUAL_STRING("lr-topic",
                             kafka_admin_ListPartitionReassignmentsResult_get_topic(listed, 0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListPartitionReassignmentsResult_get_partition(listed, 0));

    const kafka_admin_PartitionReassignment_t *pr =
        kafka_admin_ListPartitionReassignmentsResult_get_value(listed, 0);
    TEST_ASSERT_NOT_NULL(pr);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_PartitionReassignment_replica_count(pr));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_PartitionReassignment_replica(pr, 0));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_PartitionReassignment_replica(pr, 1));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_PartitionReassignment_replica(pr, 2));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_PartitionReassignment_adding_replica_count(pr));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_PartitionReassignment_removing_replica_count(pr));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_PartitionReassignment_removing_replica(pr, 0));
    /* Out-of-range broker indices are -1, never a crash. */
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_PartitionReassignment_replica(pr, 3));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_PartitionReassignment_adding_replica(pr, 0));
    TEST_ASSERT_NULL(kafka_admin_ListPartitionReassignmentsResult_get_value(listed, 1));
    kafka_admin_ListPartitionReassignmentsResult_destroy(listed);

    /* Restricting to a partition without a reassignment yields nothing: the
     * result is shorter than the request. */
    const char *other[1] = {"lr-other"};
    const int32_t other_partitions[1] = {0};
    listed = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_partition_reassignments(
        admin, false, other, other_partitions, 1, -1, &listed));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListPartitionReassignmentsResult_count(listed));
    kafka_admin_ListPartitionReassignmentsResult_destroy(listed);

    /* `cancel[i] = true` is Java's empty Optional, which reverts the
     * reassignment (Admin.java:1142-1143, MockAdminClient.java:1160-1162). The
     * replica list is deliberately non-empty here: the flag must win. */
    const char *topics[1] = {"lr-topic"};
    const int32_t partitions[1] = {0};
    const bool cancel[1] = {true};
    const int32_t replicas[2] = {1, 2};
    const int32_t *replica_ptrs[1] = {replicas};
    const int32_t replica_counts[1] = {2};
    kafka_admin_AlterPartitionReassignmentsResult_t *cancelled = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, true, &cancelled));
    TEST_ASSERT_NULL(kafka_admin_AlterPartitionReassignmentsResult_get_error(cancelled, 0));
    kafka_admin_AlterPartitionReassignmentsResult_destroy(cancelled);

    listed = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_partition_reassignments(admin, true, NULL, NULL, 0, -1, &listed));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListPartitionReassignmentsResult_count(listed));
    kafka_admin_ListPartitionReassignmentsResult_destroy(listed);

    kafka_admin_AdminClient_destroy(admin);
}

/* Regression at the FFI boundary for the core fix in this slice.
 *
 * `deleteTopics` drops the topic from the mock's `allTopics` without pruning
 * its `reassignments` map — exactly as Java's does (MockAdminClient.java:584
 * and :614 versus the only two writers, at :1158 and :1161) — so
 * create/alter/delete/list reaches `findPartitionReassignment`'s
 * "no TopicMetadata" branch, where Java throws a bare RuntimeException. Before
 * the fix the Rust mock panicked there, and that unwind would have aborted this
 * process at the `extern "C"` frame. The point of this test is that the suite
 * survives the sequence and observes an ordinary error return: were the
 * regression to come back, the test binary would die rather than fail. */
static void test_mock_admin_list_partition_reassignments_after_delete_returns_error(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "lr-gone", 1, 3);
    reassign_one(admin, "lr-gone", 0);

    const char *names[1] = {"lr-gone"};
    kafka_admin_DeleteTopicsResult_t *deleted = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_topics(admin, names, 1, -1, false, &deleted));
    TEST_ASSERT_NOT_NULL(deleted);
    TEST_ASSERT_NULL(kafka_admin_DeleteTopicsResult_get_error(deleted, 0));
    kafka_admin_DeleteTopicsResult_destroy(deleted);

    /* `listPartitionReassignments` holds a single future for the whole map
     * (ListPartitionReassignmentsResult.java:31), so the failure surfaces as
     * the call's error and no result handle is produced. */
    kafka_admin_ListPartitionReassignmentsResult_t *listed = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_list_partition_reassignments(
        admin, true, NULL, NULL, 0, -1, &listed);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(listed);
    TEST_ASSERT_EQUAL_STRING(
        "Internal MockAdminClient logic error: found reassignment for lr-gone-0, but no "
        "TopicMetadata",
        kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} alter_reassign_async_result_t;

static void on_alter_partition_reassignments(kafka_admin_AlterPartitionReassignmentsResult_t *result,
                                             kafka_common_Error_t *error, void *user_data) {
    alter_reassign_async_result_t *r = (alter_reassign_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_AlterPartitionReassignmentsResult_count(result);
        kafka_admin_AlterPartitionReassignmentsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_alter_partition_reassignments_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "ra-async", 1, 3);

    const char *topics[1] = {"ra-async"};
    const int32_t partitions[1] = {0};
    const bool cancel[1] = {false};
    const int32_t replicas[2] = {1, 2};
    const int32_t *replica_ptrs[1] = {replicas};
    const int32_t replica_counts[1] = {2};

    alter_reassign_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_partition_reassignments_async(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, false,
        on_alter_partition_reassignments, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    kafka_admin_AdminClient_destroy(admin);
}

/* An empty non-cancelled replica list is a pre-submission marshaling failure,
 * so the callback fires inline before the call returns. */
static void test_mock_admin_alter_partition_reassignments_async_empty_replicas(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "ra-async-bad", 1, 3);

    const char *topics[1] = {"ra-async-bad"};
    const int32_t partitions[1] = {0};
    const bool cancel[1] = {false};
    const int32_t replicas[1] = {0};
    const int32_t *replica_ptrs[1] = {replicas};
    const int32_t replica_counts[1] = {0};

    alter_reassign_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_partition_reassignments_async(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, true,
        on_alter_partition_reassignments, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_alter_partition_reassignments_async_null_handle(void) {
    alter_reassign_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_partition_reassignments_async(
        NULL, NULL, NULL, NULL, NULL, NULL, 0, -1, true, on_alter_partition_reassignments, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t removing_count;
} list_reassign_async_result_t;

static void on_list_partition_reassignments(kafka_admin_ListPartitionReassignmentsResult_t *result,
                                            kafka_common_Error_t *error, void *user_data) {
    list_reassign_async_result_t *r = (list_reassign_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListPartitionReassignmentsResult_count(result);
        if (r->count > 0) {
            const kafka_admin_PartitionReassignment_t *pr =
                kafka_admin_ListPartitionReassignmentsResult_get_value(result, 0);
            r->removing_count = kafka_admin_PartitionReassignment_removing_replica_count(pr);
        }
        kafka_admin_ListPartitionReassignmentsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_partition_reassignments_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "lr-async", 1, 3);
    reassign_one(admin, "lr-async", 0);

    list_reassign_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_partition_reassignments_async(admin, true, NULL, NULL, 0, -1,
                                                               on_list_partition_reassignments, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT32(1, r.removing_count);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_list_partition_reassignments_async_null_handle(void) {
    list_reassign_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_partition_reassignments_async(NULL, true, NULL, NULL, 0, -1,
                                                               on_list_partition_reassignments, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---- listOffsets -----------------------------------------------------------

/* The mock answers `earliest()` from `beginningOffsets` and everything else
 * from `endOffsets` (MockAdminClient.java:1220-1240), both seeded through the
 * mock drivers. An unseeded partition reports -1 rather than Java's NPE on
 * unboxing a null Long — a deliberate divergence documented in
 * src/admin/mock_admin_client.rs, since a panic must not cross into C. */
static void test_mock_admin_list_offsets_earliest_and_latest(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "lo-topic", 2, 1);

    const char *seed_topics[2] = {"lo-topic", "lo-topic"};
    const int32_t seed_partitions[2] = {0, 1};
    const int64_t begin[2] = {5, 7};
    const int64_t end[2] = {105, 107};
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_update_beginning_offsets(
        admin, seed_topics, seed_partitions, begin, 2));
    TEST_ASSERT_NULL(
        kafka_admin_MockAdminClient_update_end_offsets(admin, seed_topics, seed_partitions, end, 2));

    /* Partition 0 asks for the earliest offset, partition 1 for the latest, so
     * a transposed spec array would swap 5 and 107. */
    const char *topics[3] = {"lo-topic", "lo-topic", "lo-unseeded"};
    const int32_t partitions[3] = {0, 1, 0};
    const bool is_timestamp[3] = {false, false, false};
    const int64_t specs[3] = {OFFSET_SPEC_EARLIEST, OFFSET_SPEC_LATEST, OFFSET_SPEC_MAX_TIMESTAMP};

    kafka_admin_ListOffsetsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_list_offsets(
        admin, topics, partitions, is_timestamp, specs, 3, -1, ISOLATION_READ_UNCOMMITTED, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_ListOffsetsResult_count(result));

    int32_t i = find_list_offsets_key(result, "lo-topic", 0);
    TEST_ASSERT_TRUE(i >= 0);
    const kafka_admin_ListOffsetsResultInfo_t *info =
        kafka_admin_ListOffsetsResult_get_value(result, i);
    TEST_ASSERT_NOT_NULL(info);
    TEST_ASSERT_EQUAL_INT64(5, kafka_admin_ListOffsetsResultInfo_offset(info));
    /* The mock reports no timestamp and no leader epoch. */
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_ListOffsetsResultInfo_timestamp(info));
    int32_t epoch = -99;
    TEST_ASSERT_FALSE(kafka_admin_ListOffsetsResultInfo_leader_epoch(info, &epoch));
    TEST_ASSERT_EQUAL_INT32(-99, epoch);
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_get_error(result, i));

    i = find_list_offsets_key(result, "lo-topic", 1);
    TEST_ASSERT_TRUE(i >= 0);
    info = kafka_admin_ListOffsetsResult_get_value(result, i);
    TEST_ASSERT_EQUAL_INT64(107, kafka_admin_ListOffsetsResultInfo_offset(info));

    /* maxTimestamp() also reads endOffsets; the unseeded partition yields -1. */
    i = find_list_offsets_key(result, "lo-unseeded", 0);
    TEST_ASSERT_TRUE(i >= 0);
    info = kafka_admin_ListOffsetsResult_get_value(result, i);
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_ListOffsetsResultInfo_offset(info));

    /* Out-of-range indices are null / -1, never a crash. */
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_get_topic(result, 3));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_ListOffsetsResult_get_partition(result, -1));
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_get_value(result, 3));
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_get_error(result, 3));
    kafka_admin_ListOffsetsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

/* `is_timestamp` is what separates `OffsetSpec.forTimestamp(-2)` from
 * `OffsetSpec.earliest()`, which both project to -2 through Java's
 * `getOffsetFromSpec`. The mock proves the two are not interchangeable: a
 * TimestampSpec fails that partition with UnsupportedOperationException
 * ("Not implement yet", MockAdminClient.java:1230), while `earliest()` returns
 * the seeded beginning offset. Note this is a *per-partition* error here,
 * because Java's ListOffsetsResult holds one future per partition. */
static void test_mock_admin_list_offsets_timestamp_flag_is_load_bearing(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "lo-ts", 2, 1);

    const char *seed_topics[1] = {"lo-ts"};
    const int32_t seed_partitions[1] = {0};
    const int64_t begin[1] = {11};
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_update_beginning_offsets(
        admin, seed_topics, seed_partitions, begin, 1));

    const char *topics[2] = {"lo-ts", "lo-ts"};
    const int32_t partitions[2] = {0, 1};
    const bool is_timestamp[2] = {false, true};
    /* Identical values, opposite flags. */
    const int64_t specs[2] = {OFFSET_SPEC_EARLIEST, OFFSET_SPEC_EARLIEST};

    kafka_admin_ListOffsetsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_offsets(
        admin, topics, partitions, is_timestamp, specs, 2, -1, ISOLATION_READ_COMMITTED, &result));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_ListOffsetsResult_count(result));

    int32_t i = find_list_offsets_key(result, "lo-ts", 0);
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_get_error(result, i));
    TEST_ASSERT_EQUAL_INT64(
        11, kafka_admin_ListOffsetsResultInfo_offset(kafka_admin_ListOffsetsResult_get_value(result, i)));

    i = find_list_offsets_key(result, "lo-ts", 1);
    TEST_ASSERT_NULL(kafka_admin_ListOffsetsResult_get_value(result, i));
    const kafka_common_Error_t *e = kafka_admin_ListOffsetsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_Error_code(e));
    kafka_admin_ListOffsetsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

/* A value that is neither flagged as a timestamp nor a recognised sentinel, and
 * an isolation level outside {0, 1}, are both rejected before the RPC runs. */
static void test_mock_admin_list_offsets_rejects_bad_inputs(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "lo-bad", 1, 1);

    const char *topics[1] = {"lo-bad"};
    const int32_t partitions[1] = {0};
    const bool is_timestamp[1] = {false};
    const int64_t bad_spec[1] = {42};

    kafka_admin_ListOffsetsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_list_offsets(
        admin, topics, partitions, is_timestamp, bad_spec, 1, -1, ISOLATION_READ_UNCOMMITTED, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING(
        "offset spec for lo-bad-0 at index 0: 42 is not a ListOffsets timestamp sentinel; pass "
        "is_timestamp=true to request OffsetSpec.forTimestamp(42)",
        kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    const int64_t good_spec[1] = {OFFSET_SPEC_LATEST};
    result = NULL;
    err = kafka_admin_AdminClient_list_offsets(admin, topics, partitions, is_timestamp, good_spec, 1,
                                               -1, 9, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Unknown isolation level 9", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int64_t first_offset;
} list_offsets_async_result_t;

static void on_list_offsets(kafka_admin_ListOffsetsResult_t *result,
                            kafka_common_Error_t *error, void *user_data) {
    list_offsets_async_result_t *r = (list_offsets_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListOffsetsResult_count(result);
        const kafka_admin_ListOffsetsResultInfo_t *info =
            kafka_admin_ListOffsetsResult_get_value(result, 0);
        r->first_offset = info ? kafka_admin_ListOffsetsResultInfo_offset(info) : -99;
        kafka_admin_ListOffsetsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_offsets_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "lo-async", 1, 1);

    const char *topics[1] = {"lo-async"};
    const int32_t partitions[1] = {0};
    const int64_t end[1] = {77};
    TEST_ASSERT_NULL(
        kafka_admin_MockAdminClient_update_end_offsets(admin, topics, partitions, end, 1));

    const bool is_timestamp[1] = {false};
    const int64_t specs[1] = {OFFSET_SPEC_LATEST};

    list_offsets_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_offsets_async(admin, topics, partitions, is_timestamp, specs, 1, -1,
                                               ISOLATION_READ_UNCOMMITTED, on_list_offsets, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT64(77, r.first_offset);
    kafka_admin_AdminClient_destroy(admin);
}

/* An unknown isolation level is a pre-submission marshaling failure, so the
 * callback fires inline before the call returns. */
static void test_mock_admin_list_offsets_async_bad_isolation_level(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "lo-async-bad", 1, 1);

    const char *topics[1] = {"lo-async-bad"};
    const int32_t partitions[1] = {0};
    const bool is_timestamp[1] = {false};
    const int64_t specs[1] = {OFFSET_SPEC_LATEST};

    list_offsets_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_offsets_async(admin, topics, partitions, is_timestamp, specs, 1, -1,
                                               42, on_list_offsets, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_list_offsets_async_null_handle(void) {
    list_offsets_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_offsets_async(NULL, NULL, NULL, NULL, NULL, 0, -1,
                                               ISOLATION_READ_UNCOMMITTED, on_list_offsets, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

/* The mock drivers reject a production handle rather than panicking. */
static void test_mock_admin_offset_drivers_reject_non_mock(void) {
    kafka_admin_AdminClientProperties_t *props = kafka_admin_AdminClientProperties_new();
    kafka_admin_AdminClientProperties_put(props, "bootstrap.servers", "localhost:9092");
    kafka_common_Error_t *new_err = NULL;
    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, &new_err);
    kafka_admin_AdminClientProperties_destroy(props);
    TEST_ASSERT_NULL(new_err);
    TEST_ASSERT_NOT_NULL(admin);

    const char *topics[1] = {"t"};
    const int32_t partitions[1] = {0};
    const int64_t offsets[1] = {1};
    kafka_common_Error_t *err = kafka_admin_MockAdminClient_update_beginning_offsets(
        admin, topics, partitions, offsets, 1);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_destroy(err);
    err = kafka_admin_MockAdminClient_update_end_offsets(admin, topics, partitions, offsets, 1);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_close(admin, 1000);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL `out_result` must not build (and therefore not leak) a handle. */
static void test_mock_admin_b3_null_out_result(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);
    create_one(admin, "b3-null", 1, 3);

    const char *topics[1] = {"b3-null"};
    const int32_t partitions[1] = {0};
    const bool cancel[1] = {false};
    const int32_t replicas[2] = {1, 2};
    const int32_t *replica_ptrs[1] = {replicas};
    const int32_t replica_counts[1] = {2};
    const bool is_timestamp[1] = {false};
    const int64_t specs[1] = {OFFSET_SPEC_LATEST};

    /* electLeaders is unsupported by the mock, so it returns its error either
     * way; the other three succeed and must simply drop the result. */
    kafka_common_Error_t *err = kafka_admin_AdminClient_elect_leaders(
        admin, ELECTION_TYPE_PREFERRED, false, topics, partitions, 1, -1, NULL);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_Error_destroy(err);

    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, true, NULL));
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_partition_reassignments(admin, true, NULL, NULL, 0, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_offsets(admin, topics, partitions, is_timestamp,
                                                          specs, 1, -1,
                                                          ISOLATION_READ_UNCOMMITTED, NULL));

    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// B4 — groups and group offsets
//
// Java's own MockAdminClient implements only two of these nine RPCs
// (`listGroups` / `listConsumerGroups` from `groupConfigs`, and
// `listConsumerGroupOffsets` from `committedOffsets`); the other seven throw
// `UnsupportedOperationException("Not implemented yet")`, which the Rust mock
// surfaces as an exceptional future per `admin-client.md` §9. So the tests
// below split into round-trip tests for the first three and
// where-does-the-error-land tests for the rest — and the latter are not
// filler: they pin whether a failure arrives per key or as the call's error,
// which is exactly what each Java `*Result`'s future shape decides.
// ---------------------------------------------------------------------------

/* Seeds a group in the mock by writing a group config: `groupConfigs` is the
 * only map `MockAdminClient.listGroups` reads (MockAdminClient.java:728-732),
 * and `incrementalAlterConfigs` on a GROUP resource is the only writer. */
static void seed_group(kafka_admin_AdminClient_t *admin, const char *group_id) {
    alter_one_config(admin, RESOURCE_TYPE_GROUP, group_id, "consumer.session.timeout.ms", "45000",
                     OP_TYPE_SET);
}

/* Returns the index of `group_id` in a listConsumerGroupOffsets result, or -1. */
static int32_t find_group_offsets_key(const kafka_admin_ListConsumerGroupOffsetsResult_t *result,
                                      const char *group_id) {
    int32_t n = kafka_admin_ListConsumerGroupOffsetsResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *k = kafka_admin_ListConsumerGroupOffsetsResult_get_group_id(result, i);
        if (k != NULL && strcmp(k, group_id) == 0) {
            return i;
        }
    }
    return -1;
}

/* Returns the index of (topic, partition) in an offset map, or -1. */
static int32_t find_offset_entry(const kafka_admin_OffsetAndMetadataMap_t *map,
                                 const char *topic, int32_t partition) {
    int32_t n = kafka_admin_OffsetAndMetadataMap_count(map);
    for (int32_t i = 0; i < n; i++) {
        const char *t = kafka_admin_OffsetAndMetadataMap_get_topic(map, i);
        if (t != NULL && strcmp(t, topic) == 0 &&
            kafka_admin_OffsetAndMetadataMap_get_partition(map, i) == partition) {
            return i;
        }
    }
    return -1;
}

/* Returns the index of `group_id` in a listGroups result's valid listings. */
static int32_t find_group_listing(const kafka_admin_ListGroupsResult_t *result,
                                  const char *group_id) {
    int32_t n = kafka_admin_ListGroupsResult_valid_count(result);
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_GroupListing_t *listing =
            kafka_admin_ListGroupsResult_get_valid(result, i);
        const char *k = listing == NULL ? NULL : kafka_admin_GroupListing_group_id(listing);
        if (k != NULL && strcmp(k, group_id) == 0) {
            return i;
        }
    }
    return -1;
}

// ---- listGroups ------------------------------------------------------------

static void test_mock_admin_list_groups_reports_seeded_groups(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    seed_group(admin, "lg-a");
    seed_group(admin, "lg-b");

    kafka_admin_ListGroupsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_list_groups(
        admin, NULL, 0, NULL, 0, NULL, 0, -1, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_ListGroupsResult_valid_count(result));
    /* The mock never reports a per-broker failure, so the error list is empty
     * and is *not* parallel to the listing list. */
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListGroupsResult_error_count(result));
    TEST_ASSERT_NULL(kafka_admin_ListGroupsResult_get_error(result, 0));

    int32_t i = find_group_listing(result, "lg-a");
    TEST_ASSERT_TRUE(i >= 0);
    const kafka_admin_GroupListing_t *listing = kafka_admin_ListGroupsResult_get_valid(result, i);
    TEST_ASSERT_NOT_NULL(listing);
    /* MockAdminClient.java:730 builds every listing as CONSUMER / "consumer" /
     * STABLE. `GroupType.toString()` is "Consumer" (capitalised); the protocol
     * type is the lower-case wire string, and the two are unrelated. */
    TEST_ASSERT_EQUAL_STRING("Consumer", kafka_admin_GroupListing_group_type(listing));
    TEST_ASSERT_EQUAL_STRING("consumer", kafka_admin_GroupListing_protocol(listing));
    TEST_ASSERT_EQUAL_STRING("Stable", kafka_admin_GroupListing_group_state(listing));
    /* A CONSUMER-type group with a non-empty protocol is not simple. */
    TEST_ASSERT_FALSE(kafka_admin_GroupListing_is_simple_consumer_group(listing));

    TEST_ASSERT_TRUE(find_group_listing(result, "lg-b") >= 0);
    TEST_ASSERT_NULL(kafka_admin_ListGroupsResult_get_valid(result, 2));
    TEST_ASSERT_NULL(kafka_admin_ListGroupsResult_get_valid(result, -1));
    kafka_admin_ListGroupsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_list_groups_with_no_groups_is_empty(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* Filters are accepted and passed through; the mock ignores its options
     * argument entirely (MockAdminClient.java:728), so this only proves the
     * name arrays marshal without error. */
    const char *states[1] = {"Stable"};
    const char *protocols[1] = {"consumer"};
    const char *types[1] = {"Consumer"};

    kafka_admin_ListGroupsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_groups(
        admin, states, 1, protocols, 1, types, 1, 5000, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListGroupsResult_valid_count(result));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListGroupsResult_error_count(result));
    kafka_admin_ListGroupsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t valid_count;
    int32_t error_count;
} list_groups_async_result_t;

static void on_list_groups(kafka_admin_ListGroupsResult_t *result,
                           kafka_common_Error_t *error, void *user_data) {
    list_groups_async_result_t *r = (list_groups_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    if (result != NULL) {
        r->valid_count = kafka_admin_ListGroupsResult_valid_count(result);
        r->error_count = kafka_admin_ListGroupsResult_error_count(result);
        kafka_admin_ListGroupsResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_groups_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    seed_group(admin, "lg-async");

    list_groups_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_groups_async(admin, NULL, 0, NULL, 0, NULL, 0, -1,
                                              on_list_groups, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.valid_count);
    TEST_ASSERT_EQUAL_INT32(0, r.error_count);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_list_groups_async_null_handle(void) {
    list_groups_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_groups_async(NULL, NULL, 0, NULL, 0, NULL, 0, -1,
                                              on_list_groups, &r);
    /* Fires inline on this thread, before the call returns. */
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
}

// ---- listConsumerGroups ----------------------------------------------------

static void test_mock_admin_list_consumer_groups_reports_seeded_groups(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    seed_group(admin, "lcg-a");

    kafka_admin_ListConsumerGroupsResult_t *result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_consumer_groups(admin, NULL, 0, NULL, 0, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_ListConsumerGroupsResult_valid_count(result));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_ListConsumerGroupsResult_error_count(result));

    const kafka_admin_ConsumerGroupListing_t *listing =
        kafka_admin_ListConsumerGroupsResult_get_valid(result, 0);
    TEST_ASSERT_NOT_NULL(listing);
    TEST_ASSERT_EQUAL_STRING("lcg-a", kafka_admin_ConsumerGroupListing_group_id(listing));
    /* MockAdminClient.java:743 uses `new ConsumerGroupListing(g, false)`, whose
     * state and type are empty Optionals: null strings here, not "Unknown". */
    TEST_ASSERT_FALSE(kafka_admin_ConsumerGroupListing_is_simple_consumer_group(listing));
    TEST_ASSERT_NULL(kafka_admin_ConsumerGroupListing_group_state(listing));
    TEST_ASSERT_NULL(kafka_admin_ConsumerGroupListing_state(listing));
    TEST_ASSERT_NULL(kafka_admin_ConsumerGroupListing_group_type(listing));

    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupsResult_get_valid(result, 1));
    kafka_admin_ListConsumerGroupsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t valid_count;
} list_consumer_groups_async_result_t;

static void on_list_consumer_groups(kafka_admin_ListConsumerGroupsResult_t *result,
                                    kafka_common_Error_t *error, void *user_data) {
    list_consumer_groups_async_result_t *r = (list_consumer_groups_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    if (result != NULL) {
        r->valid_count = kafka_admin_ListConsumerGroupsResult_valid_count(result);
        kafka_admin_ListConsumerGroupsResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_consumer_groups_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    seed_group(admin, "lcg-async");

    list_consumer_groups_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_consumer_groups_async(admin, NULL, 0, NULL, 0, -1,
                                                       on_list_consumer_groups, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.valid_count);

    kafka_admin_AdminClient_destroy(admin);
}

// ---- describeConsumerGroups / describeClassicGroups ------------------------

static void test_mock_admin_describe_consumer_groups_reports_unsupported_per_group(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *groups[2] = {"dg-a", "dg-b"};

    kafka_admin_DescribeConsumerGroupsResult_t *result = NULL;
    /* `describedGroups()` is one future per group, so the mock's
     * UnsupportedOperationException (MockAdminClient.java:735-737) lands in
     * every per-group slot, not as a call failure. */
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_consumer_groups(admin, groups, 2, -1, true, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DescribeConsumerGroupsResult_count(result));
    /* Entries are sorted by group id. */
    TEST_ASSERT_EQUAL_STRING("dg-a",
                             kafka_admin_DescribeConsumerGroupsResult_get_group_id(result, 0));
    TEST_ASSERT_EQUAL_STRING("dg-b",
                             kafka_admin_DescribeConsumerGroupsResult_get_group_id(result, 1));
    for (int32_t i = 0; i < 2; i++) {
        TEST_ASSERT_NULL(kafka_admin_DescribeConsumerGroupsResult_get_value(result, i));
        const kafka_common_Error_t *e =
            kafka_admin_DescribeConsumerGroupsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_Error_code(e));
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    }
    TEST_ASSERT_NULL(kafka_admin_DescribeConsumerGroupsResult_get_group_id(result, 2));
    TEST_ASSERT_NULL(kafka_admin_DescribeConsumerGroupsResult_get_error(result, -1));
    kafka_admin_DescribeConsumerGroupsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_classic_groups_reports_unsupported_per_group(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *groups[1] = {"dcg"};

    kafka_admin_DescribeClassicGroupsResult_t *result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_classic_groups(admin, groups, 1, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeClassicGroupsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("dcg",
                             kafka_admin_DescribeClassicGroupsResult_get_group_id(result, 0));
    TEST_ASSERT_NULL(kafka_admin_DescribeClassicGroupsResult_get_value(result, 0));
    const kafka_common_Error_t *e =
        kafka_admin_DescribeClassicGroupsResult_get_error(result, 0);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    kafka_admin_DescribeClassicGroupsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t first_error_code;
} describe_groups_async_result_t;

static void on_describe_consumer_groups(kafka_admin_DescribeConsumerGroupsResult_t *result,
                                        kafka_common_Error_t *error, void *user_data) {
    describe_groups_async_result_t *r = (describe_groups_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    r->first_error_code = 0;
    if (result != NULL) {
        r->count = kafka_admin_DescribeConsumerGroupsResult_count(result);
        const kafka_common_Error_t *e =
            kafka_admin_DescribeConsumerGroupsResult_get_error(result, 0);
        if (e != NULL) {
            r->first_error_code = kafka_common_Error_code(e);
        }
        kafka_admin_DescribeConsumerGroupsResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_consumer_groups_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *groups[1] = {"dg-async"};

    describe_groups_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_consumer_groups_async(admin, groups, 1, -1, false,
                                                           on_describe_consumer_groups, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, r.first_error_code);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_consumer_groups_async_null_handle(void) {
    describe_groups_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_consumer_groups_async(NULL, NULL, 0, -1, false,
                                                           on_describe_consumer_groups, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
}

// ---- listConsumerGroupOffsets ----------------------------------------------

/* Seeds two committed offsets and reads them back both ways: with the group's
 * partition selection unset (Java's null Collection, "everything") and with an
 * explicit one-partition selection. */
static void test_mock_admin_list_consumer_group_offsets_round_trip(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *seed_topics[2] = {"og-a", "og-b"};
    const int32_t seed_partitions[2] = {0, 1};
    const int64_t seed_offsets[2] = {17, 23};
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_update_consumer_group_offsets(
        admin, seed_topics, seed_partitions, seed_offsets, 2));

    const char *groups[1] = {"og-group"};
    const bool all_partitions[1] = {true};
    const int32_t counts[1] = {0};

    kafka_admin_ListConsumerGroupOffsetsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, groups, all_partitions, NULL, NULL, counts, 1, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_ListConsumerGroupOffsetsResult_count(result));
    int32_t g = find_group_offsets_key(result, "og-group");
    TEST_ASSERT_TRUE(g >= 0);
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_get_error(result, g));

    const kafka_admin_OffsetAndMetadataMap_t *map =
        kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, g);
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_OffsetAndMetadataMap_count(map));

    int32_t i = find_offset_entry(map, "og-a", 0);
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_TRUE(kafka_admin_OffsetAndMetadataMap_has_offset(map, i));
    TEST_ASSERT_EQUAL_INT64(17, kafka_admin_OffsetAndMetadataMap_get_offset(map, i));
    /* Java's one-argument OffsetAndMetadata constructor normalises the metadata
     * to "", so it is a non-null empty string rather than NULL. */
    TEST_ASSERT_EQUAL_STRING("", kafka_admin_OffsetAndMetadataMap_get_metadata(map, i));
    int32_t epoch = -99;
    TEST_ASSERT_FALSE(kafka_admin_OffsetAndMetadataMap_get_leader_epoch(map, i, &epoch));
    TEST_ASSERT_EQUAL_INT32(-99, epoch);

    i = find_offset_entry(map, "og-b", 1);
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_EQUAL_INT64(23, kafka_admin_OffsetAndMetadataMap_get_offset(map, i));

    /* Out-of-range indices are inert, never a crash. */
    TEST_ASSERT_NULL(kafka_admin_OffsetAndMetadataMap_get_topic(map, 2));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_OffsetAndMetadataMap_get_partition(map, -1));
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_OffsetAndMetadataMap_get_offset(map, 2));
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);

    /* Now with an explicit selection: `all_partitions = false` plus this
     * group's own ragged arrays. The result is narrower than the seeded map,
     * which fails if the selection were ignored. */
    const char *g0_topics[1] = {"og-b"};
    const int32_t g0_partitions[1] = {1};
    const char *const *const topics[1] = {g0_topics};
    const int32_t *const partitions[1] = {g0_partitions};
    const bool some_partitions[1] = {false};
    const int32_t some_counts[1] = {1};

    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, groups, some_partitions, topics, partitions, some_counts, 1, -1, true, &result));
    TEST_ASSERT_NOT_NULL(result);
    map = kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, 0);
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_OffsetAndMetadataMap_count(map));
    TEST_ASSERT_EQUAL_STRING("og-b", kafka_admin_OffsetAndMetadataMap_get_topic(map, 0));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_OffsetAndMetadataMap_get_partition(map, 0));
    TEST_ASSERT_EQUAL_INT64(23, kafka_admin_OffsetAndMetadataMap_get_offset(map, 0));
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_list_consumer_group_offsets_rejects_a_negative_seeded_offset(void) {
    /* `update_consumer_group_offsets` does not validate: Java's
     * `updateConsumerGroupOffsets` is an unvalidated `putAll`
     * (MockAdminClient.java:1493-1495), so -1 -- Kafka's own invalid-offset
     * sentinel -- is seedable from C. Listing then has to build an
     * `OffsetAndMetadata` from it, which Java rejects with
     * `IllegalArgumentException("Invalid negative offset")`
     * (MockAdminClient.java:756, OffsetAndMetadata.java:49-50).
     *
     * This test exists because the Rust mock used to `.expect()` that
     * construction. The FFI runs the submit closure inline on the calling
     * thread, so the panic unwound out of `extern "C"` and aborted the process
     * -- this test would not have failed, it would have crashed the binary. */
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *seed_topics[2] = {"neg-a", "neg-b"};
    const int32_t seed_partitions[2] = {0, 1};
    const int64_t seed_offsets[2] = {5, -1};
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_update_consumer_group_offsets(
        admin, seed_topics, seed_partitions, seed_offsets, 2));

    const char *groups[1] = {"neg-group"};
    const bool all_partitions[1] = {true};
    const int32_t counts[1] = {0};

    kafka_admin_ListConsumerGroupOffsetsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, groups, all_partitions, NULL, NULL, counts, 1, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_ListConsumerGroupOffsetsResult_count(result));
    int32_t g = find_group_offsets_key(result, "neg-group");
    TEST_ASSERT_TRUE(g >= 0);
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, g));
    const kafka_common_Error_t *e =
        kafka_admin_ListConsumerGroupOffsetsResult_get_error(result, g);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_STRING("Invalid negative offset", kafka_common_Error_message(e));
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);

    /* Reaching this line at all is the point: the handle is still usable, so the
     * error was returned rather than the process aborted. Overwriting the bad
     * offset makes the same call succeed. */
    const char *fix_topics[1] = {"neg-b"};
    const int32_t fix_partitions[1] = {1};
    const int64_t fix_offsets[1] = {9};
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_update_consumer_group_offsets(
        admin, fix_topics, fix_partitions, fix_offsets, 1));

    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, groups, all_partitions, NULL, NULL, counts, 1, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    g = find_group_offsets_key(result, "neg-group");
    TEST_ASSERT_TRUE(g >= 0);
    TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_get_error(result, g));
    const kafka_admin_OffsetAndMetadataMap_t *map =
        kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, g);
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_OffsetAndMetadataMap_count(map));
    int32_t i = find_offset_entry(map, "neg-b", 1);
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_EQUAL_INT64(9, kafka_admin_OffsetAndMetadataMap_get_offset(map, i));
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_list_consumer_group_offsets_rejects_bad_group_ids(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const bool all_partitions[2] = {true, true};
    const int32_t counts[2] = {0, 0};

    const char *with_null[2] = {"g", NULL};
    kafka_admin_ListConsumerGroupOffsetsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, with_null, all_partitions, NULL, NULL, counts, 2, -1, false, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("group id at index 1 must not be null",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* Java takes a Map, where the second entry would silently replace the
     * first, so a duplicate group id is rejected rather than dropped. */
    const char *duplicated[2] = {"g", "g"};
    result = NULL;
    err = kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, duplicated, all_partitions, NULL, NULL, counts, 2, -1, false, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("group id `g` appears more than once at index 1",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_list_consumer_group_offsets_two_groups_are_unsupported(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* MockAdminClient.java:748-751 handles exactly one group and otherwise
     * throws; the Rust mock fails each group's future instead, so the failure
     * lands per group. */
    const char *groups[2] = {"m1", "m2"};
    const bool all_partitions[2] = {true, true};
    const int32_t counts[2] = {0, 0};

    kafka_admin_ListConsumerGroupOffsetsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, groups, all_partitions, NULL, NULL, counts, 2, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_ListConsumerGroupOffsetsResult_count(result));
    for (int32_t i = 0; i < 2; i++) {
        TEST_ASSERT_NULL(kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, i));
        const kafka_common_Error_t *e =
            kafka_admin_ListConsumerGroupOffsetsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    }
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t offset_count;
    int64_t first_offset;
} list_group_offsets_async_result_t;

static void on_list_consumer_group_offsets(kafka_admin_ListConsumerGroupOffsetsResult_t *result,
                                           kafka_common_Error_t *error, void *user_data) {
    list_group_offsets_async_result_t *r = (list_group_offsets_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    r->offset_count = -1;
    r->first_offset = -1;
    if (result != NULL) {
        r->count = kafka_admin_ListConsumerGroupOffsetsResult_count(result);
        const kafka_admin_OffsetAndMetadataMap_t *map =
            kafka_admin_ListConsumerGroupOffsetsResult_get_value(result, 0);
        if (map != NULL) {
            r->offset_count = kafka_admin_OffsetAndMetadataMap_count(map);
            r->first_offset = kafka_admin_OffsetAndMetadataMap_get_offset(map, 0);
        }
        kafka_admin_ListConsumerGroupOffsetsResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_list_consumer_group_offsets_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *seed_topics[1] = {"oga"};
    const int32_t seed_partitions[1] = {2};
    const int64_t seed_offsets[1] = {99};
    TEST_ASSERT_NULL(kafka_admin_MockAdminClient_update_consumer_group_offsets(
        admin, seed_topics, seed_partitions, seed_offsets, 1));

    const char *groups[1] = {"oga-group"};
    const bool all_partitions[1] = {true};
    const int32_t counts[1] = {0};

    list_group_offsets_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_consumer_group_offsets_async(
        admin, groups, all_partitions, NULL, NULL, counts, 1, -1, false,
        on_list_consumer_group_offsets, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT32(1, r.offset_count);
    TEST_ASSERT_EQUAL_INT64(99, r.first_offset);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_list_consumer_group_offsets_async_null_group_id(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *groups[1] = {NULL};
    const bool all_partitions[1] = {true};
    const int32_t counts[1] = {0};

    list_group_offsets_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_list_consumer_group_offsets_async(
        admin, groups, all_partitions, NULL, NULL, counts, 1, -1, false,
        on_list_consumer_group_offsets, &r);
    /* Marshaling failed, so the callback fired inline before returning. */
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);

    kafka_admin_AdminClient_destroy(admin);
}

// ---- alterConsumerGroupOffsets ---------------------------------------------

static void test_mock_admin_alter_consumer_group_offsets_reports_unsupported_per_partition(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *topics[2] = {"ac", "ac"};
    const int32_t partitions[2] = {0, 1};
    const int64_t offsets[2] = {5, 6};
    const char *metadata[2] = {"m0", NULL};
    const int32_t epochs[2] = {0, 0};
    const bool has_epoch[2] = {true, false};

    kafka_admin_AlterConsumerGroupOffsetsResult_t *result = NULL;
    /* Java's `partitionResult(tp)` is one KafkaFuture<Void> per requested
     * partition, so the mock's "Not implement yet" (Java's own typo,
     * MockAdminClient.java:1213) lands per partition. */
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, "acg", topics, partitions, offsets, metadata, epochs, has_epoch, 2, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_AlterConsumerGroupOffsetsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("ac", kafka_admin_AlterConsumerGroupOffsetsResult_get_topic(result, 0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_AlterConsumerGroupOffsetsResult_get_partition(result, 0));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_AlterConsumerGroupOffsetsResult_get_partition(result, 1));
    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_AlterConsumerGroupOffsetsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implement yet", kafka_common_Error_message(e));
    }
    kafka_admin_AlterConsumerGroupOffsetsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_alter_consumer_group_offsets_rejects_bad_input(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t partitions[1] = {0};

    /* A negative offset: Java's OffsetAndMetadata constructor throws, and the
     * index prefix tells a C caller which array entry was at fault. */
    const char *topics[1] = {"ac"};
    const int64_t bad_offsets[1] = {-1};
    kafka_admin_AlterConsumerGroupOffsetsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, "acg", topics, partitions, bad_offsets, NULL, NULL, NULL, 1, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("offset at index 0: Invalid negative offset",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* A NULL topic entry. */
    const char *null_topics[1] = {NULL};
    const int64_t offsets[1] = {0};
    result = NULL;
    err = kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, "acg", null_topics, partitions, offsets, NULL, NULL, NULL, 1, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("topic at index 0 must not be null",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* A NULL group id. */
    result = NULL;
    err = kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, NULL, topics, partitions, offsets, NULL, NULL, NULL, 1, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("group_id must not be null", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_alter_consumer_group_offsets_with_no_partitions_fails_the_call(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* With no requested partition there is no per-key slot for the outcome, so
     * the whole-request error is returned instead — Java's `all()` is then the
     * only observable too. */
    kafka_admin_AlterConsumerGroupOffsetsResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, "acg", NULL, NULL, NULL, NULL, NULL, NULL, 0, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Not implement yet", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} alter_group_offsets_async_result_t;

static void on_alter_consumer_group_offsets(kafka_admin_AlterConsumerGroupOffsetsResult_t *result,
                                            kafka_common_Error_t *error, void *user_data) {
    alter_group_offsets_async_result_t *r = (alter_group_offsets_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    if (result != NULL) {
        r->count = kafka_admin_AlterConsumerGroupOffsetsResult_count(result);
        kafka_admin_AlterConsumerGroupOffsetsResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_alter_consumer_group_offsets_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *topics[1] = {"aca"};
    const int32_t partitions[1] = {0};
    const int64_t offsets[1] = {1};

    alter_group_offsets_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_consumer_group_offsets_async(
        admin, "acg", topics, partitions, offsets, NULL, NULL, NULL, 1, -1,
        on_alter_consumer_group_offsets, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);

    /* A negative offset cannot be submitted at all, so the callback fires
     * inline with an error and no result. */
    const int64_t bad_offsets[1] = {-5};
    alter_group_offsets_async_result_t bad = {0};
    atomic_init(&bad.fired, 0);
    kafka_admin_AdminClient_alter_consumer_group_offsets_async(
        admin, "acg", topics, partitions, bad_offsets, NULL, NULL, NULL, 1, -1,
        on_alter_consumer_group_offsets, &bad);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&bad.fired));
    TEST_ASSERT_EQUAL_INT(0, bad.had_result);
    TEST_ASSERT_EQUAL_INT(1, bad.had_error);

    kafka_admin_AdminClient_destroy(admin);
}

// ---- deleteConsumerGroupOffsets --------------------------------------------

static void test_mock_admin_delete_consumer_group_offsets_reports_unsupported_per_partition(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *topics[2] = {"dc", "dc"};
    const int32_t partitions[2] = {0, 1};

    kafka_admin_DeleteConsumerGroupOffsetsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_consumer_group_offsets(
        admin, "dcg", topics, partitions, 2, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DeleteConsumerGroupOffsetsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("dc",
                             kafka_admin_DeleteConsumerGroupOffsetsResult_get_topic(result, 0));
    TEST_ASSERT_EQUAL_INT32(1,
                            kafka_admin_DeleteConsumerGroupOffsetsResult_get_partition(result, 1));
    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_DeleteConsumerGroupOffsetsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    }
    TEST_ASSERT_EQUAL_INT32(-1,
                            kafka_admin_DeleteConsumerGroupOffsetsResult_get_partition(result, -1));
    kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} delete_group_offsets_async_result_t;

static void on_delete_consumer_group_offsets(kafka_admin_DeleteConsumerGroupOffsetsResult_t *result,
                                             kafka_common_Error_t *error, void *user_data) {
    delete_group_offsets_async_result_t *r = (delete_group_offsets_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DeleteConsumerGroupOffsetsResult_count(result);
        kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_delete_consumer_group_offsets_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *topics[1] = {"dca"};
    const int32_t partitions[1] = {3};

    delete_group_offsets_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_delete_consumer_group_offsets_async(
        admin, "dcg", topics, partitions, 1, -1, on_delete_consumer_group_offsets, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);

    kafka_admin_AdminClient_destroy(admin);
}

// ---- deleteConsumerGroups --------------------------------------------------

static void test_mock_admin_delete_consumer_groups_reports_unsupported_per_group(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *groups[2] = {"z-group", "a-group"};

    kafka_admin_DeleteConsumerGroupsResult_t *result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_delete_consumer_groups(admin, groups, 2, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DeleteConsumerGroupsResult_count(result));
    /* Entries are sorted by group id, not left in request order. */
    TEST_ASSERT_EQUAL_STRING("a-group",
                             kafka_admin_DeleteConsumerGroupsResult_get_group_id(result, 0));
    TEST_ASSERT_EQUAL_STRING("z-group",
                             kafka_admin_DeleteConsumerGroupsResult_get_group_id(result, 1));
    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_DeleteConsumerGroupsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    }
    TEST_ASSERT_NULL(kafka_admin_DeleteConsumerGroupsResult_get_error(result, 2));
    kafka_admin_DeleteConsumerGroupsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} delete_groups_async_result_t;

static void on_delete_consumer_groups(kafka_admin_DeleteConsumerGroupsResult_t *result,
                                      kafka_common_Error_t *error, void *user_data) {
    delete_groups_async_result_t *r = (delete_groups_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DeleteConsumerGroupsResult_count(result);
        kafka_admin_DeleteConsumerGroupsResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_delete_consumer_groups_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *groups[1] = {"dg-async"};

    delete_groups_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_delete_consumer_groups_async(admin, groups, 1, -1,
                                                         on_delete_consumer_groups, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);

    kafka_admin_AdminClient_destroy(admin);
}

// ---- removeMembersFromConsumerGroup ----------------------------------------

static void test_mock_admin_remove_members_reports_unsupported_per_member(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *members[2] = {"instance-b", "instance-a"};

    kafka_admin_RemoveMembersFromConsumerGroupResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_remove_members_from_consumer_group(
        admin, "rm-group", false, members, 2, "rolling restart", -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_RemoveMembersFromConsumerGroupResult_count(result));
    /* Keyed by group instance id, sorted. */
    TEST_ASSERT_EQUAL_STRING(
        "instance-a",
        kafka_admin_RemoveMembersFromConsumerGroupResult_get_group_instance_id(result, 0));
    TEST_ASSERT_EQUAL_STRING(
        "instance-b",
        kafka_admin_RemoveMembersFromConsumerGroupResult_get_group_instance_id(result, 1));
    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_RemoveMembersFromConsumerGroupResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    }
    kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_remove_all_members_has_no_per_member_outcome(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* `remove_all` is Java's no-argument constructor, where `memberResult` is
     * "not applicable in 'removeAll' mode" and `all()` is the only observable.
     * The member array is deliberately non-empty: the flag must win. */
    const char *members[1] = {"ignored"};

    kafka_admin_RemoveMembersFromConsumerGroupResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_remove_members_from_consumer_group(
        admin, "rm-group", true, members, 1, NULL, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_remove_members_rejects_an_empty_member_list(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* Java's Collection constructor throws for an empty collection, so an
     * empty array must not silently become "remove everything". */
    kafka_admin_RemoveMembersFromConsumerGroupResult_t *result = NULL;
    kafka_common_Error_t *err = kafka_admin_AdminClient_remove_members_from_consumer_group(
        admin, "rm-group", false, NULL, 0, NULL, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Invalid empty members has been provided",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    /* And a NULL group id is rejected before anything else. */
    result = NULL;
    const char *members[1] = {"i"};
    err = kafka_admin_AdminClient_remove_members_from_consumer_group(admin, NULL, false, members, 1,
                                                                     NULL, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("group_id must not be null", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} remove_members_async_result_t;

static void on_remove_members(kafka_admin_RemoveMembersFromConsumerGroupResult_t *result,
                              kafka_common_Error_t *error, void *user_data) {
    remove_members_async_result_t *r = (remove_members_async_result_t *)user_data;
    r->had_result = result != NULL;
    r->had_error = error != NULL;
    if (result != NULL) {
        r->count = kafka_admin_RemoveMembersFromConsumerGroupResult_count(result);
        kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(result);
    }
    if (error != NULL) {
        kafka_common_Error_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_remove_members_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *members[1] = {"instance-async"};

    remove_members_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_remove_members_from_consumer_group_async(
        admin, "rm-group", false, members, 1, NULL, -1, on_remove_members, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);

    /* An empty member list without `remove_all` cannot be submitted, so the
     * callback fires inline with an error. */
    remove_members_async_result_t bad = {0};
    atomic_init(&bad.fired, 0);
    kafka_admin_AdminClient_remove_members_from_consumer_group_async(
        admin, "rm-group", false, NULL, 0, NULL, -1, on_remove_members, &bad);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&bad.fired));
    TEST_ASSERT_EQUAL_INT(0, bad.had_result);
    TEST_ASSERT_EQUAL_INT(1, bad.had_error);

    kafka_admin_AdminClient_destroy(admin);
}

// ---- Cross-cutting ---------------------------------------------------------

static void test_mock_admin_group_offsets_driver_rejects_non_mock(void) {
    kafka_admin_AdminClientProperties_t *props = kafka_admin_AdminClientProperties_new();
    kafka_admin_AdminClientProperties_put(props, "bootstrap.servers", "localhost:9092");
    kafka_common_Error_t *new_err = NULL;
    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, &new_err);
    kafka_admin_AdminClientProperties_destroy(props);
    TEST_ASSERT_NULL(new_err);
    TEST_ASSERT_NOT_NULL(admin);

    const char *topics[1] = {"t"};
    const int32_t partitions[1] = {0};
    const int64_t offsets[1] = {1};
    kafka_common_Error_t *err = kafka_admin_MockAdminClient_update_consumer_group_offsets(
        admin, topics, partitions, offsets, 1);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("this operation is only supported on a MockAdminClient",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);

    kafka_admin_AdminClient_close(admin, 1000);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL `out_result` must not leak the result handle or crash: the sync entry
 * points simply do not build one. */
static void test_mock_admin_b4_null_out_result(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    seed_group(admin, "nb4");
    const char *groups[1] = {"nb4"};
    const bool all_partitions[1] = {true};
    const int32_t counts[1] = {0};
    const char *topics[1] = {"t"};
    const int32_t partitions[1] = {0};
    const int64_t offsets[1] = {1};
    const char *members[1] = {"i"};

    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_groups(admin, NULL, 0, NULL, 0, NULL, 0, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_groups(admin, NULL, 0, NULL, 0, -1, NULL));
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_consumer_groups(admin, groups, 1, -1, false, NULL));
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_classic_groups(admin, groups, 1, -1, false, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_consumer_group_offsets(
        admin, groups, all_partitions, NULL, NULL, counts, 1, -1, false, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_consumer_group_offsets(
        admin, "nb4", topics, partitions, offsets, NULL, NULL, NULL, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_consumer_group_offsets(
        admin, "nb4", topics, partitions, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_consumer_groups(admin, groups, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_remove_members_from_consumer_group(
        admin, "nb4", false, members, 1, NULL, -1, NULL));

    kafka_admin_AdminClient_destroy(admin);
}

// ---------------------------------------------------------------------------
// B5a — ACLs and client quotas
//
// Java's own MockAdminClient throws UnsupportedOperationException for all five
// of these RPCs — createAcls (MockAdminClient.java:806), describeAcls (:811),
// deleteAcls (:816), describeClientQuotas (:1243) and alterClientQuotas
// (:1248) — so nothing here can reach a populated success path. What these
// tests do cover, and the Rust unit tests cannot, is the C-visible surface:
// that each entry point links, that its marshaling accepts and rejects the
// right shapes with Java's exact messages, that the "unsupported" outcome
// lands in the right slot for each of the three result shapes, and that the
// two-level array parameter types decay correctly from C.
//
// Note the two different Java strings: the ACL RPCs throw "Not implemented
// yet", the quota RPCs "Not implement yet" (Java's own typo, preserved).
// ---------------------------------------------------------------------------

/* Java AclOperation / AclPermissionType / ResourceType / PatternType codes. */
#define ACL_RESOURCE_TYPE_UNKNOWN 0
#define ACL_RESOURCE_TYPE_ANY 1
#define ACL_RESOURCE_TYPE_TOPIC 2
#define ACL_RESOURCE_TYPE_GROUP 3
#define ACL_PATTERN_TYPE_ANY 1
#define ACL_PATTERN_TYPE_MATCH 2
#define ACL_PATTERN_TYPE_LITERAL 3
#define ACL_PATTERN_TYPE_PREFIXED 4
#define ACL_OPERATION_ANY 1
#define ACL_OPERATION_READ 3
#define ACL_OPERATION_WRITE 4
#define ACL_OPERATION_DESCRIBE 8
#define ACL_PERMISSION_ANY 1
#define ACL_PERMISSION_DENY 2
#define ACL_PERMISSION_ALLOW 3

/* Wire match types for a client-quota filter component. */
#define QUOTA_MATCH_EXACT 0
#define QUOTA_MATCH_DEFAULT 1
#define QUOTA_MATCH_SPECIFIED 2

// ---- createAcls -------------------------------------------------------------

static void test_mock_admin_create_acls_reports_unsupported_per_binding(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* Two bindings whose every field differs, so a column read from the wrong
     * array shows up. Request order is z-then-a; the result is sorted. */
    const int32_t resource_types[2] = {ACL_RESOURCE_TYPE_TOPIC, ACL_RESOURCE_TYPE_GROUP};
    const char *resource_names[2] = {"z-topic", "a-group"};
    const int32_t pattern_types[2] = {ACL_PATTERN_TYPE_LITERAL, ACL_PATTERN_TYPE_PREFIXED};
    const char *principals[2] = {"User:zoe", "User:alice"};
    const char *hosts[2] = {"10.0.0.9", "10.0.0.1"};
    const int32_t operations[2] = {ACL_OPERATION_WRITE, ACL_OPERATION_READ};
    const int32_t permissions[2] = {ACL_PERMISSION_DENY, ACL_PERMISSION_ALLOW};

    kafka_admin_CreateAclsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_acls(admin, resource_types, resource_names,
                                                         pattern_types, principals, hosts,
                                                         operations, permissions, 2, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_CreateAclsResult_count(result));

    /* Sorted by resource type first, so GROUP (3) sorts after TOPIC (2): the
     * z-topic row is index 0 even though its name sorts last. Checking the
     * whole row together is what catches a column swap. */
    const kafka_common_AclBinding_t *b0 = kafka_admin_CreateAclsResult_get_binding(result, 0);
    TEST_ASSERT_NOT_NULL(b0);
    TEST_ASSERT_EQUAL_INT32(ACL_RESOURCE_TYPE_TOPIC, kafka_common_AclBinding_resource_type(b0));
    TEST_ASSERT_EQUAL_STRING("z-topic", kafka_common_AclBinding_resource_name(b0));
    TEST_ASSERT_EQUAL_INT32(ACL_PATTERN_TYPE_LITERAL, kafka_common_AclBinding_pattern_type(b0));
    TEST_ASSERT_EQUAL_STRING("User:zoe", kafka_common_AclBinding_principal(b0));
    TEST_ASSERT_EQUAL_STRING("10.0.0.9", kafka_common_AclBinding_host(b0));
    TEST_ASSERT_EQUAL_INT32(ACL_OPERATION_WRITE, kafka_common_AclBinding_operation(b0));
    TEST_ASSERT_EQUAL_INT32(ACL_PERMISSION_DENY, kafka_common_AclBinding_permission_type(b0));

    const kafka_common_AclBinding_t *b1 = kafka_admin_CreateAclsResult_get_binding(result, 1);
    TEST_ASSERT_NOT_NULL(b1);
    TEST_ASSERT_EQUAL_INT32(ACL_RESOURCE_TYPE_GROUP, kafka_common_AclBinding_resource_type(b1));
    TEST_ASSERT_EQUAL_STRING("a-group", kafka_common_AclBinding_resource_name(b1));
    TEST_ASSERT_EQUAL_INT32(ACL_PATTERN_TYPE_PREFIXED, kafka_common_AclBinding_pattern_type(b1));
    TEST_ASSERT_EQUAL_STRING("User:alice", kafka_common_AclBinding_principal(b1));
    TEST_ASSERT_EQUAL_STRING("10.0.0.1", kafka_common_AclBinding_host(b1));
    TEST_ASSERT_EQUAL_INT32(ACL_OPERATION_READ, kafka_common_AclBinding_operation(b1));
    TEST_ASSERT_EQUAL_INT32(ACL_PERMISSION_ALLOW, kafka_common_AclBinding_permission_type(b1));

    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e = kafka_admin_CreateAclsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    }
    TEST_ASSERT_NULL(kafka_admin_CreateAclsResult_get_binding(result, 2));
    TEST_ASSERT_NULL(kafka_admin_CreateAclsResult_get_error(result, 2));
    kafka_admin_CreateAclsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_create_acls_rejects_what_javas_constructors_reject(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *names[1] = {"t"};
    const char *principals[1] = {"User:a"};
    const char *hosts[1] = {"*"};

    struct {
        int32_t resource_type;
        int32_t pattern_type;
        int32_t operation;
        int32_t permission;
        const char *message;
    } cases[] = {
        {ACL_RESOURCE_TYPE_ANY, ACL_PATTERN_TYPE_LITERAL, ACL_OPERATION_READ, ACL_PERMISSION_ALLOW,
         "acl at index 0: resourceType must not be ANY"},
        {ACL_RESOURCE_TYPE_TOPIC, ACL_PATTERN_TYPE_MATCH, ACL_OPERATION_READ, ACL_PERMISSION_ALLOW,
         "acl at index 0: patternType must not be MATCH"},
        {ACL_RESOURCE_TYPE_TOPIC, ACL_PATTERN_TYPE_ANY, ACL_OPERATION_READ, ACL_PERMISSION_ALLOW,
         "acl at index 0: patternType must not be ANY"},
        {ACL_RESOURCE_TYPE_TOPIC, ACL_PATTERN_TYPE_LITERAL, ACL_OPERATION_ANY, ACL_PERMISSION_ALLOW,
         "acl at index 0: operation must not be ANY"},
        {ACL_RESOURCE_TYPE_TOPIC, ACL_PATTERN_TYPE_LITERAL, ACL_OPERATION_READ, ACL_PERMISSION_ANY,
         "acl at index 0: permissionType must not be ANY"},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const int32_t rt[1] = {cases[i].resource_type};
        const int32_t pt[1] = {cases[i].pattern_type};
        const int32_t op[1] = {cases[i].operation};
        const int32_t pm[1] = {cases[i].permission};
        kafka_admin_CreateAclsResult_t *result = NULL;
        kafka_common_Error_t *error = kafka_admin_AdminClient_create_acls(
            admin, rt, names, pt, principals, hosts, op, pm, 1, -1, &result);
        TEST_ASSERT_NOT_NULL(error);
        TEST_ASSERT_NULL(result);
        TEST_ASSERT_EQUAL_STRING(cases[i].message, kafka_common_Error_message(error));
        kafka_common_Error_destroy(error);
    }

    /* A NULL entry in a non-nullable string array names the row. */
    const int32_t rt[2] = {ACL_RESOURCE_TYPE_TOPIC, ACL_RESOURCE_TYPE_TOPIC};
    const int32_t pt[2] = {ACL_PATTERN_TYPE_LITERAL, ACL_PATTERN_TYPE_LITERAL};
    const int32_t op[2] = {ACL_OPERATION_READ, ACL_OPERATION_READ};
    const int32_t pm[2] = {ACL_PERMISSION_ALLOW, ACL_PERMISSION_ALLOW};
    const char *two_names[2] = {"t0", "t1"};
    const char *null_principal[2] = {"User:a", NULL};
    const char *two_hosts[2] = {"*", "*"};
    kafka_admin_CreateAclsResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_create_acls(
        admin, rt, two_names, pt, null_principal, two_hosts, op, pm, 2, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("principal at index 1 must not be null",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    char message[128];
} acl_async_result_t;

static void record_async_error(acl_async_result_t *r, kafka_common_Error_t *error) {
    r->had_error = error != NULL;
    if (error != NULL) {
        const char *m = kafka_common_Error_message(error);
        if (m != NULL) {
            snprintf(r->message, sizeof(r->message), "%s", m);
        }
        kafka_common_Error_destroy(error);
    }
}

static void on_create_acls(kafka_admin_CreateAclsResult_t *result,
                           kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_CreateAclsResult_count(result);
        kafka_admin_CreateAclsResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_create_acls_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t rt[1] = {ACL_RESOURCE_TYPE_TOPIC};
    const char *names[1] = {"async-topic"};
    const int32_t pt[1] = {ACL_PATTERN_TYPE_LITERAL};
    const char *principals[1] = {"User:a"};
    const char *hosts[1] = {"*"};
    const int32_t op[1] = {ACL_OPERATION_READ};
    const int32_t pm[1] = {ACL_PERMISSION_ALLOW};

    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_create_acls_async(admin, rt, names, pt, principals, hosts, op, pm, 1,
                                              -1, on_create_acls, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_create_acls_async_reports_marshaling_failure(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t rt[1] = {ACL_RESOURCE_TYPE_ANY};
    const char *names[1] = {"t"};
    const int32_t pt[1] = {ACL_PATTERN_TYPE_LITERAL};
    const char *principals[1] = {"User:a"};
    const char *hosts[1] = {"*"};
    const int32_t op[1] = {ACL_OPERATION_READ};
    const int32_t pm[1] = {ACL_PERMISSION_ALLOW};

    /* The RPC is never submitted, so the callback fires inline on this thread
     * before the call returns — the "cannot be submitted at all" arm of the
     * documented callback-thread contract. */
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_create_acls_async(admin, rt, names, pt, principals, hosts, op, pm, 1,
                                              -1, on_create_acls, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
    TEST_ASSERT_EQUAL_STRING("acl at index 0: resourceType must not be ANY", r.message);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_create_acls_async_null_handle(void) {
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_create_acls_async(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, -1,
                                              on_create_acls, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
}

// ---- describeAcls -----------------------------------------------------------

static void test_mock_admin_describe_acls_fails_the_whole_call(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* DescribeAclsResult holds one future for the whole call, so there is no
     * per-key slot: the failure comes back from the function itself and
     * `out_result` is left untouched. This is the shape difference from
     * createAcls above, and it follows from the Java result type. */
    kafka_admin_DescribeAclsResult_t *result = NULL;
    kafka_common_Error_t *error =
        kafka_admin_AdminClient_describe_acls(admin, ACL_RESOURCE_TYPE_TOPIC, "t",
                                              ACL_PATTERN_TYPE_LITERAL, "User:a", "*",
                                              ACL_OPERATION_READ, ACL_PERMISSION_ALLOW, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_acls_accepts_any_match_and_nulls(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* A *filter* accepts exactly the combinations a *binding* rejects: ANY on
     * all four enums, MATCH pattern types, and NULL strings meaning "match
     * any". Reaching the mock's "Not implemented yet" rather than an
     * IllegalArgument is what proves marshaling accepted them — createAcls
     * with the same enum values fails before it ever reaches the mock. */
    kafka_admin_DescribeAclsResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_describe_acls(
        admin, ACL_RESOURCE_TYPE_ANY, NULL, ACL_PATTERN_TYPE_ANY, NULL, NULL, ACL_OPERATION_ANY,
        ACL_PERMISSION_ANY, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    error = kafka_admin_AdminClient_describe_acls(admin, ACL_RESOURCE_TYPE_TOPIC, "prefix",
                                                  ACL_PATTERN_TYPE_MATCH, NULL, NULL,
                                                  ACL_OPERATION_DESCRIBE, ACL_PERMISSION_DENY, -1,
                                                  &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void on_describe_acls(kafka_admin_DescribeAclsResult_t *result,
                             kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DescribeAclsResult_count(result);
        kafka_admin_DescribeAclsResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_acls_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_acls_async(admin, ACL_RESOURCE_TYPE_ANY, NULL,
                                                ACL_PATTERN_TYPE_ANY, NULL, NULL, ACL_OPERATION_ANY,
                                                ACL_PERMISSION_ANY, -1, on_describe_acls, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    /* Single-future RPC: the mock's failure arrives as `error`, not inside a
     * result handle. */
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_acls_async_null_handle(void) {
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_acls_async(NULL, ACL_RESOURCE_TYPE_ANY, NULL,
                                                ACL_PATTERN_TYPE_ANY, NULL, NULL, ACL_OPERATION_ANY,
                                                ACL_PERMISSION_ANY, -1, on_describe_acls, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
}

// ---- deleteAcls -------------------------------------------------------------

static void test_mock_admin_delete_acls_reports_unsupported_per_filter(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* Row 0 has a null name and principal (match any), row 1 an empty name.
     * The empty name must survive as "" and not collapse into the null. */
    const int32_t resource_types[2] = {ACL_RESOURCE_TYPE_ANY, ACL_RESOURCE_TYPE_TOPIC};
    const char *resource_names[2] = {NULL, ""};
    const int32_t pattern_types[2] = {ACL_PATTERN_TYPE_ANY, ACL_PATTERN_TYPE_LITERAL};
    const char *principals[2] = {NULL, "User:alice"};
    const char *hosts[2] = {NULL, "10.0.0.1"};
    const int32_t operations[2] = {ACL_OPERATION_ANY, ACL_OPERATION_DESCRIBE};
    const int32_t permissions[2] = {ACL_PERMISSION_ANY, ACL_PERMISSION_DENY};

    kafka_admin_DeleteAclsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_acls(admin, resource_types, resource_names,
                                                         pattern_types, principals, hosts,
                                                         operations, permissions, 2, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DeleteAclsResult_count(result));

    /* Sorted by resource type: ANY (1) before TOPIC (2). */
    const kafka_common_AclBindingFilter_t *f0 = kafka_admin_DeleteAclsResult_get_filter(result, 0);
    TEST_ASSERT_NOT_NULL(f0);
    TEST_ASSERT_EQUAL_INT32(ACL_RESOURCE_TYPE_ANY, kafka_common_AclBindingFilter_resource_type(f0));
    TEST_ASSERT_NULL(kafka_common_AclBindingFilter_resource_name(f0));
    TEST_ASSERT_NULL(kafka_common_AclBindingFilter_principal(f0));
    TEST_ASSERT_NULL(kafka_common_AclBindingFilter_host(f0));
    TEST_ASSERT_EQUAL_INT32(ACL_PATTERN_TYPE_ANY, kafka_common_AclBindingFilter_pattern_type(f0));
    TEST_ASSERT_EQUAL_INT32(ACL_OPERATION_ANY, kafka_common_AclBindingFilter_operation(f0));
    TEST_ASSERT_EQUAL_INT32(ACL_PERMISSION_ANY,
                            kafka_common_AclBindingFilter_permission_type(f0));

    const kafka_common_AclBindingFilter_t *f1 = kafka_admin_DeleteAclsResult_get_filter(result, 1);
    TEST_ASSERT_NOT_NULL(f1);
    /* Present but empty: a pointer to "", never the null that means match-any. */
    TEST_ASSERT_NOT_NULL(kafka_common_AclBindingFilter_resource_name(f1));
    TEST_ASSERT_EQUAL_STRING("", kafka_common_AclBindingFilter_resource_name(f1));
    TEST_ASSERT_EQUAL_STRING("User:alice", kafka_common_AclBindingFilter_principal(f1));
    TEST_ASSERT_EQUAL_STRING("10.0.0.1", kafka_common_AclBindingFilter_host(f1));
    TEST_ASSERT_EQUAL_INT32(ACL_PATTERN_TYPE_LITERAL,
                            kafka_common_AclBindingFilter_pattern_type(f1));
    TEST_ASSERT_EQUAL_INT32(ACL_OPERATION_DESCRIBE, kafka_common_AclBindingFilter_operation(f1));
    TEST_ASSERT_EQUAL_INT32(ACL_PERMISSION_DENY,
                            kafka_common_AclBindingFilter_permission_type(f1));

    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e = kafka_admin_DeleteAclsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
        /* A filter whose own future failed deleted nothing, so it has no
         * per-ACL entries — which is a different thing from a filter that
         * succeeded and matched nothing. */
        TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteAclsResult_get_result_count(result, i));
        TEST_ASSERT_NULL(kafka_admin_DeleteAclsResult_get_binding(result, i, 0));
        TEST_ASSERT_NULL(kafka_admin_DeleteAclsResult_get_result_error(result, i, 0));
    }
    TEST_ASSERT_NULL(kafka_admin_DeleteAclsResult_get_filter(result, 2));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DeleteAclsResult_get_result_count(result, 2));
    kafka_admin_DeleteAclsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void on_delete_acls(kafka_admin_DeleteAclsResult_t *result,
                           kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DeleteAclsResult_count(result);
        kafka_admin_DeleteAclsResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_delete_acls_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t rt[1] = {ACL_RESOURCE_TYPE_ANY};
    const char *names[1] = {NULL};
    const int32_t pt[1] = {ACL_PATTERN_TYPE_ANY};
    const char *principals[1] = {NULL};
    const char *hosts[1] = {NULL};
    const int32_t op[1] = {ACL_OPERATION_ANY};
    const int32_t pm[1] = {ACL_PERMISSION_ANY};

    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_delete_acls_async(admin, rt, names, pt, principals, hosts, op, pm, 1,
                                              -1, on_delete_acls, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_delete_acls_async_null_handle(void) {
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_delete_acls_async(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, -1,
                                              on_delete_acls, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
}

// ---- describeClientQuotas ---------------------------------------------------

static void test_mock_admin_describe_client_quotas_fails_the_whole_call(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *entity_types[3] = {"user", "client-id", "ip"};
    const int32_t match_types[3] = {QUOTA_MATCH_EXACT, QUOTA_MATCH_DEFAULT, QUOTA_MATCH_SPECIFIED};
    const char *match_names[3] = {"alice", NULL, NULL};

    /* Like describeAcls, one future for the whole call: no per-key slot, so the
     * mock's failure is the call's error. Note Java's own typo in the message,
     * "Not implement yet", which the ACL RPCs do not share. */
    kafka_admin_DescribeClientQuotasResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_describe_client_quotas(
        admin, entity_types, match_types, match_names, 3, false, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Not implement yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* No components at all is Java's ClientQuotaFilter.all(); still reaches the
     * mock rather than being rejected. */
    error = kafka_admin_AdminClient_describe_client_quotas(admin, NULL, NULL, NULL, 0, false, -1,
                                                           &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("Not implement yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_client_quotas_rejects_bad_components(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *entity_types[1] = {"user"};
    const char *no_name[1] = {NULL};

    /* EXACT with no name: the discriminant says "match this name" and there is
     * none, which Java's `ClientQuotaFilterComponent.ofEntity` could not
     * express either. */
    const int32_t exact[1] = {QUOTA_MATCH_EXACT};
    kafka_admin_DescribeClientQuotasResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_describe_client_quotas(
        admin, entity_types, exact, no_name, 1, false, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("quota filter component at index 0 has match type EXACT but no match name",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    const int32_t bogus[1] = {7};
    error = kafka_admin_AdminClient_describe_client_quotas(admin, entity_types, bogus, no_name, 1,
                                                           false, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("quota filter component at index 0 has unknown match type 7",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    const char *null_type[1] = {NULL};
    const int32_t def[1] = {QUOTA_MATCH_DEFAULT};
    error = kafka_admin_AdminClient_describe_client_quotas(admin, null_type, def, no_name, 1, false,
                                                           -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("entity type at index 0 must not be null",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void on_describe_client_quotas(kafka_admin_DescribeClientQuotasResult_t *result,
                                      kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DescribeClientQuotasResult_count(result);
        kafka_admin_DescribeClientQuotasResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_client_quotas_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_client_quotas_async(admin, NULL, NULL, NULL, 0, false, -1,
                                                         on_describe_client_quotas, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implement yet", r.message);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_client_quotas_async_null_handle(void) {
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_client_quotas_async(NULL, NULL, NULL, NULL, 0, false, -1,
                                                         on_describe_client_quotas, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
}

// ---- alterClientQuotas ------------------------------------------------------

static void test_mock_admin_alter_client_quotas_reports_unsupported_per_entity(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    /* Two alterations. The first names two entity types, one of them the
     * built-in default entity (a NULL name); the second is a single ip entity.
     * The two-level parameter types need the inner arrays declared separately
     * so the outer array decays to `const char *const *const *`. */
    const char *e0_types[2] = {"user", "client-id"};
    const char *e0_names[2] = {"alice", NULL};
    const char *e1_types[1] = {"ip"};
    const char *e1_names[1] = {"10.0.0.1"};
    const char *const *const entity_types[2] = {e0_types, e1_types};
    const char *const *const entity_names[2] = {e0_names, e1_names};
    const int32_t entity_counts[2] = {2, 1};

    const char *k0[1] = {"producer_byte_rate"};
    const char *k1[2] = {"consumer_byte_rate", "request_percentage"};
    const char *const *const op_keys[2] = {k0, k1};
    const double v0[1] = {1024.0};
    const double v1[2] = {0.0, 50.0};
    const double *const op_values[2] = {v0, v1};
    /* The first op of row 1 has no value: Java's `Op(key, null)`, i.e.
     * remove. Its slot in `v1` holds 0.0, a legal quota value, which is why the
     * flag and not a sentinel carries the meaning.
     *
     * The entity counts (2, 1) and op counts (1, 2) differ per row on purpose,
     * so swapping the two count arrays is visible rather than a no-op. */
    const bool h0[1] = {true};
    const bool h1[2] = {false, true};
    const bool *const op_has_values[2] = {h0, h1};
    const int32_t op_counts[2] = {1, 2};

    kafka_admin_AlterClientQuotasResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_client_quotas(
        admin, entity_types, entity_names, entity_counts, op_keys, op_values, op_has_values,
        op_counts, 2, -1, false, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_AlterClientQuotasResult_count(result));

    /* Sorted by (entity type, name) pairs: the ["client-id"=default,
     * "user"=alice] entity sorts before ["ip"=10.0.0.1]. */
    const kafka_common_ClientQuotaEntity_t *e0 =
        kafka_admin_AlterClientQuotasResult_get_entity(result, 0);
    TEST_ASSERT_NOT_NULL(e0);
    TEST_ASSERT_EQUAL_INT32(2, kafka_common_ClientQuotaEntity_entry_count(e0));
    TEST_ASSERT_EQUAL_STRING("client-id", kafka_common_ClientQuotaEntity_get_entry_type(e0, 0));
    /* A null name at an in-range index is the built-in default entity, not an
     * absent entry and not the empty name. */
    TEST_ASSERT_NULL(kafka_common_ClientQuotaEntity_get_entry_name(e0, 0));
    TEST_ASSERT_EQUAL_STRING("user", kafka_common_ClientQuotaEntity_get_entry_type(e0, 1));
    TEST_ASSERT_EQUAL_STRING("alice", kafka_common_ClientQuotaEntity_get_entry_name(e0, 1));
    TEST_ASSERT_NULL(kafka_common_ClientQuotaEntity_get_entry_type(e0, 2));

    const kafka_common_ClientQuotaEntity_t *e1 =
        kafka_admin_AlterClientQuotasResult_get_entity(result, 1);
    TEST_ASSERT_NOT_NULL(e1);
    TEST_ASSERT_EQUAL_INT32(1, kafka_common_ClientQuotaEntity_entry_count(e1));
    TEST_ASSERT_EQUAL_STRING("ip", kafka_common_ClientQuotaEntity_get_entry_type(e1, 0));
    TEST_ASSERT_EQUAL_STRING("10.0.0.1", kafka_common_ClientQuotaEntity_get_entry_name(e1, 0));

    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_AlterClientQuotasResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implement yet", kafka_common_Error_message(e));
    }
    TEST_ASSERT_NULL(kafka_admin_AlterClientQuotasResult_get_entity(result, 2));
    TEST_ASSERT_NULL(kafka_admin_AlterClientQuotasResult_get_error(result, 2));
    kafka_admin_AlterClientQuotasResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_alter_client_quotas_rejects_duplicate_entities(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *dup_types[2] = {"user", "user"};
    const char *dup_names[2] = {"alice", "bob"};
    const char *const *const entity_types[1] = {dup_types};
    const char *const *const entity_names[1] = {dup_names};
    const int32_t entity_counts[1] = {2};

    kafka_admin_AlterClientQuotasResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_alter_client_quotas(
        admin, entity_types, entity_names, entity_counts, NULL, NULL, NULL, NULL, 1, -1, false,
        &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("quota alteration at index 0 repeats entity type `user`",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* The same entity in two alterations. Java accepts this -- it sends both
     * and only the future map collapses (KafkaAdminClient.java:4301-4313) --
     * but the C result is a flat array built from that map, so the caller
     * could not tell which of its two rows the surviving outcome describes.
     * Rejected at the exact row instead; see read_client_quota_alterations. */
    const char *types[1] = {"user"};
    const char *names[1] = {"alice"};
    const char *const *const two_types[2] = {types, types};
    const char *const *const two_names[2] = {names, names};
    const int32_t ones[2] = {1, 1};
    error = kafka_admin_AdminClient_alter_client_quotas(admin, two_types, two_names, ones, NULL,
                                                        NULL, NULL, NULL, 2, -1, false, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING(
        "quota alteration at index 1 repeats an entity already altered by an earlier entry",
        kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void on_alter_client_quotas(kafka_admin_AlterClientQuotasResult_t *result,
                                   kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_AlterClientQuotasResult_count(result);
        kafka_admin_AlterClientQuotasResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_alter_client_quotas_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *types[1] = {"user"};
    const char *names[1] = {"async-user"};
    const char *const *const entity_types[1] = {types};
    const char *const *const entity_names[1] = {names};
    const int32_t entity_counts[1] = {1};

    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_client_quotas_async(admin, entity_types, entity_names,
                                                      entity_counts, NULL, NULL, NULL, NULL, 1, -1,
                                                      true, on_alter_client_quotas, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, r.had_result);
    TEST_ASSERT_EQUAL_INT(0, r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_alter_client_quotas_async_null_handle(void) {
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_alter_client_quotas_async(NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                                      NULL, 0, -1, false, on_alter_client_quotas,
                                                      &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
}

// ---- ownership: a NULL out_result must not build (or leak) a handle ---------

static void test_mock_admin_b5a_null_out_result(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const int32_t rt[1] = {ACL_RESOURCE_TYPE_TOPIC};
    const char *names[1] = {"t"};
    const int32_t pt[1] = {ACL_PATTERN_TYPE_LITERAL};
    const char *principals[1] = {"User:a"};
    const char *hosts[1] = {"*"};
    const int32_t op[1] = {ACL_OPERATION_READ};
    const int32_t pm[1] = {ACL_PERMISSION_ALLOW};
    const char *types[1] = {"user"};
    const char *entity_names_inner[1] = {"alice"};
    const char *const *const entity_types[1] = {types};
    const char *const *const entity_names[1] = {entity_names_inner};
    const int32_t entity_counts[1] = {1};

    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_acls(admin, rt, names, pt, principals, hosts,
                                                         op, pm, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_delete_acls(admin, rt, names, pt, principals, hosts,
                                                         op, pm, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_client_quotas(
        admin, entity_types, entity_names, entity_counts, NULL, NULL, NULL, NULL, 1, -1, false,
        NULL));

    /* The two single-future RPCs return the mock's error whether or not the
     * caller wants the result, so they are checked for a non-null error
     * instead. Passing NULL must still not build a handle. */
    kafka_common_Error_t *error = kafka_admin_AdminClient_describe_acls(
        admin, ACL_RESOURCE_TYPE_ANY, NULL, ACL_PATTERN_TYPE_ANY, NULL, NULL, ACL_OPERATION_ANY,
        ACL_PERMISSION_ANY, -1, NULL);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);
    error = kafka_admin_AdminClient_describe_client_quotas(admin, NULL, NULL, NULL, 0, false, -1,
                                                           NULL);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

/* ==== B5b: SCRAM, delegation tokens and features ===========================
 *
 * Two of these eight RPCs are declined by Java's own `MockAdminClient`
 * (`describeUserScramCredentials` / `alterUserScramCredentials`,
 * MockAdminClient.java:1251-1259), so their success drains are unreachable
 * here and are covered by the Rust FFI unit tests against hand-built fixtures.
 * The other six *are* implemented by the mock, so they are exercised end to
 * end: a token is created, described, renewed and expired using the HMAC the
 * broker handed back, and the feature levels are seeded and then read and
 * updated.
 */

#define SCRAM_MECHANISM_UNKNOWN 0
#define SCRAM_MECHANISM_SHA_256 1
#define SCRAM_MECHANISM_SHA_512 2

#define UPGRADE_TYPE_UNKNOWN 0
#define UPGRADE_TYPE_UPGRADE 1
#define UPGRADE_TYPE_SAFE_DOWNGRADE 2
#define UPGRADE_TYPE_UNSAFE_DOWNGRADE 3

static void test_mock_admin_describe_user_scram_credentials_fails_the_whole_call(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *users[2] = {"alice", "bob"};

    /* One future for the whole response, so the mock's refusal is the call's
     * error rather than a per-user one. */
    kafka_admin_DescribeUserScramCredentialsResult_t *result = NULL;
    kafka_common_Error_t *error =
        kafka_admin_AdminClient_describe_user_scram_credentials(admin, users, 2, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* An empty user list means "every user" and reaches the same refusal. */
    error = kafka_admin_AdminClient_describe_user_scram_credentials(admin, NULL, 0, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_alter_user_scram_credentials_reports_unsupported_per_user(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    /* Deliberately mixed and ragged: an upsertion with an explicit salt, a
     * deletion, and an upsertion with no salt. The two password lengths and the
     * salt length all differ, so substituting one length array for another is
     * visible rather than a no-op. */
    const char *users[3] = {"alice", "bob", "carol"};
    const bool is_deletions[3] = {false, true, false};
    const int32_t mechanisms[3] = {SCRAM_MECHANISM_SHA_256, SCRAM_MECHANISM_SHA_512,
                                   SCRAM_MECHANISM_SHA_512};
    const int32_t iterations[3] = {4096, 0, 8192};
    const uint8_t alice_password[3] = {'p', 'w', '1'};
    const uint8_t carol_password[5] = {'p', 'w', '2', '3', '4'};
    const uint8_t *const passwords[3] = {alice_password, NULL, carol_password};
    const int32_t password_lens[3] = {3, 0, 5};
    const uint8_t alice_salt[2] = {0xaa, 0xbb};
    const uint8_t empty_salt[1] = {0};
    /* carol supplies an *explicitly empty* salt: has_salts[2] is true with a
     * length of 0, which is Java's four-argument constructor with a zero-length
     * array, not the salt-generating three-argument one. bob is a deletion and
     * has no salt at all. */
    const uint8_t *const salts[3] = {alice_salt, NULL, empty_salt};
    const int32_t salt_lens[3] = {2, 0, 0};
    const bool has_salts[3] = {true, false, true};

    kafka_admin_AlterUserScramCredentialsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_user_scram_credentials(
        admin, users, is_deletions, mechanisms, iterations, passwords, password_lens, salts,
        salt_lens, has_salts, 3, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_AlterUserScramCredentialsResult_count(result));

    /* Sorted by user name. */
    TEST_ASSERT_EQUAL_STRING("alice", kafka_admin_AlterUserScramCredentialsResult_get_user(result, 0));
    TEST_ASSERT_EQUAL_STRING("bob", kafka_admin_AlterUserScramCredentialsResult_get_user(result, 1));
    TEST_ASSERT_EQUAL_STRING("carol", kafka_admin_AlterUserScramCredentialsResult_get_user(result, 2));
    for (int32_t i = 0; i < 3; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_AlterUserScramCredentialsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
    }
    TEST_ASSERT_NULL(kafka_admin_AlterUserScramCredentialsResult_get_user(result, 3));
    TEST_ASSERT_NULL(kafka_admin_AlterUserScramCredentialsResult_get_error(result, 3));
    kafka_admin_AlterUserScramCredentialsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_alter_user_scram_credentials_rejects_bad_rows(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *with_null[2] = {"alice", NULL};
    const bool is_deletions[2] = {true, true};
    const int32_t mechanisms[2] = {SCRAM_MECHANISM_SHA_256, SCRAM_MECHANISM_SHA_256};

    kafka_admin_AlterUserScramCredentialsResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_alter_user_scram_credentials(
        admin, with_null, is_deletions, mechanisms, NULL, NULL, NULL, NULL, NULL, NULL, 2, -1,
        &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("scram alteration user at index 1 must not be null",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* An upsertion with an empty password is NOT rejected here: Java records
     * "Password must not be empty" against that user only
     * (KafkaAdminClient.java:4414-4416) and still sends every other user's
     * alteration, so failing the whole call would drop them. The row reaches
     * the mock, which refuses every user with "Not implemented yet". */
    const char *users[1] = {"alice"};
    const bool upsertion[1] = {false};
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_user_scram_credentials(
        admin, users, upsertion, mechanisms, NULL, NULL, NULL, NULL, NULL, NULL, 1, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_AlterUserScramCredentialsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("alice",
                             kafka_admin_AlterUserScramCredentialsResult_get_user(result, 0));
    TEST_ASSERT_EQUAL_STRING(
        "Not implemented yet",
        kafka_common_Error_message(
            kafka_admin_AlterUserScramCredentialsResult_get_error(result, 0)));
    kafka_admin_AlterUserScramCredentialsResult_destroy(result);
    result = NULL;

    /* A deletion in the same position needs no password at all, which is what
     * the explicit flag buys. */
    const bool deletion[1] = {true};
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_user_scram_credentials(
        admin, users, deletion, mechanisms, NULL, NULL, NULL, NULL, NULL, NULL, 1, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_AlterUserScramCredentialsResult_count(result));
    kafka_admin_AlterUserScramCredentialsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void on_describe_user_scram_credentials(kafka_admin_DescribeUserScramCredentialsResult_t *result,
                                               kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DescribeUserScramCredentialsResult_count(result);
        kafka_admin_DescribeUserScramCredentialsResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_alter_user_scram_credentials(kafka_admin_AlterUserScramCredentialsResult_t *result,
                                            kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_AlterUserScramCredentialsResult_count(result);
        kafka_admin_AlterUserScramCredentialsResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_scram_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *users[1] = {"alice"};

    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_user_scram_credentials_async(
        admin, users, 1, -1, on_describe_user_scram_credentials, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", r.message);

    const bool is_deletions[1] = {true};
    const int32_t mechanisms[1] = {SCRAM_MECHANISM_SHA_512};
    acl_async_result_t a = {0};
    atomic_init(&a.fired, 0);
    kafka_admin_AdminClient_alter_user_scram_credentials_async(
        admin, users, is_deletions, mechanisms, NULL, NULL, NULL, NULL, NULL, NULL, 1, -1,
        on_alter_user_scram_credentials, &a);
    TEST_ASSERT_TRUE(wait_for(&a.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, a.had_result);
    TEST_ASSERT_EQUAL_INT(0, a.had_error);
    TEST_ASSERT_EQUAL_INT32(1, a.count);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_scram_async_null_handle_and_marshaling_failure(void) {
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_user_scram_credentials_async(NULL, NULL, 0, -1,
                                                                  on_describe_user_scram_credentials, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);

    acl_async_result_t a = {0};
    atomic_init(&a.fired, 0);
    kafka_admin_AdminClient_alter_user_scram_credentials_async(NULL, NULL, NULL, NULL, NULL, NULL,
                                                               NULL, NULL, NULL, NULL, 0, -1,
                                                               on_alter_user_scram_credentials, &a);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&a.fired));
    TEST_ASSERT_EQUAL_INT(1, a.had_error);

    /* Marshaling failure fires the callback inline too, before the call
     * returns, so the flag is read without waiting. */
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *with_null[1] = {NULL};
    const bool is_deletions[1] = {true};
    const int32_t mechanisms[1] = {SCRAM_MECHANISM_SHA_256};
    acl_async_result_t m = {0};
    atomic_init(&m.fired, 0);
    kafka_admin_AdminClient_alter_user_scram_credentials_async(
        admin, with_null, is_deletions, mechanisms, NULL, NULL, NULL, NULL, NULL, NULL, 1, -1,
        on_alter_user_scram_credentials, &m);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&m.fired));
    TEST_ASSERT_EQUAL_INT(0, m.had_result);
    TEST_ASSERT_EQUAL_STRING("scram alteration user at index 0 must not be null", m.message);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_delegation_token_lifecycle(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    /* Two renewers with different names, so a transposition of the two arrays
     * or of the two rows is visible. The mock makes the *first* renewer the
     * owner, mirroring MockAdminClient.createDelegationToken. */
    const char *renewer_types[2] = {"User", "User"};
    const char *renewer_names[2] = {"owner-principal", "second-renewer"};

    kafka_admin_CreateDelegationTokenResult_t *created = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_delegation_token(
        admin, renewer_types, renewer_names, 2, NULL, NULL, 86400000, -1, &created));
    TEST_ASSERT_NOT_NULL(created);

    const kafka_common_DelegationToken_t *token =
        kafka_admin_CreateDelegationTokenResult_get_token(created);
    TEST_ASSERT_NOT_NULL(token);
    const kafka_common_TokenInformation_t *info = kafka_common_DelegationToken_token_info(token);
    TEST_ASSERT_NOT_NULL(info);
    TEST_ASSERT_EQUAL_INT32(2, kafka_common_TokenInformation_renewer_count(info));
    const kafka_common_KafkaPrincipal_t *owner = kafka_common_TokenInformation_owner(info);
    TEST_ASSERT_EQUAL_STRING("User", kafka_common_KafkaPrincipal_principal_type(owner));
    TEST_ASSERT_EQUAL_STRING("owner-principal", kafka_common_KafkaPrincipal_name(owner));
    TEST_ASSERT_EQUAL_STRING("second-renewer",
                             kafka_common_KafkaPrincipal_name(
                                 kafka_common_TokenInformation_get_renewer(info, 1)));
    TEST_ASSERT_EQUAL_INT64(86400000, kafka_common_TokenInformation_max_timestamp(info));
    /* Copy the token id out, not just the pointer: every getter on these
     * handles returns a borrow that dies with the result handle destroyed
     * below. Keeping the pointer reads freed memory -- and does so
     * intermittently, which is how this was caught. */
    const char *token_id_borrowed = kafka_common_TokenInformation_token_id(info);
    TEST_ASSERT_NOT_NULL(token_id_borrowed);
    char token_id[128];
    TEST_ASSERT_TRUE(strlen(token_id_borrowed) < sizeof(token_id));
    snprintf(token_id, sizeof(token_id), "%s", token_id_borrowed);

    /* The HMAC is borrowed from the same handle and needs the same treatment;
     * the mock uses the token id's bytes as the HMAC. */
    int32_t hmac_len = 0;
    const uint8_t *hmac_borrowed = kafka_common_DelegationToken_hmac(token, &hmac_len);
    TEST_ASSERT_TRUE(hmac_len > 0);
    uint8_t hmac[128];
    TEST_ASSERT_TRUE((size_t)hmac_len <= sizeof(hmac));
    memcpy(hmac, hmac_borrowed, (size_t)hmac_len);
    /* The base64 form is the same bytes, so it must be non-empty too. */
    TEST_ASSERT_TRUE(strlen(kafka_common_DelegationToken_hmac_as_base64_string(token)) > 0);
    kafka_admin_CreateDelegationTokenResult_destroy(created);

    /* describeDelegationToken with no filter sees it. */
    kafka_admin_DescribeDelegationTokenResult_t *described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_delegation_token(admin, false, NULL, NULL, 0,
                                                                       -1, &described));
    TEST_ASSERT_NOT_NULL(described);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeDelegationTokenResult_count(described));
    const kafka_common_DelegationToken_t *listed =
        kafka_admin_DescribeDelegationTokenResult_get_token(described, 0);
    TEST_ASSERT_EQUAL_STRING(
        token_id, kafka_common_TokenInformation_token_id(kafka_common_DelegationToken_token_info(listed)));
    TEST_ASSERT_NULL(kafka_admin_DescribeDelegationTokenResult_get_token(described, 1));
    kafka_admin_DescribeDelegationTokenResult_destroy(described);

    /* Renewing moves the expiry to the requested period. */
    kafka_admin_RenewDelegationTokenResult_t *renewed = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_renew_delegation_token(admin, hmac, hmac_len, 4242, -1,
                                                                    &renewed));
    TEST_ASSERT_NOT_NULL(renewed);
    TEST_ASSERT_EQUAL_INT64(4242, kafka_admin_RenewDelegationTokenResult_expiry_timestamp(renewed));
    kafka_admin_RenewDelegationTokenResult_destroy(renewed);

    /* A wrong HMAC is DELEGATION_TOKEN_NOT_FOUND, not a silent no-op. */
    const uint8_t bogus[3] = {0x01, 0x02, 0x03};
    kafka_common_Error_t *error =
        kafka_admin_AdminClient_renew_delegation_token(admin, bogus, 3, 1, -1, &renewed);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);

    /* Expiring with the -1 sentinel removes it immediately. */
    kafka_admin_ExpireDelegationTokenResult_t *expired = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_expire_delegation_token(admin, hmac, hmac_len, -1, -1, &expired));
    TEST_ASSERT_NOT_NULL(expired);
    TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(expired));
    kafka_admin_ExpireDelegationTokenResult_destroy(expired);

    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_delegation_token(admin, false, NULL, NULL, 0,
                                                                       -1, &described));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeDelegationTokenResult_count(described));
    kafka_admin_DescribeDelegationTokenResult_destroy(described);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_delegation_token_owner_filter(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *alice_types[1] = {"User"};
    const char *alice_names[1] = {"alice"};
    const char *bob_types[1] = {"User"};
    const char *bob_names[1] = {"bob"};

    kafka_admin_CreateDelegationTokenResult_t *created = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_delegation_token(admin, alice_types, alice_names,
                                                                     1, NULL, NULL, -1, -1, &created));
    kafka_admin_CreateDelegationTokenResult_destroy(created);
    created = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_delegation_token(admin, bob_types, bob_names, 1,
                                                                     NULL, NULL, -1, -1, &created));
    kafka_admin_CreateDelegationTokenResult_destroy(created);

    /* No filter describes both. */
    kafka_admin_DescribeDelegationTokenResult_t *result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_delegation_token(admin, false, NULL, NULL, 0, -1, &result));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DescribeDelegationTokenResult_count(result));
    kafka_admin_DescribeDelegationTokenResult_destroy(result);

    /* A filter naming only alice describes one -- so the flag and the filter
     * are not the same knob, and passing the owner arrays without setting the
     * flag would be a different request. */
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_delegation_token(admin, true, alice_types,
                                                                       alice_names, 1, -1, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeDelegationTokenResult_count(result));
    const kafka_common_TokenInformation_t *info = kafka_common_DelegationToken_token_info(
        kafka_admin_DescribeDelegationTokenResult_get_token(result, 0));
    TEST_ASSERT_EQUAL_STRING("alice",
                             kafka_common_KafkaPrincipal_name(kafka_common_TokenInformation_owner(info)));
    kafka_admin_DescribeDelegationTokenResult_destroy(result);

    /* The same owner arrays with the flag off describe everything again. */
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_delegation_token(admin, false, alice_types,
                                                                       alice_names, 1, -1, &result));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DescribeDelegationTokenResult_count(result));
    kafka_admin_DescribeDelegationTokenResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_create_delegation_token_rejects_bad_input(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    /* A NULL renewer name is rejected during marshaling, as Java's
     * KafkaPrincipal constructor rejects a null name. */
    const char *types[2] = {"User", "User"};
    const char *names[2] = {"alice", NULL};
    kafka_admin_CreateDelegationTokenResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_create_delegation_token(
        admin, types, names, 2, NULL, NULL, -1, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("renewer principal name at index 1 must not be null",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* A non-User renewer reaches the mock, which refuses it. */
    const char *group_types[1] = {"Group"};
    const char *group_names[1] = {"admins"};
    error = kafka_admin_AdminClient_create_delegation_token(admin, group_types, group_names, 1, NULL,
                                                            NULL, -1, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);

    /* No renewer at all: MockAdminClient makes `options.renewers().get(0)` the
     * owner (MockAdminClient.java:652), so Java throws a catchable
     * IndexOutOfBoundsException here. In Rust an index panic would unwind out of
     * this extern "C" call and abort the process, so the mock completes the
     * future exceptionally instead. This is the *documented default* -- an empty
     * renewer list is legal against a real broker -- which is why it needs a
     * test of its own. */
    result = NULL;
    error = kafka_admin_AdminClient_create_delegation_token(admin, NULL, NULL, 0, NULL, NULL, -1, -1,
                                                            &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("createDelegationToken requires at least one renewer: MockAdminClient "
                             "makes the first renewer the owner",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_features_reports_seeded_levels(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    /* All twelve numbers distinct, so transposing any two of the three level
     * arrays is visible. */
    const char *features[3] = {"metadata.version", "transaction.version", "group.version"};
    const int16_t levels[3] = {17, 2, 1};
    const int16_t min_levels[3] = {14, 1, 0};
    const int16_t max_levels[3] = {21, 3, 4};
    TEST_ASSERT_NULL(
        kafka_admin_MockAdminClient_set_feature_levels(admin, features, levels, min_levels, max_levels, 3));

    kafka_admin_DescribeFeaturesResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_features(admin, false, 0, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_DescribeFeaturesResult_finalized_count(result));
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_DescribeFeaturesResult_supported_count(result));

    /* Sorted by name: group.version, metadata.version, transaction.version. A
     * finalized range is [level, level]; a supported one is [min, max]. */
    TEST_ASSERT_EQUAL_STRING("group.version",
                             kafka_admin_DescribeFeaturesResult_get_finalized_feature(result, 0));
    TEST_ASSERT_EQUAL_INT16(1, kafka_admin_DescribeFeaturesResult_get_finalized_min_version_level(result, 0));
    TEST_ASSERT_EQUAL_INT16(1, kafka_admin_DescribeFeaturesResult_get_finalized_max_version_level(result, 0));
    TEST_ASSERT_EQUAL_STRING("metadata.version",
                             kafka_admin_DescribeFeaturesResult_get_finalized_feature(result, 1));
    TEST_ASSERT_EQUAL_INT16(17, kafka_admin_DescribeFeaturesResult_get_finalized_max_version_level(result, 1));

    TEST_ASSERT_EQUAL_STRING("group.version",
                             kafka_admin_DescribeFeaturesResult_get_supported_feature(result, 0));
    TEST_ASSERT_EQUAL_INT16(0, kafka_admin_DescribeFeaturesResult_get_supported_min_version(result, 0));
    TEST_ASSERT_EQUAL_INT16(4, kafka_admin_DescribeFeaturesResult_get_supported_max_version(result, 0));
    TEST_ASSERT_EQUAL_STRING("metadata.version",
                             kafka_admin_DescribeFeaturesResult_get_supported_feature(result, 1));
    TEST_ASSERT_EQUAL_INT16(14, kafka_admin_DescribeFeaturesResult_get_supported_min_version(result, 1));
    TEST_ASSERT_EQUAL_INT16(21, kafka_admin_DescribeFeaturesResult_get_supported_max_version(result, 1));

    /* Out of range is -1, not 0: 0 is a legal version level. */
    TEST_ASSERT_EQUAL_INT16(-1, kafka_admin_DescribeFeaturesResult_get_supported_min_version(result, 3));
    TEST_ASSERT_NULL(kafka_admin_DescribeFeaturesResult_get_finalized_feature(result, 3));

    int64_t epoch = 0;
    TEST_ASSERT_TRUE(kafka_admin_DescribeFeaturesResult_finalized_features_epoch(result, &epoch));
    TEST_ASSERT_EQUAL_INT64(123, epoch);
    kafka_admin_DescribeFeaturesResult_destroy(result);

    /* Pinning a node id is a different request but the same answer from the
     * mock, which ignores its options. */
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_features(admin, true, 0, -1, &result));
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_DescribeFeaturesResult_finalized_count(result));
    kafka_admin_DescribeFeaturesResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_update_features_applies_and_validates(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *seed[1] = {"metadata.version"};
    const int16_t levels[1] = {17};
    const int16_t min_levels[1] = {14};
    const int16_t max_levels[1] = {21};
    TEST_ASSERT_NULL(
        kafka_admin_MockAdminClient_set_feature_levels(admin, seed, levels, min_levels, max_levels, 1));

    /* validate_only leaves the level alone. */
    const char *features[1] = {"metadata.version"};
    const int16_t targets[1] = {19};
    const int32_t upgrade[1] = {UPGRADE_TYPE_UPGRADE};
    kafka_admin_UpdateFeaturesResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_update_features(admin, features, targets, upgrade, 1, -1,
                                                             true, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_UpdateFeaturesResult_count(result));
    TEST_ASSERT_EQUAL_STRING("metadata.version", kafka_admin_UpdateFeaturesResult_get_feature(result, 0));
    TEST_ASSERT_NULL(kafka_admin_UpdateFeaturesResult_get_error(result, 0));
    kafka_admin_UpdateFeaturesResult_destroy(result);

    kafka_admin_DescribeFeaturesResult_t *described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_features(admin, false, 0, -1, &described));
    TEST_ASSERT_EQUAL_INT16(
        17, kafka_admin_DescribeFeaturesResult_get_finalized_max_version_level(described, 0));
    kafka_admin_DescribeFeaturesResult_destroy(described);

    /* Applying for real moves it. */
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_update_features(admin, features, targets, upgrade, 1, -1,
                                                             false, &result));
    TEST_ASSERT_NULL(kafka_admin_UpdateFeaturesResult_get_error(result, 0));
    kafka_admin_UpdateFeaturesResult_destroy(result);

    described = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_features(admin, false, 0, -1, &described));
    TEST_ASSERT_EQUAL_INT16(
        19, kafka_admin_DescribeFeaturesResult_get_finalized_max_version_level(described, 0));
    kafka_admin_DescribeFeaturesResult_destroy(described);

    /* Above the seeded maximum is a per-feature error, not a call error. */
    const int16_t too_high[1] = {99};
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_update_features(admin, features, too_high, upgrade, 1, -1,
                                                             false, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_UpdateFeaturesResult_count(result));
    const kafka_common_Error_t *e = kafka_admin_UpdateFeaturesResult_get_error(result, 0);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_STRING("Invalid update version 99 for feature metadata.version. Can't upgrade above 21",
                             kafka_common_Error_message(e));
    kafka_admin_UpdateFeaturesResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_update_features_rejects_bad_input(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *duplicated[2] = {"metadata.version", "metadata.version"};
    const int16_t targets[2] = {17, 18};
    const int32_t upgrade[2] = {UPGRADE_TYPE_UPGRADE, UPGRADE_TYPE_UPGRADE};

    kafka_admin_UpdateFeaturesResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_update_features(
        admin, duplicated, targets, upgrade, 2, -1, false, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("feature update at index 1 repeats feature `metadata.version`",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* FeatureUpdate's own constructor rejects level 0 with UPGRADE. */
    const char *features[1] = {"metadata.version"};
    const int16_t zero[1] = {0};
    error = kafka_admin_AdminClient_update_features(admin, features, zero, upgrade, 1, -1, false,
                                                    &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("feature update at index 0: The upgradeType flag should be set to "
                             "SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:0 is < 1.",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* An UNKNOWN upgrade type marshals fine and is refused by the broker side;
     * on the mock that is a per-feature error. */
    const int32_t unknown[1] = {UPGRADE_TYPE_UNKNOWN};
    const int16_t one[1] = {1};
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_update_features(admin, features, one, unknown, 1, -1, false, &result));
    TEST_ASSERT_NOT_NULL(kafka_admin_UpdateFeaturesResult_get_error(result, 0));
    kafka_admin_UpdateFeaturesResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void on_create_delegation_token(kafka_admin_CreateDelegationTokenResult_t *result,
                                       kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        const kafka_common_TokenInformation_t *info =
            kafka_common_DelegationToken_token_info(kafka_admin_CreateDelegationTokenResult_get_token(result));
        r->count = kafka_common_TokenInformation_renewer_count(info);
        kafka_admin_CreateDelegationTokenResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_describe_delegation_token(kafka_admin_DescribeDelegationTokenResult_t *result,
                                         kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DescribeDelegationTokenResult_count(result);
        kafka_admin_DescribeDelegationTokenResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_renew_delegation_token(kafka_admin_RenewDelegationTokenResult_t *result,
                                      kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = (int32_t)kafka_admin_RenewDelegationTokenResult_expiry_timestamp(result);
        kafka_admin_RenewDelegationTokenResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_expire_delegation_token(kafka_admin_ExpireDelegationTokenResult_t *result,
                                       kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = (int32_t)kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(result);
        kafka_admin_ExpireDelegationTokenResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_describe_features(kafka_admin_DescribeFeaturesResult_t *result,
                                 kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DescribeFeaturesResult_finalized_count(result);
        kafka_admin_DescribeFeaturesResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_update_features(kafka_admin_UpdateFeaturesResult_t *result,
                               kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_UpdateFeaturesResult_count(result);
        kafka_admin_UpdateFeaturesResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_b5b_token_and_feature_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *types[2] = {"User", "User"};
    const char *names[2] = {"alice", "bob"};

    acl_async_result_t c = {0};
    atomic_init(&c.fired, 0);
    kafka_admin_AdminClient_create_delegation_token_async(admin, types, names, 2, NULL, NULL, -1, -1,
                                                          on_create_delegation_token, &c);
    TEST_ASSERT_TRUE(wait_for(&c.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, c.had_result);
    TEST_ASSERT_EQUAL_INT(0, c.had_error);
    TEST_ASSERT_EQUAL_INT32(2, c.count);

    acl_async_result_t d = {0};
    atomic_init(&d.fired, 0);
    kafka_admin_AdminClient_describe_delegation_token_async(admin, false, NULL, NULL, 0, -1,
                                                            on_describe_delegation_token, &d);
    TEST_ASSERT_TRUE(wait_for(&d.fired, 1));
    TEST_ASSERT_EQUAL_INT32(1, d.count);

    /* Renewing an unknown HMAC is the call's error, so the async error path is
     * exercised as well as the success one. */
    const uint8_t bogus[2] = {0x01, 0x02};
    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_renew_delegation_token_async(admin, bogus, 2, 1, -1,
                                                         on_renew_delegation_token, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(0, r.had_result);
    TEST_ASSERT_EQUAL_INT(1, r.had_error);

    acl_async_result_t x = {0};
    atomic_init(&x.fired, 0);
    kafka_admin_AdminClient_expire_delegation_token_async(admin, bogus, 2, -1, -1,
                                                          on_expire_delegation_token, &x);
    TEST_ASSERT_TRUE(wait_for(&x.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, x.had_error);

    const char *seed[1] = {"metadata.version"};
    const int16_t levels[1] = {17};
    const int16_t min_levels[1] = {14};
    const int16_t max_levels[1] = {21};
    TEST_ASSERT_NULL(
        kafka_admin_MockAdminClient_set_feature_levels(admin, seed, levels, min_levels, max_levels, 1));

    acl_async_result_t f = {0};
    atomic_init(&f.fired, 0);
    kafka_admin_AdminClient_describe_features_async(admin, false, 0, -1, on_describe_features, &f);
    TEST_ASSERT_TRUE(wait_for(&f.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, f.had_result);
    TEST_ASSERT_EQUAL_INT32(1, f.count);

    const int16_t targets[1] = {19};
    const int32_t upgrade[1] = {UPGRADE_TYPE_UPGRADE};
    acl_async_result_t u = {0};
    atomic_init(&u.fired, 0);
    kafka_admin_AdminClient_update_features_async(admin, seed, targets, upgrade, 1, -1, false,
                                                  on_update_features, &u);
    TEST_ASSERT_TRUE(wait_for(&u.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, u.had_result);
    TEST_ASSERT_EQUAL_INT32(1, u.count);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_b5b_async_null_handle(void) {
    acl_async_result_t c = {0};
    atomic_init(&c.fired, 0);
    kafka_admin_AdminClient_create_delegation_token_async(NULL, NULL, NULL, 0, NULL, NULL, -1, -1,
                                                          on_create_delegation_token, &c);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&c.fired));
    TEST_ASSERT_EQUAL_INT(1, c.had_error);

    acl_async_result_t d = {0};
    atomic_init(&d.fired, 0);
    kafka_admin_AdminClient_describe_delegation_token_async(NULL, false, NULL, NULL, 0, -1,
                                                            on_describe_delegation_token, &d);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&d.fired));
    TEST_ASSERT_EQUAL_INT(1, d.had_error);

    acl_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_renew_delegation_token_async(NULL, NULL, 0, -1, -1,
                                                         on_renew_delegation_token, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_EQUAL_INT(1, r.had_error);

    acl_async_result_t x = {0};
    atomic_init(&x.fired, 0);
    kafka_admin_AdminClient_expire_delegation_token_async(NULL, NULL, 0, -1, -1,
                                                          on_expire_delegation_token, &x);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&x.fired));
    TEST_ASSERT_EQUAL_INT(1, x.had_error);

    acl_async_result_t f = {0};
    atomic_init(&f.fired, 0);
    kafka_admin_AdminClient_describe_features_async(NULL, false, 0, -1, on_describe_features, &f);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&f.fired));
    TEST_ASSERT_EQUAL_INT(1, f.had_error);

    acl_async_result_t u = {0};
    atomic_init(&u.fired, 0);
    kafka_admin_AdminClient_update_features_async(NULL, NULL, NULL, NULL, 0, -1, false,
                                                  on_update_features, &u);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&u.fired));
    TEST_ASSERT_EQUAL_INT(1, u.had_error);
}

static void test_mock_admin_b5b_null_out_result(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *users[1] = {"alice"};
    const bool is_deletions[1] = {true};
    const int32_t mechanisms[1] = {SCRAM_MECHANISM_SHA_256};
    const char *types[1] = {"User"};
    const char *names[1] = {"alice"};
    const char *features[1] = {"metadata.version"};
    const int16_t targets[1] = {1};
    const int32_t upgrade[1] = {UPGRADE_TYPE_UPGRADE};

    /* Per-key RPCs succeed with a NULL out_result and must not build a handle. */
    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_user_scram_credentials(
        admin, users, is_deletions, mechanisms, NULL, NULL, NULL, NULL, NULL, NULL, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_update_features(admin, features, targets, upgrade, 1, -1,
                                                             true, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_delegation_token(admin, types, names, 1, NULL,
                                                                     NULL, -1, -1, NULL));
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_delegation_token(admin, false, NULL, NULL, 0, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_features(admin, false, 0, -1, NULL));

    /* The refused single-future RPC still returns its error. */
    kafka_common_Error_t *error =
        kafka_admin_AdminClient_describe_user_scram_credentials(admin, users, 1, -1, NULL);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);

    /* Renew/expire against a token that does not exist likewise. */
    const uint8_t bogus[1] = {0x01};
    error = kafka_admin_AdminClient_renew_delegation_token(admin, bogus, 1, -1, -1, NULL);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);
    error = kafka_admin_AdminClient_expire_delegation_token(admin, bogus, 1, -1, -1, NULL);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_set_feature_levels_rejects_non_mock(void) {
    /* Mock-only configuration must refuse a real client handle, as the other
     * `MockAdminClient_*` setters do. */
    kafka_admin_AdminClientProperties_t *props = kafka_admin_AdminClientProperties_new();
    kafka_admin_AdminClientProperties_put(props, "bootstrap.servers", "localhost:9092");
    kafka_common_Error_t *error = NULL;
    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, &error);
    kafka_admin_AdminClientProperties_destroy(props);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NOT_NULL(admin);

    const char *features[1] = {"metadata.version"};
    const int16_t levels[1] = {1};
    error = kafka_admin_MockAdminClient_set_feature_levels(admin, features, levels, levels, levels, 1);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("this operation is only supported on a MockAdminClient",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_close(admin, 1000);
    kafka_admin_AdminClient_destroy(admin);
}

/* ---------------------------------------------------------------------------
 * B6 -- producers and transactions
 *
 * Java's MockAdminClient throws `UnsupportedOperationException("Not implemented
 * yet")` for all six (MockAdminClient.java:1368-1395), so what these assert is
 * the per-key *shape*: which keys came back, that each carries the mock's error
 * verbatim, and that a value accessor on a failed row reports Java's own absent
 * sentinel. The success side of every value accessor is covered by the Rust
 * unit tests in src/ffi/admin.rs, which build the outcome maps directly.
 * ------------------------------------------------------------------------- */

/* Returns the index of `topic`/`partition` in a describeProducers result, or -1. */
static int32_t find_producer_partition(const kafka_admin_DescribeProducersResult_t *result,
                                       const char *topic, int32_t partition) {
    int32_t n = kafka_admin_DescribeProducersResult_count(result);
    for (int32_t i = 0; i < n; i++) {
        const char *t = kafka_admin_DescribeProducersResult_get_topic(result, i);
        if (t != NULL && strcmp(t, topic) == 0 &&
            kafka_admin_DescribeProducersResult_get_partition(result, i) == partition) {
            return i;
        }
    }
    return -1;
}

static void test_mock_admin_describe_producers_reports_unsupported_per_partition(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    /* Deliberately ragged: two partitions of one topic and one of another, so a
     * topic/partition column transposition changes the key set. */
    const char *topics[3] = {"alpha", "alpha", "beta"};
    const int32_t partitions[3] = {0, 4, 2};

    kafka_admin_DescribeProducersResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_producers(admin, topics, partitions, 3, true, 1,
                                                                -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_DescribeProducersResult_count(result));
    TEST_ASSERT_NOT_EQUAL(-1, find_producer_partition(result, "alpha", 0));
    TEST_ASSERT_NOT_EQUAL(-1, find_producer_partition(result, "alpha", 4));
    int32_t beta = find_producer_partition(result, "beta", 2);
    TEST_ASSERT_NOT_EQUAL(-1, beta);
    TEST_ASSERT_EQUAL(-1, find_producer_partition(result, "beta", 0));

    for (int32_t i = 0; i < 3; i++) {
        const kafka_common_Error_t *e = kafka_admin_DescribeProducersResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
        /* A failed partition has no producers, and its Optionals are absent. */
        TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeProducersResult_get_producer_count(result, i));
        int64_t offset = 7;
        TEST_ASSERT_FALSE(kafka_admin_DescribeProducersResult_get_current_transaction_start_offset(
            result, i, 0, &offset));
        TEST_ASSERT_EQUAL_INT64(7, offset); /* untouched when absent */
    }
    TEST_ASSERT_NULL(kafka_admin_DescribeProducersResult_get_topic(result, 3));
    TEST_ASSERT_EQUAL_INT32(-1, kafka_admin_DescribeProducersResult_get_partition(result, 3));
    kafka_admin_DescribeProducersResult_destroy(result);

    /* A duplicate partition collapses to one row, as Java's Map-keyed result
     * does. */
    const char *dup_topics[2] = {"alpha", "alpha"};
    const int32_t dup_partitions[2] = {0, 0};
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_producers(admin, dup_topics, dup_partitions, 2,
                                                                false, 0, -1, &result));
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_DescribeProducersResult_count(result));
    kafka_admin_DescribeProducersResult_destroy(result);

    /* No partitions requested: nothing to join, so an empty success. */
    result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_producers(admin, NULL, NULL, 0, false, 0, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_DescribeProducersResult_count(result));
    kafka_admin_DescribeProducersResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_describe_transactions_reports_unsupported_per_id(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    const char *ids[2] = {"txn-b", "txn-a"};
    kafka_admin_DescribeTransactionsResult_t *result = NULL;
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_describe_transactions(admin, ids, 2, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DescribeTransactionsResult_count(result));
    /* Rows are sorted by transactional id, not by request order. */
    TEST_ASSERT_EQUAL_STRING(
        "txn-a", kafka_admin_DescribeTransactionsResult_get_transactional_id(result, 0));
    TEST_ASSERT_EQUAL_STRING(
        "txn-b", kafka_admin_DescribeTransactionsResult_get_transactional_id(result, 1));
    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e =
            kafka_admin_DescribeTransactionsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
        /* A failed row reports Java's own fallback state name and the absent
         * scalars, never 0 -- 0 is a legal producer id and coordinator id. */
        TEST_ASSERT_EQUAL_STRING("Unknown", kafka_admin_DescribeTransactionsResult_get_state(result, i));
        TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_DescribeTransactionsResult_get_producer_id(result, i));
        TEST_ASSERT_EQUAL_INT32(-1,
                                kafka_admin_DescribeTransactionsResult_get_coordinator_id(result, i));
        TEST_ASSERT_EQUAL_INT32(
            0, kafka_admin_DescribeTransactionsResult_get_topic_partition_count(result, i));
        int64_t start = 5;
        TEST_ASSERT_FALSE(kafka_admin_DescribeTransactionsResult_get_transaction_start_time_ms(
            result, i, &start));
        TEST_ASSERT_EQUAL_INT64(5, start);
    }
    TEST_ASSERT_NULL(kafka_admin_DescribeTransactionsResult_get_transactional_id(result, 2));
    kafka_admin_DescribeTransactionsResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_fence_producers_reports_unsupported_per_id(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    const char *ids[2] = {"txn-y", "txn-x"};
    kafka_admin_FenceProducersResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_fence_producers(admin, ids, 2, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_FenceProducersResult_count(result));
    TEST_ASSERT_EQUAL_STRING("txn-x", kafka_admin_FenceProducersResult_get_transactional_id(result, 0));
    TEST_ASSERT_EQUAL_STRING("txn-y", kafka_admin_FenceProducersResult_get_transactional_id(result, 1));
    for (int32_t i = 0; i < 2; i++) {
        const kafka_common_Error_t *e = kafka_admin_FenceProducersResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(e));
        /* ProducerIdAndEpoch.NONE, not 0. */
        TEST_ASSERT_EQUAL_INT64(-1, kafka_admin_FenceProducersResult_get_producer_id(result, i));
        TEST_ASSERT_EQUAL_INT16(-1, kafka_admin_FenceProducersResult_get_epoch_id(result, i));
    }
    TEST_ASSERT_NULL(kafka_admin_FenceProducersResult_get_transactional_id(result, 2));
    kafka_admin_FenceProducersResult_destroy(result);

    /* An empty batch has nothing to join, so it resolves without a broker. */
    result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_fence_producers(admin, NULL, 0, -1, &result));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_FenceProducersResult_count(result));
    kafka_admin_FenceProducersResult_destroy(result);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_list_transactions_fails_the_whole_call(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    /* Java's mock fails the top-level broker-discovery future, and that is the
     * one listTransactions failure mode that is the *call's* error rather than a
     * per-broker row. */
    const char *states[2] = {"Ongoing", "PrepareAbort"};
    const int64_t producer_ids[3] = {11, 22, 33};
    kafka_admin_ListTransactionsResult_t *result = NULL;
    kafka_common_Error_t *error = kafka_admin_AdminClient_list_transactions(
        admin, states, 2, producer_ids, 3, 60000, "txn-.*", -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* Same with every filter left unset -- a NULL pattern must not be read. */
    error = kafka_admin_AdminClient_list_transactions(admin, NULL, 0, NULL, 0, -1, NULL, -1, &result);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(result);
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_abort_and_terminate_transaction_report_unsupported(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    /* Neither RPC has a result handle: Java's AbortTransactionResult exposes
     * only all(), and TerminateTransactionResult only result(), so success is a
     * null return and there is nothing to free. */
    kafka_common_Error_t *error =
        kafka_admin_AdminClient_abort_transaction(admin, "txn-topic", 3, 91234, 7, 42, -1);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* A NULL topic is a marshaling failure, before any submit. */
    error = kafka_admin_AdminClient_abort_transaction(admin, NULL, 0, 1, 1, 1, -1);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("abort transaction topic must not be null",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    /* 65537 truncates to 1 under a bare cast, which is a legal epoch, so an
     * out-of-range producer epoch must be rejected rather than narrowed. */
    error = kafka_admin_AdminClient_abort_transaction(admin, "txn-topic", 0, 1, 65537, 1, -1);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("producer epoch 65537 does not fit in a 16-bit epoch",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    error = kafka_admin_AdminClient_force_terminate_transaction(admin, "txn-a", -1);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    error = kafka_admin_AdminClient_force_terminate_transaction(admin, NULL, -1);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("transactional id must not be null",
                             kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

static void on_describe_producers(kafka_admin_DescribeProducersResult_t *result,
                                  kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DescribeProducersResult_count(result);
        kafka_admin_DescribeProducersResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_describe_transactions(kafka_admin_DescribeTransactionsResult_t *result,
                                     kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_DescribeTransactionsResult_count(result);
        kafka_admin_DescribeTransactionsResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_fence_producers(kafka_admin_FenceProducersResult_t *result,
                               kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_FenceProducersResult_count(result);
        kafka_admin_FenceProducersResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void on_list_transactions(kafka_admin_ListTransactionsResult_t *result,
                                 kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = result != NULL;
    if (result != NULL) {
        r->count = kafka_admin_ListTransactionsResult_count(result);
        kafka_admin_ListTransactionsResult_destroy(result);
    }
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

/* The two void-result RPCs share the result-less callback shape, so one handler
 * serves both. */
static void on_void_transaction_op(kafka_common_Error_t *error, void *user_data) {
    acl_async_result_t *r = (acl_async_result_t *)user_data;
    r->had_result = error == NULL;
    record_async_error(r, error);
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_b6_async_fires_once(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    const char *topics[2] = {"alpha", "beta"};
    const int32_t partitions[2] = {0, 1};
    acl_async_result_t p = {0};
    atomic_init(&p.fired, 0);
    kafka_admin_AdminClient_describe_producers_async(admin, topics, partitions, 2, false, 0, -1,
                                                    on_describe_producers, &p);
    TEST_ASSERT_TRUE(wait_for(&p.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, p.had_result);
    TEST_ASSERT_EQUAL_INT32(2, p.count);

    const char *ids[2] = {"txn-a", "txn-b"};
    acl_async_result_t d = {0};
    atomic_init(&d.fired, 0);
    kafka_admin_AdminClient_describe_transactions_async(admin, ids, 2, -1, on_describe_transactions,
                                                       &d);
    TEST_ASSERT_TRUE(wait_for(&d.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, d.had_result);
    TEST_ASSERT_EQUAL_INT32(2, d.count);

    acl_async_result_t f = {0};
    atomic_init(&f.fired, 0);
    kafka_admin_AdminClient_fence_producers_async(admin, ids, 2, -1, on_fence_producers, &f);
    TEST_ASSERT_TRUE(wait_for(&f.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, f.had_result);
    TEST_ASSERT_EQUAL_INT32(2, f.count);

    /* listTransactions fails wholesale on the mock, so the callback gets the
     * error rather than a result. */
    acl_async_result_t l = {0};
    atomic_init(&l.fired, 0);
    kafka_admin_AdminClient_list_transactions_async(admin, NULL, 0, NULL, 0, -1, NULL, -1,
                                                    on_list_transactions, &l);
    TEST_ASSERT_TRUE(wait_for(&l.fired, 1));
    TEST_ASSERT_EQUAL_INT(0, l.had_result);
    TEST_ASSERT_EQUAL_INT(1, l.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", l.message);

    acl_async_result_t a = {0};
    atomic_init(&a.fired, 0);
    kafka_admin_AdminClient_abort_transaction_async(admin, "txn-topic", 3, 91234, 7, 42, -1,
                                                   on_void_transaction_op, &a);
    TEST_ASSERT_TRUE(wait_for(&a.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, a.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", a.message);

    acl_async_result_t t = {0};
    atomic_init(&t.fired, 0);
    kafka_admin_AdminClient_force_terminate_transaction_async(admin, "txn-a", -1,
                                                             on_void_transaction_op, &t);
    TEST_ASSERT_TRUE(wait_for(&t.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, t.had_error);
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", t.message);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_b6_async_null_handle_and_marshaling_failure(void) {
    /* A NULL handle still honours the callback obligation, inline. */
    acl_async_result_t p = {0};
    atomic_init(&p.fired, 0);
    kafka_admin_AdminClient_describe_producers_async(NULL, NULL, NULL, 0, false, 0, -1,
                                                    on_describe_producers, &p);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&p.fired));
    TEST_ASSERT_EQUAL_INT(1, p.had_error);

    acl_async_result_t d = {0};
    atomic_init(&d.fired, 0);
    kafka_admin_AdminClient_describe_transactions_async(NULL, NULL, 0, -1, on_describe_transactions,
                                                       &d);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&d.fired));
    TEST_ASSERT_EQUAL_INT(1, d.had_error);

    acl_async_result_t f = {0};
    atomic_init(&f.fired, 0);
    kafka_admin_AdminClient_fence_producers_async(NULL, NULL, 0, -1, on_fence_producers, &f);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&f.fired));
    TEST_ASSERT_EQUAL_INT(1, f.had_error);

    acl_async_result_t l = {0};
    atomic_init(&l.fired, 0);
    kafka_admin_AdminClient_list_transactions_async(NULL, NULL, 0, NULL, 0, -1, NULL, -1,
                                                    on_list_transactions, &l);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&l.fired));
    TEST_ASSERT_EQUAL_INT(1, l.had_error);

    acl_async_result_t a = {0};
    atomic_init(&a.fired, 0);
    kafka_admin_AdminClient_abort_transaction_async(NULL, "t", 0, 1, 1, 1, -1, on_void_transaction_op,
                                                   &a);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&a.fired));
    TEST_ASSERT_EQUAL_INT(1, a.had_error);

    acl_async_result_t t = {0};
    atomic_init(&t.fired, 0);
    kafka_admin_AdminClient_force_terminate_transaction_async(NULL, "t", -1, on_void_transaction_op,
                                                             &t);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&t.fired));
    TEST_ASSERT_EQUAL_INT(1, t.had_error);

    /* Marshaling failures fire inline on a *valid* handle too, so the callback
     * can run before the entry point returns. */
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    acl_async_result_t bad_topic = {0};
    atomic_init(&bad_topic.fired, 0);
    kafka_admin_AdminClient_abort_transaction_async(admin, NULL, 0, 1, 1, 1, -1,
                                                   on_void_transaction_op, &bad_topic);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&bad_topic.fired));
    TEST_ASSERT_EQUAL_INT(1, bad_topic.had_error);
    TEST_ASSERT_EQUAL_STRING("abort transaction topic must not be null", bad_topic.message);

    acl_async_result_t bad_epoch = {0};
    atomic_init(&bad_epoch.fired, 0);
    kafka_admin_AdminClient_abort_transaction_async(admin, "t", 0, 1, 65537, 1, -1,
                                                   on_void_transaction_op, &bad_epoch);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&bad_epoch.fired));
    TEST_ASSERT_EQUAL_STRING("producer epoch 65537 does not fit in a 16-bit epoch",
                             bad_epoch.message);

    acl_async_result_t bad_id = {0};
    atomic_init(&bad_id.fired, 0);
    kafka_admin_AdminClient_force_terminate_transaction_async(admin, NULL, -1,
                                                             on_void_transaction_op, &bad_id);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&bad_id.fired));
    TEST_ASSERT_EQUAL_STRING("transactional id must not be null", bad_id.message);

    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_b6_null_out_result(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *topics[1] = {"alpha"};
    const int32_t partitions[1] = {0};
    const char *ids[1] = {"txn-a"};

    /* The per-key RPCs succeed with a NULL out_result and must not build a
     * handle. */
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_producers(admin, topics, partitions, 1, false, 0,
                                                                -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_describe_transactions(admin, ids, 1, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_fence_producers(admin, ids, 1, -1, NULL));

    /* listTransactions fails the whole call, so it still returns its error. */
    kafka_common_Error_t *error =
        kafka_admin_AdminClient_list_transactions(admin, NULL, 0, NULL, 0, -1, NULL, -1, NULL);
    TEST_ASSERT_NOT_NULL(error);
    kafka_common_Error_destroy(error);

    kafka_admin_AdminClient_destroy(admin);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_mock_admin_create_close_destroy);
    RUN_TEST(test_mock_admin_rejects_zero_brokers);
    RUN_TEST(test_mock_admin_close_async);
    RUN_TEST(test_mock_admin_close_async_null_handle_fires_error);
    RUN_TEST(test_admin_properties_lifecycle);
    RUN_TEST(test_admin_properties_from_configs);
    RUN_TEST(test_admin_client_new_rejects_empty_bootstrap);
    RUN_TEST(test_admin_client_new_null_props);
    RUN_TEST(test_mock_admin_create_topics_sync);
    RUN_TEST(test_mock_admin_create_topics_partial_failure);
    RUN_TEST(test_mock_admin_create_topics_broker_defaults);
    RUN_TEST(test_mock_admin_create_topics_replicas_assignment);
    RUN_TEST(test_mock_admin_new_topic_null_handling);
    RUN_TEST(test_mock_admin_create_topics_async_partial_failure);
    RUN_TEST(test_mock_admin_create_topics_async_null_handle);
    RUN_TEST(test_mock_admin_list_topics_sync);
    RUN_TEST(test_mock_admin_list_topics_async);
    RUN_TEST(test_mock_admin_list_topics_call_error);
    RUN_TEST(test_mock_admin_describe_topics_by_names);
    RUN_TEST(test_mock_admin_describe_topics_by_ids);
    RUN_TEST(test_mock_admin_describe_topics_async);
    RUN_TEST(test_mock_admin_describe_topics_by_ids_async_bad_id);
    RUN_TEST(test_mock_admin_delete_topics_by_names);
    RUN_TEST(test_mock_admin_delete_topics_by_ids);
    RUN_TEST(test_mock_admin_delete_topics_async);
    RUN_TEST(test_mock_admin_delete_topics_by_ids_async);
    RUN_TEST(test_mock_admin_create_partitions_reports_unsupported_per_topic);
    RUN_TEST(test_mock_admin_create_partitions_with_assignments_and_sorting);
    RUN_TEST(test_mock_admin_create_partitions_null_and_empty_handling);
    RUN_TEST(test_mock_admin_create_partitions_async);
    RUN_TEST(test_mock_admin_create_partitions_async_null_handle);
    RUN_TEST(test_mock_admin_delete_records_reports_unsupported_per_partition);
    RUN_TEST(test_mock_admin_delete_records_empty_and_null_handling);
    RUN_TEST(test_mock_admin_delete_records_async);
    RUN_TEST(test_mock_admin_delete_records_async_null_handle);
    RUN_TEST(test_mock_admin_null_out_result);
    RUN_TEST(test_mock_admin_describe_cluster_sync);
    RUN_TEST(test_mock_admin_describe_cluster_call_error);
    RUN_TEST(test_mock_admin_describe_cluster_async);
    RUN_TEST(test_mock_admin_describe_cluster_async_null_handle);
    RUN_TEST(test_mock_admin_describe_configs_partial_failure);
    RUN_TEST(test_mock_admin_describe_configs_null_row_skipped);
    RUN_TEST(test_mock_admin_describe_configs_async_partial_failure);
    RUN_TEST(test_mock_admin_describe_configs_async_null_handle);
    RUN_TEST(test_mock_admin_incremental_alter_configs_set_then_delete);
    RUN_TEST(test_mock_admin_incremental_alter_configs_partial_failure);
    RUN_TEST(test_mock_admin_incremental_alter_configs_bad_op_type);
    RUN_TEST(test_mock_admin_incremental_alter_configs_async);
    RUN_TEST(test_mock_admin_incremental_alter_configs_async_bad_op_type);
    RUN_TEST(test_mock_admin_incremental_alter_configs_async_null_handle);
    RUN_TEST(test_mock_admin_list_config_resources);
    RUN_TEST(test_mock_admin_list_config_resources_async);
    RUN_TEST(test_mock_admin_list_config_resources_async_null_handle);
    RUN_TEST(test_mock_admin_list_client_metrics_resources);
    RUN_TEST(test_mock_admin_list_client_metrics_resources_async);
    RUN_TEST(test_mock_admin_list_client_metrics_resources_async_null_handle);
    RUN_TEST(test_mock_admin_describe_log_dirs);
    RUN_TEST(test_mock_admin_describe_log_dirs_async);
    RUN_TEST(test_mock_admin_describe_log_dirs_async_null_handle);
    RUN_TEST(test_mock_admin_alter_replica_log_dirs_partial_failure);
    RUN_TEST(test_mock_admin_alter_replica_log_dirs_async);
    RUN_TEST(test_mock_admin_alter_replica_log_dirs_async_null_handle);
    RUN_TEST(test_mock_admin_describe_replica_log_dirs);
    RUN_TEST(test_mock_admin_describe_replica_log_dirs_async);
    RUN_TEST(test_mock_admin_describe_replica_log_dirs_async_null_handle);
    RUN_TEST(test_mock_admin_b2_null_out_result);
    RUN_TEST(test_mock_admin_elect_leaders_reports_unsupported);
    RUN_TEST(test_mock_admin_elect_leaders_rejects_bad_election_type);
    RUN_TEST(test_mock_admin_elect_leaders_async);
    RUN_TEST(test_mock_admin_elect_leaders_async_bad_election_type);
    RUN_TEST(test_mock_admin_elect_leaders_async_null_handle);
    RUN_TEST(test_mock_admin_alter_partition_reassignments_partial_failure);
    RUN_TEST(test_mock_admin_alter_partition_reassignments_rejects_empty_replicas);
    RUN_TEST(test_mock_admin_list_partition_reassignments_round_trip);
    RUN_TEST(test_mock_admin_list_partition_reassignments_after_delete_returns_error);
    RUN_TEST(test_mock_admin_alter_partition_reassignments_async);
    RUN_TEST(test_mock_admin_alter_partition_reassignments_async_empty_replicas);
    RUN_TEST(test_mock_admin_alter_partition_reassignments_async_null_handle);
    RUN_TEST(test_mock_admin_list_partition_reassignments_async);
    RUN_TEST(test_mock_admin_list_partition_reassignments_async_null_handle);
    RUN_TEST(test_mock_admin_list_offsets_earliest_and_latest);
    RUN_TEST(test_mock_admin_list_offsets_timestamp_flag_is_load_bearing);
    RUN_TEST(test_mock_admin_list_offsets_rejects_bad_inputs);
    RUN_TEST(test_mock_admin_list_offsets_async);
    RUN_TEST(test_mock_admin_list_offsets_async_bad_isolation_level);
    RUN_TEST(test_mock_admin_list_offsets_async_null_handle);
    RUN_TEST(test_mock_admin_offset_drivers_reject_non_mock);
    RUN_TEST(test_mock_admin_b3_null_out_result);
    RUN_TEST(test_mock_admin_list_groups_reports_seeded_groups);
    RUN_TEST(test_mock_admin_list_groups_with_no_groups_is_empty);
    RUN_TEST(test_mock_admin_list_groups_async);
    RUN_TEST(test_mock_admin_list_groups_async_null_handle);
    RUN_TEST(test_mock_admin_list_consumer_groups_reports_seeded_groups);
    RUN_TEST(test_mock_admin_list_consumer_groups_async);
    RUN_TEST(test_mock_admin_describe_consumer_groups_reports_unsupported_per_group);
    RUN_TEST(test_mock_admin_describe_classic_groups_reports_unsupported_per_group);
    RUN_TEST(test_mock_admin_describe_consumer_groups_async);
    RUN_TEST(test_mock_admin_describe_consumer_groups_async_null_handle);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_round_trip);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_rejects_a_negative_seeded_offset);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_rejects_bad_group_ids);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_two_groups_are_unsupported);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_async);
    RUN_TEST(test_mock_admin_list_consumer_group_offsets_async_null_group_id);
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
    RUN_TEST(test_mock_admin_group_offsets_driver_rejects_non_mock);
    RUN_TEST(test_mock_admin_b4_null_out_result);
    RUN_TEST(test_mock_admin_create_acls_reports_unsupported_per_binding);
    RUN_TEST(test_mock_admin_create_acls_rejects_what_javas_constructors_reject);
    RUN_TEST(test_mock_admin_create_acls_async);
    RUN_TEST(test_mock_admin_create_acls_async_reports_marshaling_failure);
    RUN_TEST(test_mock_admin_create_acls_async_null_handle);
    RUN_TEST(test_mock_admin_describe_acls_fails_the_whole_call);
    RUN_TEST(test_mock_admin_describe_acls_accepts_any_match_and_nulls);
    RUN_TEST(test_mock_admin_describe_acls_async);
    RUN_TEST(test_mock_admin_describe_acls_async_null_handle);
    RUN_TEST(test_mock_admin_delete_acls_reports_unsupported_per_filter);
    RUN_TEST(test_mock_admin_delete_acls_async);
    RUN_TEST(test_mock_admin_delete_acls_async_null_handle);
    RUN_TEST(test_mock_admin_describe_client_quotas_fails_the_whole_call);
    RUN_TEST(test_mock_admin_describe_client_quotas_rejects_bad_components);
    RUN_TEST(test_mock_admin_describe_client_quotas_async);
    RUN_TEST(test_mock_admin_describe_client_quotas_async_null_handle);
    RUN_TEST(test_mock_admin_alter_client_quotas_reports_unsupported_per_entity);
    RUN_TEST(test_mock_admin_alter_client_quotas_rejects_duplicate_entities);
    RUN_TEST(test_mock_admin_alter_client_quotas_async);
    RUN_TEST(test_mock_admin_alter_client_quotas_async_null_handle);
    RUN_TEST(test_mock_admin_b5a_null_out_result);
    RUN_TEST(test_mock_admin_describe_user_scram_credentials_fails_the_whole_call);
    RUN_TEST(test_mock_admin_alter_user_scram_credentials_reports_unsupported_per_user);
    RUN_TEST(test_mock_admin_alter_user_scram_credentials_rejects_bad_rows);
    RUN_TEST(test_mock_admin_scram_async);
    RUN_TEST(test_mock_admin_scram_async_null_handle_and_marshaling_failure);
    RUN_TEST(test_mock_admin_delegation_token_lifecycle);
    RUN_TEST(test_mock_admin_describe_delegation_token_owner_filter);
    RUN_TEST(test_mock_admin_create_delegation_token_rejects_bad_input);
    RUN_TEST(test_mock_admin_describe_features_reports_seeded_levels);
    RUN_TEST(test_mock_admin_update_features_applies_and_validates);
    RUN_TEST(test_mock_admin_update_features_rejects_bad_input);
    RUN_TEST(test_mock_admin_b5b_token_and_feature_async);
    RUN_TEST(test_mock_admin_b5b_async_null_handle);
    RUN_TEST(test_mock_admin_b5b_null_out_result);
    RUN_TEST(test_mock_admin_set_feature_levels_rejects_non_mock);
    RUN_TEST(test_mock_admin_describe_producers_reports_unsupported_per_partition);
    RUN_TEST(test_mock_admin_describe_transactions_reports_unsupported_per_id);
    RUN_TEST(test_mock_admin_fence_producers_reports_unsupported_per_id);
    RUN_TEST(test_mock_admin_list_transactions_fails_the_whole_call);
    RUN_TEST(test_mock_admin_abort_and_terminate_transaction_report_unsupported);
    RUN_TEST(test_mock_admin_b6_async_fires_once);
    RUN_TEST(test_mock_admin_b6_async_null_handle_and_marshaling_failure);
    RUN_TEST(test_mock_admin_b6_null_out_result);
    return UNITY_END();
}
