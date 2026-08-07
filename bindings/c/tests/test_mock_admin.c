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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_create_topics(
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

static void on_close(kafka_common_KafkaError_t *error, void *user_data) {
    op_result_t *r = (op_result_t *)user_data;
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err = NULL;
    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, &err);
    TEST_ASSERT_NULL(admin);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);
    kafka_admin_AdminClientProperties_destroy(props);
}

static void test_admin_client_new_null_props(void) {
    kafka_common_KafkaError_t *err = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_new(NULL, &err));
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_create_topics(
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_create_topics(
        admin, topics, 3, -1, false, false, &result);
    /* A per-key failure is NOT a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(3, kafka_admin_CreateTopicsResult_count(result));

    int32_t i_existing = find_create_key(result, "existing");
    int32_t i_fresh = find_create_key(result, "fresh");
    int32_t i_many = find_create_key(result, "too-many-replicas");
    TEST_ASSERT_TRUE(i_existing >= 0 && i_fresh >= 0 && i_many >= 0);

    const kafka_common_KafkaError_t *e_existing =
        kafka_admin_CreateTopicsResult_get_error(result, i_existing);
    TEST_ASSERT_NOT_NULL(e_existing);
    TEST_ASSERT_EQUAL_INT32(TOPIC_ALREADY_EXISTS_CODE,
                            kafka_common_KafkaError_code(e_existing));
    TEST_ASSERT_EQUAL_STRING("Topic existing exists already.",
                             kafka_common_KafkaError_message(e_existing));
    TEST_ASSERT_NULL(kafka_admin_CreateTopicsResult_get_value(result, i_existing));

    const kafka_common_KafkaError_t *e_many =
        kafka_admin_CreateTopicsResult_get_error(result, i_many);
    TEST_ASSERT_NOT_NULL(e_many);
    TEST_ASSERT_EQUAL_INT32(INVALID_REPLICATION_FACTOR_CODE,
                            kafka_common_KafkaError_code(e_many));

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

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t error_code_for_existing;
    int32_t partitions_for_fresh;
} create_async_result_t;

static void on_create(kafka_admin_CreateTopicsResult_t *result,
                      kafka_common_KafkaError_t *error, void *user_data) {
    create_async_result_t *r = (create_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_CreateTopicsResult_count(result);
        int32_t i = find_create_key(result, "existing");
        if (i >= 0) {
            const kafka_common_KafkaError_t *e =
                kafka_admin_CreateTopicsResult_get_error(result, i);
            r->error_code_for_existing = e ? kafka_common_KafkaError_code(e) : 0;
        }
        int32_t j = find_create_key(result, "fresh-async");
        if (j >= 0) {
            const kafka_admin_TopicMetadataAndConfig_t *mc =
                kafka_admin_CreateTopicsResult_get_value(result, j);
            r->partitions_for_fresh =
                mc ? kafka_admin_TopicMetadataAndConfig_num_partitions(mc) : -1;
        }
        kafka_admin_CreateTopicsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
    kafka_admin_AdminClient_create_topics_async(admin, topics, 2, -1, false, false,
                                                on_create, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(2, r.count);
    TEST_ASSERT_EQUAL_INT32(TOPIC_ALREADY_EXISTS_CODE, r.error_code_for_existing);
    TEST_ASSERT_EQUAL_INT32(3, r.partitions_for_fresh);

    kafka_admin_NewTopic_destroy(a);
    kafka_admin_NewTopic_destroy(b);
    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_create_topics_async_null_handle(void) {
    create_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_create_topics_async(NULL, NULL, 0, -1, false, false,
                                                on_create, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
}

// ---------------------------------------------------------------------------
// listTopics
// ---------------------------------------------------------------------------

static void test_mock_admin_list_topics_sync(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "beta", 1, 1);
    create_one(admin, "alpha", 1, 1);

    kafka_admin_ListTopicsResult_t *result = NULL;
    kafka_common_KafkaError_t *err =
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
                    kafka_common_KafkaError_t *error, void *user_data) {
    list_async_result_t *r = (list_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListTopicsResult_count(result);
        kafka_admin_ListTopicsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err =
        kafka_admin_AdminClient_list_topics(admin, -1, true, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("The mock timed out the request.",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_describe_topics(
        admin, names, 2, -1, false, -1, &result);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DescribeTopicsResult_count(result));

    int32_t i_ok = find_describe_key(result, "described");
    int32_t i_bad = find_describe_key(result, "missing");
    TEST_ASSERT_TRUE(i_ok >= 0 && i_bad >= 0);

    /* Partial failure: one description, one per-key error. */
    const kafka_common_KafkaError_t *e =
        kafka_admin_DescribeTopicsResult_get_error(result, i_bad);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE,
                            kafka_common_KafkaError_code(e));
    TEST_ASSERT_EQUAL_STRING("Topic missing not found.", kafka_common_KafkaError_message(e));
    TEST_ASSERT_NULL(kafka_admin_DescribeTopicsResult_get_value(result, i_bad));

    TEST_ASSERT_NULL(kafka_admin_DescribeTopicsResult_get_error(result, i_ok));
    const kafka_admin_TopicDescription_t *d =
        kafka_admin_DescribeTopicsResult_get_value(result, i_ok);
    TEST_ASSERT_NOT_NULL(d);
    TEST_ASSERT_EQUAL_STRING("described", kafka_admin_TopicDescription_name(d));
    TEST_ASSERT_FALSE(kafka_admin_TopicDescription_is_internal(d));
    TEST_ASSERT_TRUE(strlen(kafka_admin_TopicDescription_topic_id(d)) > 0);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_TopicDescription_partition_count(d));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicDescription_authorized_operation_count(d));
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
    /* The mock reports an empty (not absent) ELR set, so the count is 0. */
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicPartitionInfo_elr_count(p0));
    TEST_ASSERT_EQUAL_INT32(0, kafka_admin_TopicPartitionInfo_last_known_elr_count(p0));
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_describe_topics_by_ids(
        admin, bad_ids, 1, -1, false, -1, &bad_result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(bad_result);
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t partition_count;
} describe_async_result_t;

static void on_describe(kafka_admin_DescribeTopicsResult_t *result,
                        kafka_common_KafkaError_t *error, void *user_data) {
    describe_async_result_t *r = (describe_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_DescribeTopicsResult_count(result);
        const kafka_admin_TopicDescription_t *d =
            kafka_admin_DescribeTopicsResult_get_value(result, 0);
        r->partition_count = d ? kafka_admin_TopicDescription_partition_count(d) : -1;
        kafka_admin_DescribeTopicsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_describe_topics_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "async-described", 5, 1);

    const char *names[1] = {"async-described"};
    describe_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_describe_topics_async(admin, names, 1, -1, false, -1,
                                                  on_describe, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
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
    kafka_admin_AdminClient_describe_topics_by_ids_async(admin, bad_ids, 1, -1, false,
                                                         -1, on_describe, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_delete_topics(
        admin, names, 2, -1, false, &result);
    /* Partial failure is not a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_DeleteTopicsResult_count(result));

    int32_t i_ok = find_delete_key(result, "doomed");
    int32_t i_bad = find_delete_key(result, "never-existed");
    TEST_ASSERT_TRUE(i_ok >= 0 && i_bad >= 0);
    TEST_ASSERT_NULL(kafka_admin_DeleteTopicsResult_get_error(result, i_ok));

    const kafka_common_KafkaError_t *e =
        kafka_admin_DeleteTopicsResult_get_error(result, i_bad);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE,
                            kafka_common_KafkaError_code(e));
    TEST_ASSERT_EQUAL_STRING("Topic never-existed does not exist.",
                             kafka_common_KafkaError_message(e));
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

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t error_code_for_missing;
} delete_async_result_t;

static void on_delete(kafka_admin_DeleteTopicsResult_t *result,
                      kafka_common_KafkaError_t *error, void *user_data) {
    delete_async_result_t *r = (delete_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_DeleteTopicsResult_count(result);
        int32_t i = find_delete_key(result, "nope");
        if (i >= 0) {
            const kafka_common_KafkaError_t *e =
                kafka_admin_DeleteTopicsResult_get_error(result, i);
            r->error_code_for_missing = e ? kafka_common_KafkaError_code(e) : 0;
        }
        kafka_admin_DeleteTopicsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_delete_topics_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    create_one(admin, "gone-async", 1, 1);

    const char *names[2] = {"gone-async", "nope"};
    delete_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_delete_topics_async(admin, names, 2, -1, false, on_delete, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(2, r.count);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, r.error_code_for_missing);
    kafka_admin_AdminClient_destroy(admin);
}

static void test_mock_admin_delete_topics_by_ids_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    const char *bad_ids[1] = {"@@@@"};
    delete_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_delete_topics_by_ids_async(admin, bad_ids, 1, -1, false,
                                                        on_delete, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
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

    kafka_admin_NewPartitions_t *np = kafka_admin_NewPartitions_new(4);
    TEST_ASSERT_NOT_NULL(np);
    const char *topics[1] = {"grow-me"};
    const kafka_admin_NewPartitions_t *specs[1] = {np};

    kafka_admin_CreatePartitionsResult_t *result = NULL;
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_create_partitions(
        admin, topics, specs, 1, 5000, false, true, &result);
    /* A per-topic failure is NOT a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(1, kafka_admin_CreatePartitionsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("grow-me",
                             kafka_admin_CreatePartitionsResult_get_key(result, 0));

    const kafka_common_KafkaError_t *e =
        kafka_admin_CreatePartitionsResult_get_error(result, 0);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_KafkaError_code(e));
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_KafkaError_message(e));

    /* Out-of-range indices are null / no crash. */
    TEST_ASSERT_NULL(kafka_admin_CreatePartitionsResult_get_key(result, 1));
    TEST_ASSERT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, 1));
    TEST_ASSERT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, -1));

    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_admin_CreatePartitionsResult_destroy(NULL);
    kafka_admin_NewPartitions_destroy(np);
    kafka_admin_AdminClient_destroy(admin);
}

/* Replica assignments switch the entry to Java's
 * NewPartitions.increaseTo(totalCount, newAssignments) form. The mock rejects
 * the RPC either way, so this pins the marshaling path: the assignment is
 * accepted, entries stay sorted by topic name, and every key gets an outcome. */
static void test_mock_admin_create_partitions_with_assignments_and_sorting(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(3);

    kafka_admin_NewPartitions_t *plain = kafka_admin_NewPartitions_new(2);
    kafka_admin_NewPartitions_t *assigned = kafka_admin_NewPartitions_new(3);
    int32_t brokers0[] = {0, 1};
    int32_t brokers1[] = {1, 2};
    kafka_admin_NewPartitions_add_assignment(assigned, brokers0, 2);
    kafka_admin_NewPartitions_add_assignment(assigned, brokers1, 2);
    /* Null handle / null broker array are no-ops, not crashes. */
    kafka_admin_NewPartitions_add_assignment(NULL, brokers0, 2);
    kafka_admin_NewPartitions_add_assignment(assigned, NULL, 2);

    /* Deliberately unsorted input; the result is sorted by topic name. */
    const char *topics[2] = {"zeta", "alpha"};
    const kafka_admin_NewPartitions_t *specs[2] = {plain, assigned};

    kafka_admin_CreatePartitionsResult_t *result = NULL;
    TEST_ASSERT_NULL(kafka_admin_AdminClient_create_partitions(admin, topics, specs, 2,
                                                               -1, false, true, &result));
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_CreatePartitionsResult_count(result));
    TEST_ASSERT_EQUAL_STRING("alpha", kafka_admin_CreatePartitionsResult_get_key(result, 0));
    TEST_ASSERT_EQUAL_STRING("zeta", kafka_admin_CreatePartitionsResult_get_key(result, 1));
    TEST_ASSERT_NOT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, 0));
    TEST_ASSERT_NOT_NULL(kafka_admin_CreatePartitionsResult_get_error(result, 1));

    kafka_admin_CreatePartitionsResult_destroy(result);
    kafka_admin_NewPartitions_destroy(plain);
    kafka_admin_NewPartitions_destroy(assigned);
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

    kafka_admin_NewPartitions_t *np = kafka_admin_NewPartitions_new(2);
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

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t error_code_for_first;
} create_partitions_async_result_t;

static void on_create_partitions(kafka_admin_CreatePartitionsResult_t *result,
                                 kafka_common_KafkaError_t *error, void *user_data) {
    create_partitions_async_result_t *r = (create_partitions_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_CreatePartitionsResult_count(result);
        const kafka_common_KafkaError_t *e =
            kafka_admin_CreatePartitionsResult_get_error(result, 0);
        r->error_code_for_first = e ? kafka_common_KafkaError_code(e) : 0;
        kafka_admin_CreatePartitionsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
    }
    atomic_fetch_add(&r->fired, 1);
}

static void test_mock_admin_create_partitions_async(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);
    kafka_admin_NewPartitions_t *np = kafka_admin_NewPartitions_new(6);
    const char *topics[1] = {"async-grow"};
    const kafka_admin_NewPartitions_t *specs[1] = {np};

    create_partitions_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_create_partitions_async(admin, topics, specs, 1, -1, false,
                                                    true, on_create_partitions, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, r.error_code_for_first);

    kafka_admin_NewPartitions_destroy(np);
    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_create_partitions_async_null_handle(void) {
    create_partitions_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_create_partitions_async(NULL, NULL, NULL, 0, -1, false, true,
                                                    on_create_partitions, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_delete_records(
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
        const kafka_common_KafkaError_t *e =
            kafka_admin_DeleteRecordsResult_get_error(result, i);
        TEST_ASSERT_NOT_NULL(e);
        TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_KafkaError_code(e));
        TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_KafkaError_message(e));
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

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
    int32_t error_code_for_first;
} delete_records_async_result_t;

static void on_delete_records(kafka_admin_DeleteRecordsResult_t *result,
                              kafka_common_KafkaError_t *error, void *user_data) {
    delete_records_async_result_t *r = (delete_records_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_DeleteRecordsResult_count(result);
        const kafka_common_KafkaError_t *e =
            kafka_admin_DeleteRecordsResult_get_error(result, 0);
        r->error_code_for_first = e ? kafka_common_KafkaError_code(e) : 0;
        kafka_admin_DeleteRecordsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
    kafka_admin_AdminClient_delete_records_async(admin, topics, partitions, offsets, 1,
                                                 -1, on_delete_records, &r);
    TEST_ASSERT_TRUE(wait_for(&r.fired, 1));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_result);
    TEST_ASSERT_FALSE(r.had_error);
    TEST_ASSERT_EQUAL_INT32(1, r.count);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, r.error_code_for_first);

    kafka_admin_AdminClient_destroy(admin);
}

/* A NULL handle must still honor the callback obligation, with an error. */
static void test_mock_admin_delete_records_async_null_handle(void) {
    delete_records_async_result_t r = {0};
    atomic_init(&r.fired, 0);
    kafka_admin_AdminClient_delete_records_async(NULL, NULL, NULL, NULL, 0, -1,
                                                 on_delete_records, &r);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&r.fired));
    TEST_ASSERT_TRUE(r.had_error);
    TEST_ASSERT_FALSE(r.had_result);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_incremental_alter_configs(
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
    kafka_common_KafkaError_t *err =
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

    /* Empty set, not absent: -1 would mean the broker did not report them. */
    TEST_ASSERT_EQUAL_INT32(
        0, kafka_admin_DescribeClusterResult_authorized_operation_count(result));
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
    kafka_common_KafkaError_t *err =
        kafka_admin_AdminClient_describe_cluster(admin, -1, false, false, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_KafkaError_destroy(err);

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
                                kafka_common_KafkaError_t *error, void *user_data) {
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
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_describe_configs(
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
    const kafka_common_KafkaError_t *e = kafka_admin_DescribeConfigsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, kafka_common_KafkaError_code(e));

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
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_KafkaError_code(e));

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
                                kafka_common_KafkaError_t *error, void *user_data) {
    describe_configs_async_result_t *r = (describe_configs_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_DescribeConfigsResult_count(result);
        int32_t i = find_describe_configs_key(result, RESOURCE_TYPE_TOPIC, "async-missing");
        const kafka_common_KafkaError_t *e =
            i >= 0 ? kafka_admin_DescribeConfigsResult_get_error(result, i) : NULL;
        r->error_code_for_missing = e ? kafka_common_KafkaError_code(e) : 0;
        i = find_describe_configs_key(result, RESOURCE_TYPE_BROKER, "0");
        r->has_value_for_broker =
            i >= 0 && kafka_admin_DescribeConfigsResult_get_value(result, i) != NULL;
        kafka_admin_DescribeConfigsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
    const kafka_common_KafkaError_t *e = kafka_admin_AlterConfigsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(INVALID_REQUEST_CODE, kafka_common_KafkaError_code(e));

    i = find_alter_configs_key(result, RESOURCE_TYPE_TOPIC, "alter-missing");
    TEST_ASSERT_TRUE(i >= 0);
    e = kafka_admin_AlterConfigsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, kafka_common_KafkaError_code(e));

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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_incremental_alter_configs(
        admin, types, resources, keys, values, ops, 1, -1, false, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_KafkaError_destroy(err);
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
                             kafka_common_KafkaError_t *error, void *user_data) {
    alter_configs_async_result_t *r = (alter_configs_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_AlterConfigsResult_count(result);
        const kafka_common_KafkaError_t *e = kafka_admin_AlterConfigsResult_get_error(result, 0);
        r->error_code_for_first = e ? kafka_common_KafkaError_code(e) : 0;
        kafka_admin_AlterConfigsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
                                     kafka_common_KafkaError_t *error, void *user_data) {
    list_config_resources_async_result_t *r = (list_config_resources_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListConfigResourcesResult_count(result);
        kafka_admin_ListConfigResourcesResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
                                   kafka_common_KafkaError_t *error, void *user_data) {
    list_client_metrics_async_result_t *r = (list_client_metrics_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_ListClientMetricsResourcesResult_count(result);
        kafka_admin_ListClientMetricsResourcesResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err =
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
                                 kafka_common_KafkaError_t *error, void *user_data) {
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
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_alter_replica_log_dirs(
        admin, topics, partitions, broker_ids, log_dirs, 4, -1, &result);
    TEST_ASSERT_NULL(err); /* per-replica failures are not call failures */
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(4, kafka_admin_AlterReplicaLogDirsResult_count(result));

    int32_t i = find_alter_replica_key(result, "mv-topic", 0, 0);
    TEST_ASSERT_TRUE(i >= 0);
    TEST_ASSERT_NULL(kafka_admin_AlterReplicaLogDirsResult_get_error(result, i));

    i = find_alter_replica_key(result, "mv-topic", 1, 0);
    TEST_ASSERT_TRUE(i >= 0);
    const kafka_common_KafkaError_t *e =
        kafka_admin_AlterReplicaLogDirsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(KAFKA_STORAGE_ERROR_CODE, kafka_common_KafkaError_code(e));

    i = find_alter_replica_key(result, "mv-missing", 0, 0);
    TEST_ASSERT_TRUE(i >= 0);
    e = kafka_admin_AlterReplicaLogDirsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(REPLICA_NOT_AVAILABLE_CODE, kafka_common_KafkaError_code(e));

    i = find_alter_replica_key(result, "mv-topic", 0, 9);
    TEST_ASSERT_TRUE(i >= 0);
    e = kafka_admin_AlterReplicaLogDirsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(REPLICA_NOT_AVAILABLE_CODE, kafka_common_KafkaError_code(e));

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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_describe_replica_log_dirs(
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
                                      kafka_common_KafkaError_t *error, void *user_data) {
    alter_replica_async_result_t *r = (alter_replica_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_AlterReplicaLogDirsResult_count(result);
        kafka_admin_AlterReplicaLogDirsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
                                         kafka_common_KafkaError_t *error, void *user_data) {
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
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_alter_partition_reassignments(
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_elect_leaders(
        admin, ELECTION_TYPE_PREFERRED, false, topics, partitions, 2, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_KafkaError_code(err));
    TEST_ASSERT_EQUAL_STRING("Not implemented yet", kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    /* `all_partitions = true` is Java's null Set: the arrays are not read, so
     * passing NULL for them is fine and the mock still refuses. */
    result = NULL;
    err = kafka_admin_AdminClient_elect_leaders(admin, ELECTION_TYPE_UNCLEAN, true, NULL, NULL, 0, -1,
                                                &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

/* An election type outside Java's {0, 1} is rejected before the RPC is issued,
 * with `ElectionType.valueOf(byte)`'s own message. */
static void test_mock_admin_elect_leaders_rejects_bad_election_type(void) {
    kafka_admin_AdminClient_t *admin = kafka_admin_MockAdminClient_new(1);

    kafka_admin_ElectLeadersResult_t *result = NULL;
    kafka_common_KafkaError_t *err =
        kafka_admin_AdminClient_elect_leaders(admin, 7, true, NULL, NULL, 0, -1, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Value 7 must be one of [PREFERRED, UNCLEAN]",
                             kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    kafka_admin_AdminClient_destroy(admin);
}

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t error_code;
} elect_leaders_async_result_t;

static void on_elect_leaders(kafka_admin_ElectLeadersResult_t *result,
                             kafka_common_KafkaError_t *error, void *user_data) {
    elect_leaders_async_result_t *r = (elect_leaders_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        kafka_admin_ElectLeadersResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        r->error_code = kafka_common_KafkaError_code(error);
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 2, -1, true, &result);
    /* A per-partition failure is not a call failure. */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(2, kafka_admin_AlterPartitionReassignmentsResult_count(result));

    /* Sorted by topic then partition, so "ra-missing" precedes "ra-topic". */
    TEST_ASSERT_EQUAL_STRING("ra-missing",
                             kafka_admin_AlterPartitionReassignmentsResult_get_topic(result, 0));
    const kafka_common_KafkaError_t *e =
        kafka_admin_AlterPartitionReassignmentsResult_get_error(result, 0);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNKNOWN_TOPIC_OR_PARTITION_CODE, kafka_common_KafkaError_code(e));

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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, true, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING(
        "reassignment for ra-empty-0 at index 0: Cannot create a new partition reassignment without "
        "any replicas",
        kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

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

typedef struct {
    atomic_int fired;
    int had_result;
    int had_error;
    int32_t count;
} alter_reassign_async_result_t;

static void on_alter_partition_reassignments(kafka_admin_AlterPartitionReassignmentsResult_t *result,
                                             kafka_common_KafkaError_t *error, void *user_data) {
    alter_reassign_async_result_t *r = (alter_reassign_async_result_t *)user_data;
    if (result != NULL) {
        r->had_result = 1;
        r->count = kafka_admin_AlterPartitionReassignmentsResult_count(result);
        kafka_admin_AlterPartitionReassignmentsResult_destroy(result);
    }
    if (error != NULL) {
        r->had_error = 1;
        kafka_common_KafkaError_destroy(error);
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
                                            kafka_common_KafkaError_t *error, void *user_data) {
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
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_list_offsets(
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
    const kafka_common_KafkaError_t *e = kafka_admin_ListOffsetsResult_get_error(result, i);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQUAL_INT32(UNSUPPORTED_VERSION_CODE, kafka_common_KafkaError_code(e));
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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_list_offsets(
        admin, topics, partitions, is_timestamp, bad_spec, 1, -1, ISOLATION_READ_UNCOMMITTED, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING(
        "offset spec for lo-bad-0 at index 0: 42 is not a ListOffsets timestamp sentinel; pass "
        "is_timestamp=true to request OffsetSpec.forTimestamp(42)",
        kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

    const int64_t good_spec[1] = {OFFSET_SPEC_LATEST};
    result = NULL;
    err = kafka_admin_AdminClient_list_offsets(admin, topics, partitions, is_timestamp, good_spec, 1,
                                               -1, 9, &result);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(result);
    TEST_ASSERT_EQUAL_STRING("Unknown isolation level 9", kafka_common_KafkaError_message(err));
    kafka_common_KafkaError_destroy(err);

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
                            kafka_common_KafkaError_t *error, void *user_data) {
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
        kafka_common_KafkaError_destroy(error);
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
    kafka_common_KafkaError_t *new_err = NULL;
    kafka_admin_AdminClient_t *admin = kafka_admin_AdminClient_new(props, &new_err);
    kafka_admin_AdminClientProperties_destroy(props);
    TEST_ASSERT_NULL(new_err);
    TEST_ASSERT_NOT_NULL(admin);

    const char *topics[1] = {"t"};
    const int32_t partitions[1] = {0};
    const int64_t offsets[1] = {1};
    kafka_common_KafkaError_t *err = kafka_admin_MockAdminClient_update_beginning_offsets(
        admin, topics, partitions, offsets, 1);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);
    err = kafka_admin_MockAdminClient_update_end_offsets(admin, topics, partitions, offsets, 1);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

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
    kafka_common_KafkaError_t *err = kafka_admin_AdminClient_elect_leaders(
        admin, ELECTION_TYPE_PREFERRED, false, topics, partitions, 1, -1, NULL);
    TEST_ASSERT_NOT_NULL(err);
    kafka_common_KafkaError_destroy(err);

    TEST_ASSERT_NULL(kafka_admin_AdminClient_alter_partition_reassignments(
        admin, topics, partitions, cancel, replica_ptrs, replica_counts, 1, -1, true, NULL));
    TEST_ASSERT_NULL(
        kafka_admin_AdminClient_list_partition_reassignments(admin, true, NULL, NULL, 0, -1, NULL));
    TEST_ASSERT_NULL(kafka_admin_AdminClient_list_offsets(admin, topics, partitions, is_timestamp,
                                                          specs, 1, -1,
                                                          ISOLATION_READ_UNCOMMITTED, NULL));

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
    return UNITY_END();
}
