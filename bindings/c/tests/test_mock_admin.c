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
    return UNITY_END();
}
