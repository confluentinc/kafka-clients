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
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

/* Helper: create a KafkaProducer with a single bootstrap.servers config. */
static kafka_producer_Producer_t *create_producer(const char *bootstrap,
                                                   kafka_common_KafkaError_t **out_err) {
    const char *configs[] = {
        "bootstrap.servers", bootstrap,
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, out_err);
    kafka_producer_ProducerProperties_destroy(props);
    return producer;
}

// ---------------------------------------------------------------------------
// ProducerProperties tests
// ---------------------------------------------------------------------------

void test_properties_new_and_put(void) {
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_new();
    TEST_ASSERT_NOT_NULL(props);

    kafka_producer_ProducerProperties_put(props, "bootstrap.servers", "localhost:9092");

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

void test_properties_from_configs(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        "client.id",         "test-client",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    TEST_ASSERT_NOT_NULL(props);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

void test_properties_from_configs_null(void) {
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(NULL);
    TEST_ASSERT_NULL(props);
}

void test_properties_from_configs_odd(void) {
    /* Missing value for second key — odd number of entries. */
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        "client.id",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    TEST_ASSERT_NULL(props);
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

void test_create_close_destroy(void) {
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer = create_producer("localhost:9092", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    kafka_producer_Producer_destroy(producer);
}

void test_create_destroy_without_close(void) {
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer = create_producer("localhost:9092", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    /* Destroy without close -- Drop impl handles cleanup */
    kafka_producer_Producer_destroy(producer);
}

void test_create_null_props(void) {
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(NULL, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(producer);
    kafka_common_KafkaError_destroy(err);
}

void test_create_null_out_error(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    /* NULL out_error: caller doesn't want error details, should still work */
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, NULL);
    TEST_ASSERT_NOT_NULL(producer);
    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, NULL);
    kafka_producer_Producer_destroy(producer);
}

void test_create_invalid_config_value(void) {
    const char *configs[] = {
        "batch.size", "not-a-number",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(producer);
    kafka_common_KafkaError_destroy(err);
    kafka_producer_ProducerProperties_destroy(props);
}

// ---------------------------------------------------------------------------
// Mock-specific operations should be no-ops for KafkaProducer
// ---------------------------------------------------------------------------

void test_mock_ops_noop(void) {
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer = create_producer("localhost:9092", &err);
    TEST_ASSERT_NULL(err);

    TEST_ASSERT_FALSE(kafka_producer_MockProducer_complete_next(producer));
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_error_next(producer, 2, NULL));
    TEST_ASSERT_EQUAL_INT32(0, kafka_producer_MockProducer_history_count(producer));
    kafka_producer_MockProducer_clear(producer); /* no-op */

    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Multiple config entries
// ---------------------------------------------------------------------------

void test_create_multiple_config(void) {
    const char *configs[] = {
        "bootstrap.servers", "localhost:9092,localhost:9093",
        "client.id",         "my-producer",
        "batch.size",        "32768",
        NULL
    };
    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_from_configs(configs);
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *producer =
        kafka_producer_KafkaProducer_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(producer);

    kafka_producer_ProducerProperties_destroy(props);
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(void) {
    UNITY_BEGIN();

    /* ProducerProperties */
    RUN_TEST(test_properties_new_and_put);
    RUN_TEST(test_properties_from_configs);
    RUN_TEST(test_properties_from_configs_null);
    RUN_TEST(test_properties_from_configs_odd);

    /* Lifecycle */
    RUN_TEST(test_create_close_destroy);
    RUN_TEST(test_create_destroy_without_close);
    RUN_TEST(test_create_null_props);
    RUN_TEST(test_create_null_out_error);
    RUN_TEST(test_create_invalid_config_value);

    /* Mock ops */
    RUN_TEST(test_mock_ops_noop);

    /* Multiple config */
    RUN_TEST(test_create_multiple_config);

    return UNITY_END();
}
