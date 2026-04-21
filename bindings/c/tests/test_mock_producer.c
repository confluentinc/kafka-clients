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
#include <inttypes.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

void test_create_and_destroy(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    TEST_ASSERT_NOT_NULL(producer);
    kafka_producer_Producer_destroy(producer);
}

void test_destroy_null(void) {
    kafka_producer_Producer_destroy(NULL);
}

// ---------------------------------------------------------------------------
// Send tests
// ---------------------------------------------------------------------------

void test_send_with_key_and_value(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "my-key";
    const uint8_t value[] = "my-value";

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "test-topic", 2, -1,
        key, (int32_t)sizeof(key) - 1,
        value, (int32_t)sizeof(value) - 1,
        &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    /* Get metadata */
    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);

    /* Verify getters */
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(2, kafka_producer_RecordMetadata_partition(metadata));

    const char *returned_topic = kafka_producer_RecordMetadata_topic(metadata);
    TEST_ASSERT_NOT_NULL(returned_topic);
    TEST_ASSERT_EQUAL_STRING("test-topic", returned_topic);

    /* MockProducer always returns NO_TIMESTAMP (-1) */
    TEST_ASSERT_EQUAL_INT64(-1, kafka_producer_RecordMetadata_timestamp(metadata));

    /* History */
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_null_key(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "value-only";

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_send_null_value(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "key-only";

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        key, (int32_t)sizeof(key) - 1,
        NULL, -1,
        &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Multiple sends (incrementing offsets)
// ---------------------------------------------------------------------------

void test_multiple_sends_incrementing_offsets(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    for (int i = 0; i < 3; i++) {
        kafka_common_KafkaError_t *err = NULL;
        kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
            producer, "topic", 0, -1,
            NULL, -1, NULL, -1, &err);
        TEST_ASSERT_NULL(err);

        err = NULL;
        kafka_producer_RecordMetadata_t *metadata =
            kafka_producer_FutureRecordMetadata_get(future, &err);
        TEST_ASSERT_NULL(err);
        TEST_ASSERT_EQUAL_INT64((int64_t)i, kafka_producer_RecordMetadata_offset(metadata));

        kafka_producer_RecordMetadata_destroy(metadata);
        kafka_producer_FutureRecordMetadata_destroy(future);
    }

    TEST_ASSERT_EQUAL_INT32(3, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Manual complete mode
// ---------------------------------------------------------------------------

void test_manual_complete(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(future);
    TEST_ASSERT_FALSE(kafka_producer_FutureRecordMetadata_is_done(future));

    /* Complete it */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_complete_next(producer));
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    /* Get metadata and verify getters */
    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_manual_error(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);

    /* Complete with error (code 2 = CorruptMessage) */
    TEST_ASSERT_TRUE(kafka_producer_MockProducer_error_next(producer, 2, "test error"));

    err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(future, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(metadata);

    /* Inspect error */
    TEST_ASSERT_EQUAL_INT32(2, kafka_common_KafkaError_code(err));

    const char *msg = kafka_common_KafkaError_message(err);
    TEST_ASSERT_NOT_NULL(msg);

    kafka_common_KafkaError_destroy(err);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

void test_complete_next_no_pending(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
    TEST_ASSERT_FALSE(kafka_producer_MockProducer_complete_next(producer));
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Batch send
// ---------------------------------------------------------------------------

void test_send_batch(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "key";
    const uint8_t value[] = "value";

    kafka_producer_ProducerRecord_t records[2] = {
        { "topic1", -1, -1, key, (int32_t)sizeof(key) - 1,
          value, (int32_t)sizeof(value) - 1 },
        { "topic2", 1, -1, NULL, -1, NULL, -1 },
    };

    kafka_producer_FutureRecordMetadata_t *futures[2] = { NULL, NULL };
    kafka_common_KafkaError_t *errors[2] = { NULL, NULL };

    int32_t sent = kafka_producer_Producer_send_batch(
        producer, records, 2, futures, errors);
    TEST_ASSERT_EQUAL_INT32(2, sent);

    for (int i = 0; i < 2; i++) {
        TEST_ASSERT_NULL(errors[i]);
        TEST_ASSERT_NOT_NULL(futures[i]);
        TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(futures[i]));
        kafka_producer_FutureRecordMetadata_destroy(futures[i]);
    }

    TEST_ASSERT_EQUAL_INT32(2, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_Producer_destroy(producer);
}

void test_send_batch_partial_failure(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    /* Record at index 1 has null topic -> error */
    kafka_producer_ProducerRecord_t records[3] = {
        { "topic1", -1, -1, NULL, -1, NULL, -1 },
        { NULL, -1, -1, NULL, -1, NULL, -1 },  /* null topic */
        { "topic3", -1, -1, NULL, -1, NULL, -1 },
    };

    kafka_producer_FutureRecordMetadata_t *futures[3] = { NULL, NULL, NULL };
    kafka_common_KafkaError_t *errors[3] = { NULL, NULL, NULL };

    int32_t sent = kafka_producer_Producer_send_batch(
        producer, records, 3, futures, errors);
    TEST_ASSERT_EQUAL_INT32(2, sent);

    /* Index 0: success */
    TEST_ASSERT_NOT_NULL(futures[0]);
    TEST_ASSERT_NULL(errors[0]);

    /* Index 1: error */
    TEST_ASSERT_NULL(futures[1]);
    TEST_ASSERT_NOT_NULL(errors[1]);

    /* Index 2: success */
    TEST_ASSERT_NOT_NULL(futures[2]);
    TEST_ASSERT_NULL(errors[2]);

    kafka_producer_FutureRecordMetadata_destroy(futures[0]);
    kafka_common_KafkaError_destroy(errors[1]);
    kafka_producer_FutureRecordMetadata_destroy(futures[2]);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Close then send
// ---------------------------------------------------------------------------

void test_close_then_send(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);

    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

void test_flush(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_FALSE(kafka_producer_FutureRecordMetadata_is_done(future));

    err = NULL;
    kafka_producer_Producer_flush(producer, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_TRUE(kafka_producer_FutureRecordMetadata_is_done(future));

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// Error inspection
// ---------------------------------------------------------------------------

void test_error_inspection(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_close(producer, &err);
    TEST_ASSERT_NULL(err);

    err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(future);

    TEST_ASSERT_NOT_EQUAL(0, kafka_common_KafkaError_code(err));

    const char *msg = kafka_common_KafkaError_message(err);
    TEST_ASSERT_NOT_NULL(msg);
    TEST_ASSERT_TRUE(strlen(msg) > 0);

    /* is_retriable and is_fatal should be callable */
    (void)kafka_common_KafkaError_is_retriable(err);
    (void)kafka_common_KafkaError_is_fatal(err);

    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);
}

void test_error_null_safety(void) {
    TEST_ASSERT_EQUAL_INT32(0, kafka_common_KafkaError_code(NULL));
    TEST_ASSERT_NULL(kafka_common_KafkaError_message(NULL));
    TEST_ASSERT_FALSE(kafka_common_KafkaError_is_retriable(NULL));
    TEST_ASSERT_FALSE(kafka_common_KafkaError_is_fatal(NULL));
    kafka_common_KafkaError_destroy(NULL);  /* no-op */
}

// ---------------------------------------------------------------------------
// Mock-specific: clear and history
// ---------------------------------------------------------------------------

void test_mock_clear(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_FutureRecordMetadata_t *future = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(1, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_MockProducer_clear(producer);
    TEST_ASSERT_EQUAL_INT32(0, kafka_producer_MockProducer_history_count(producer));

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// RecordMetadata getters from batch send
// ---------------------------------------------------------------------------

void test_record_metadata_getters_from_batch(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "key";
    const uint8_t value[] = "value";

    kafka_producer_ProducerRecord_t records[1] = {
        { "getter-topic", 7, -1, key, (int32_t)sizeof(key) - 1,
          value, (int32_t)sizeof(value) - 1 },
    };

    kafka_producer_FutureRecordMetadata_t *futures[1] = { NULL };
    kafka_common_KafkaError_t *errors[1] = { NULL };

    int32_t sent = kafka_producer_Producer_send_batch(
        producer, records, 1, futures, errors);
    TEST_ASSERT_EQUAL_INT32(1, sent);
    TEST_ASSERT_NULL(errors[0]);

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_RecordMetadata_t *metadata =
        kafka_producer_FutureRecordMetadata_get(futures[0], &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(metadata);

    /* Verify all getters */
    TEST_ASSERT_EQUAL_INT64(0, kafka_producer_RecordMetadata_offset(metadata));
    TEST_ASSERT_EQUAL_INT32(7, kafka_producer_RecordMetadata_partition(metadata));

    const char *topic = kafka_producer_RecordMetadata_topic(metadata);
    TEST_ASSERT_NOT_NULL(topic);
    TEST_ASSERT_EQUAL_STRING("getter-topic", topic);

    TEST_ASSERT_EQUAL_INT64(-1, kafka_producer_RecordMetadata_timestamp(metadata));

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(futures[0]);
    kafka_producer_Producer_destroy(producer);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(void) {
    UNITY_BEGIN();

    /* Lifecycle */
    RUN_TEST(test_create_and_destroy);
    RUN_TEST(test_destroy_null);

    /* Send */
    RUN_TEST(test_send_with_key_and_value);
    RUN_TEST(test_send_null_key);
    RUN_TEST(test_send_null_value);
    RUN_TEST(test_multiple_sends_incrementing_offsets);

    /* Manual complete */
    RUN_TEST(test_manual_complete);
    RUN_TEST(test_manual_error);
    RUN_TEST(test_complete_next_no_pending);

    /* Batch */
    RUN_TEST(test_send_batch);
    RUN_TEST(test_send_batch_partial_failure);

    /* Close */
    RUN_TEST(test_close_then_send);

    /* Flush */
    RUN_TEST(test_flush);

    /* Error */
    RUN_TEST(test_error_inspection);
    RUN_TEST(test_error_null_safety);

    /* Mock-specific */
    RUN_TEST(test_mock_clear);

    /* RecordMetadata getters */
    RUN_TEST(test_record_metadata_getters_from_batch);

    return UNITY_END();
}
