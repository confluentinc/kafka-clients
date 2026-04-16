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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>

static int tests_run = 0;
static int tests_passed = 0;

#define ASSERT(expr, msg) do { \
    if (!(expr)) { \
        fprintf(stderr, "  FAIL: %s (line %d): %s\n", msg, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

#define ASSERT_SUCCESS(err) do { \
    if ((err) != NULL) { \
        fprintf(stderr, "  FAIL (line %d): expected success but got error: %s\n", \
                __LINE__, kafka_common_KafkaError_message(err)); \
        kafka_common_KafkaError_destroy(err); \
        return 1; \
    } \
} while (0)

#define ASSERT_ERROR(err) do { \
    if ((err) == NULL) { \
        fprintf(stderr, "  FAIL (line %d): expected error but got success\n", __LINE__); \
        return 1; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    tests_run++; \
    printf("Running %s ... ", #fn); \
    if (fn() == 0) { \
        tests_passed++; \
        printf("OK\n"); \
    } else { \
        printf("FAILED\n"); \
    } \
} while (0)

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

static int test_create_and_destroy(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    ASSERT(producer != NULL, "producer should not be null");
    kafka_producer_Producer_destroy(producer);
    return 0;
}

static int test_destroy_null(void) {
    kafka_producer_Producer_destroy(NULL);
    return 0;
}

// ---------------------------------------------------------------------------
// Send tests
// ---------------------------------------------------------------------------

static int test_send_with_key_and_value(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "my-key";
    const uint8_t value[] = "my-value";
    kafka_producer_FutureRecordMetadata_t *future = NULL;

    kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
        producer, "test-topic", 2, -1,
        key, (int32_t)sizeof(key) - 1,
        value, (int32_t)sizeof(value) - 1,
        &future);
    ASSERT_SUCCESS(err);
    ASSERT(future != NULL, "future should not be null");
    ASSERT(kafka_producer_FutureRecordMetadata_is_done(future),
           "future should be done with auto_complete");

    /* Get metadata */
    kafka_producer_RecordMetadata_t *metadata = NULL;
    err = kafka_producer_FutureRecordMetadata_get(future, &metadata);
    ASSERT_SUCCESS(err);
    ASSERT(metadata != NULL, "metadata should not be null");

    /* Verify getters */
    ASSERT(kafka_producer_RecordMetadata_offset(metadata) == 0,
           "offset should be 0");
    ASSERT(kafka_producer_RecordMetadata_partition(metadata) == 2,
           "partition should be 2");

    const char *returned_topic = kafka_producer_RecordMetadata_topic(metadata);
    ASSERT(returned_topic != NULL, "topic should not be null");
    ASSERT(strcmp(returned_topic, "test-topic") == 0, "topic should match");

    /* MockProducer always returns NO_TIMESTAMP (-1) */
    ASSERT(kafka_producer_RecordMetadata_timestamp(metadata) == -1,
           "timestamp should be -1 (NO_TIMESTAMP from MockProducer)");

    /* History */
    ASSERT(kafka_producer_MockProducer_history_count(producer) == 1,
           "history should have 1 record");

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

static int test_send_null_key(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t value[] = "value-only";
    kafka_producer_FutureRecordMetadata_t *future = NULL;

    kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1,
        value, (int32_t)sizeof(value) - 1,
        &future);
    ASSERT_SUCCESS(err);
    ASSERT(future != NULL, "future should not be null");
    ASSERT(kafka_producer_FutureRecordMetadata_is_done(future),
           "future should be done");

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

static int test_send_null_value(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    const uint8_t key[] = "key-only";
    kafka_producer_FutureRecordMetadata_t *future = NULL;

    kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        key, (int32_t)sizeof(key) - 1,
        NULL, -1,
        &future);
    ASSERT_SUCCESS(err);
    ASSERT(future != NULL, "future should not be null");

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// Multiple sends (incrementing offsets)
// ---------------------------------------------------------------------------

static int test_multiple_sends_incrementing_offsets(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    for (int i = 0; i < 3; i++) {
        kafka_producer_FutureRecordMetadata_t *future = NULL;
        kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
            producer, "topic", 0, -1,
            NULL, -1, NULL, -1, &future);
        ASSERT_SUCCESS(err);

        kafka_producer_RecordMetadata_t *metadata = NULL;
        err = kafka_producer_FutureRecordMetadata_get(future, &metadata);
        ASSERT_SUCCESS(err);
        ASSERT(kafka_producer_RecordMetadata_offset(metadata) == (int64_t)i,
               "offset should increment");

        kafka_producer_RecordMetadata_destroy(metadata);
        kafka_producer_FutureRecordMetadata_destroy(future);
    }

    ASSERT(kafka_producer_MockProducer_history_count(producer) == 3,
           "history should have 3 records");

    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// Manual complete mode
// ---------------------------------------------------------------------------

static int test_manual_complete(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
    kafka_producer_FutureRecordMetadata_t *future = NULL;

    kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &future);
    ASSERT_SUCCESS(err);
    ASSERT(future != NULL, "future should not be null");
    ASSERT(!kafka_producer_FutureRecordMetadata_is_done(future),
           "future should not be done yet");

    /* Complete it */
    ASSERT(kafka_producer_MockProducer_complete_next(producer),
           "complete_next should return true");
    ASSERT(kafka_producer_FutureRecordMetadata_is_done(future),
           "future should be done after complete_next");

    /* Get metadata and verify getters */
    kafka_producer_RecordMetadata_t *metadata = NULL;
    err = kafka_producer_FutureRecordMetadata_get(future, &metadata);
    ASSERT_SUCCESS(err);
    ASSERT(metadata != NULL, "metadata should not be null");

    ASSERT(kafka_producer_RecordMetadata_offset(metadata) == 0,
           "offset should be 0");

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

static int test_manual_error(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
    kafka_producer_FutureRecordMetadata_t *future = NULL;

    kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &future);
    ASSERT_SUCCESS(err);

    /* Complete with error (code 2 = CorruptMessage) */
    ASSERT(kafka_producer_MockProducer_error_next(producer, 2, "test error"),
           "error_next should return true");

    kafka_producer_RecordMetadata_t *metadata = NULL;
    err = kafka_producer_FutureRecordMetadata_get(future, &metadata);
    ASSERT_ERROR(err);
    ASSERT(metadata == NULL, "metadata should be null on error");

    /* Inspect error */
    int32_t code = kafka_common_KafkaError_code(err);
    ASSERT(code == 2, "error code should be 2 (CorruptMessage)");

    const char *msg = kafka_common_KafkaError_message(err);
    ASSERT(msg != NULL, "error message should not be null");

    kafka_common_KafkaError_destroy(err);
    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

static int test_complete_next_no_pending(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
    ASSERT(!kafka_producer_MockProducer_complete_next(producer),
           "complete_next should return false with no pending");
    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// Batch send
// ---------------------------------------------------------------------------

static int test_send_batch(void) {
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
    ASSERT(sent == 2, "both records should succeed");

    for (int i = 0; i < 2; i++) {
        ASSERT(errors[i] == NULL, "error should be null");
        ASSERT(futures[i] != NULL, "future should not be null");
        ASSERT(kafka_producer_FutureRecordMetadata_is_done(futures[i]),
               "future should be done");
        kafka_producer_FutureRecordMetadata_destroy(futures[i]);
    }

    ASSERT(kafka_producer_MockProducer_history_count(producer) == 2,
           "history should have 2 records");

    kafka_producer_Producer_destroy(producer);
    return 0;
}

static int test_send_batch_partial_failure(void) {
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
    ASSERT(sent == 2, "two records should succeed");

    /* Index 0: success */
    ASSERT(futures[0] != NULL, "future[0] should be valid");
    ASSERT(errors[0] == NULL, "error[0] should be null");

    /* Index 1: error */
    ASSERT(futures[1] == NULL, "future[1] should be null");
    ASSERT(errors[1] != NULL, "error[1] should be non-null");

    /* Index 2: success */
    ASSERT(futures[2] != NULL, "future[2] should be valid");
    ASSERT(errors[2] == NULL, "error[2] should be null");

    kafka_producer_FutureRecordMetadata_destroy(futures[0]);
    kafka_common_KafkaError_destroy(errors[1]);
    kafka_producer_FutureRecordMetadata_destroy(futures[2]);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// Close then send
// ---------------------------------------------------------------------------

static int test_close_then_send(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = kafka_producer_Producer_close(producer);
    ASSERT_SUCCESS(err);

    kafka_producer_FutureRecordMetadata_t *future = NULL;
    err = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &future);
    ASSERT_ERROR(err);
    ASSERT(future == NULL, "future should be null after close");

    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

static int test_flush(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(false);
    kafka_producer_FutureRecordMetadata_t *future = NULL;

    kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &future);
    ASSERT_SUCCESS(err);
    ASSERT(!kafka_producer_FutureRecordMetadata_is_done(future),
           "future should not be done before flush");

    err = kafka_producer_Producer_flush(producer);
    ASSERT_SUCCESS(err);
    ASSERT(kafka_producer_FutureRecordMetadata_is_done(future),
           "future should be done after flush");

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// Error inspection
// ---------------------------------------------------------------------------

static int test_error_inspection(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

    kafka_common_KafkaError_t *err = kafka_producer_Producer_close(producer);
    ASSERT_SUCCESS(err);

    kafka_producer_FutureRecordMetadata_t *future = NULL;
    err = kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &future);
    ASSERT_ERROR(err);

    int32_t code = kafka_common_KafkaError_code(err);
    ASSERT(code != 0, "error code should be non-zero");

    const char *msg = kafka_common_KafkaError_message(err);
    ASSERT(msg != NULL, "error message should not be null");
    ASSERT(strlen(msg) > 0, "error message should not be empty");

    /* is_retriable and is_fatal should be callable */
    (void)kafka_common_KafkaError_is_retriable(err);
    (void)kafka_common_KafkaError_is_fatal(err);

    kafka_common_KafkaError_destroy(err);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

static int test_error_null_safety(void) {
    ASSERT(kafka_common_KafkaError_code(NULL) == 0,
           "code(null) should be 0");
    ASSERT(kafka_common_KafkaError_message(NULL) == NULL,
           "message(null) should be null");
    ASSERT(!kafka_common_KafkaError_is_retriable(NULL),
           "is_retriable(null) should be false");
    ASSERT(!kafka_common_KafkaError_is_fatal(NULL),
           "is_fatal(null) should be false");
    kafka_common_KafkaError_destroy(NULL);  /* no-op */
    return 0;
}

// ---------------------------------------------------------------------------
// Mock-specific: clear and history
// ---------------------------------------------------------------------------

static int test_mock_clear(void) {
    kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);
    kafka_producer_FutureRecordMetadata_t *future = NULL;

    kafka_producer_Producer_send(
        producer, "topic", -1, -1,
        NULL, -1, NULL, -1, &future);
    ASSERT(kafka_producer_MockProducer_history_count(producer) == 1,
           "history should have 1 record");

    kafka_producer_MockProducer_clear(producer);
    ASSERT(kafka_producer_MockProducer_history_count(producer) == 0,
           "history should be empty after clear");

    kafka_producer_FutureRecordMetadata_destroy(future);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// RecordMetadata getters from batch send
// ---------------------------------------------------------------------------

static int test_record_metadata_getters_from_batch(void) {
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
    ASSERT(sent == 1, "record should succeed");
    ASSERT(errors[0] == NULL, "error should be null");

    kafka_producer_RecordMetadata_t *metadata = NULL;
    kafka_common_KafkaError_t *err = kafka_producer_FutureRecordMetadata_get(
        futures[0], &metadata);
    ASSERT_SUCCESS(err);
    ASSERT(metadata != NULL, "metadata should not be null");

    /* Verify all getters */
    ASSERT(kafka_producer_RecordMetadata_offset(metadata) == 0,
           "offset should be 0");
    ASSERT(kafka_producer_RecordMetadata_partition(metadata) == 7,
           "partition should be 7");

    const char *topic = kafka_producer_RecordMetadata_topic(metadata);
    ASSERT(topic != NULL, "topic should not be null");
    ASSERT(strcmp(topic, "getter-topic") == 0, "topic should match");

    ASSERT(kafka_producer_RecordMetadata_timestamp(metadata) == -1,
           "timestamp should be -1 (NO_TIMESTAMP from MockProducer)");

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(futures[0]);
    kafka_producer_Producer_destroy(producer);
    return 0;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(void) {
    printf("confluent-kafka-c: MockProducer test suite\n");
    printf("==========================================\n\n");

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

    printf("\n==========================================\n");
    printf("%d/%d tests passed\n", tests_passed, tests_run);

    return tests_passed == tests_run ? 0 : 1;
}
