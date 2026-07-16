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

// Smoke test for the KIP-932 share-consumer C ABI, driving a MockShareConsumer
// (broker-less) through the whole subscribe -> poll -> acknowledge -> commit ->
// close flow, plus the registered acknowledgement-commit callback register/clear
// path and the acquisition-lock-timeout getter.

#include <confluent_kafka.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void test_create_and_destroy(void) {
    kafka_consumer_ShareConsumer_t *consumer = kafka_consumer_MockShareConsumer_new();
    TEST_ASSERT_NOT_NULL(consumer);
    kafka_consumer_ShareConsumer_destroy(consumer);
}

void test_destroy_null(void) {
    kafka_consumer_ShareConsumer_destroy(NULL);
}

// ---------------------------------------------------------------------------
// Subscribe / subscription
// ---------------------------------------------------------------------------

void test_subscribe_and_read_subscription(void) {
    kafka_consumer_ShareConsumer_t *consumer = kafka_consumer_MockShareConsumer_new();
    TEST_ASSERT_NOT_NULL(consumer);

    const char *topics[] = {"share-topic"};
    kafka_common_KafkaError_t *err = kafka_consumer_ShareConsumer_subscribe(consumer, topics, 1);
    TEST_ASSERT_NULL(err);

    kafka_common_KafkaError_t *sub_err = NULL;
    kafka_consumer_StringList_t *sub = kafka_consumer_ShareConsumer_subscription(consumer, &sub_err);
    TEST_ASSERT_NULL(sub_err);
    TEST_ASSERT_NOT_NULL(sub);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_StringList_count(sub));
    TEST_ASSERT_EQUAL_STRING("share-topic", kafka_consumer_StringList_get(sub, 0));
    kafka_consumer_StringList_destroy(sub);

    kafka_consumer_ShareConsumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// Full flow: subscribe -> add_record -> poll -> read -> acknowledge ->
// commit_sync -> close -> destroy
// ---------------------------------------------------------------------------

void test_subscribe_poll_acknowledge_commit_close(void) {
    kafka_consumer_ShareConsumer_t *consumer = kafka_consumer_MockShareConsumer_new();
    TEST_ASSERT_NOT_NULL(consumer);

    const char *topics[] = {"share-topic"};
    kafka_common_KafkaError_t *err = kafka_consumer_ShareConsumer_subscribe(consumer, topics, 1);
    TEST_ASSERT_NULL(err);

    // Seed one record on the subscribed topic-partition.
    const uint8_t key[] = "k1";
    const uint8_t value[] = "v1";
    kafka_common_KafkaError_t *add_err = NULL;
    kafka_consumer_MockShareConsumer_add_record(
        consumer, "share-topic", 0,
        key, (int32_t)sizeof(key) - 1,
        value, (int32_t)sizeof(value) - 1,
        7, &add_err);
    TEST_ASSERT_NULL(add_err);

    // Poll it back.
    kafka_common_KafkaError_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_ShareConsumer_poll(consumer, 0, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    TEST_ASSERT_NOT_NULL(records);
    TEST_ASSERT_EQUAL_INT32(1, kafka_consumer_ConsumerRecords_count(records));

    const kafka_consumer_ConsumerRecord_t *rec = kafka_consumer_ConsumerRecords_get(records, 0);
    TEST_ASSERT_NOT_NULL(rec);
    TEST_ASSERT_EQUAL_INT64(7, kafka_consumer_ConsumerRecord_offset(rec));

    int32_t key_len = 0;
    const uint8_t *key_ptr = kafka_consumer_ConsumerRecord_key(rec, &key_len);
    TEST_ASSERT_EQUAL_INT32(2, key_len);
    TEST_ASSERT_EQUAL_INT8_ARRAY(key, key_ptr, 2);

    int32_t value_len = 0;
    const uint8_t *value_ptr = kafka_consumer_ConsumerRecord_value(rec, &value_len);
    TEST_ASSERT_EQUAL_INT32(2, value_len);
    TEST_ASSERT_EQUAL_INT8_ARRAY(value, value_ptr, 2);

    // Acknowledge (accept) the polled record; the mock accepts unconditionally.
    kafka_common_KafkaError_t *ack_err = kafka_consumer_ShareConsumer_acknowledge(consumer, rec);
    TEST_ASSERT_NULL(ack_err);

    // The record pointer stays valid until the batch is destroyed.
    kafka_consumer_ConsumerRecords_destroy(records);

    // commit_sync returns a (possibly empty) per-partition outcome map.
    kafka_common_KafkaError_t *commit_err = NULL;
    kafka_consumer_ShareCommitResult_t *result =
        kafka_consumer_ShareConsumer_commit_sync(consumer, &commit_err);
    TEST_ASSERT_NULL(commit_err);
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT32(0, kafka_consumer_ShareCommitResult_count(result));
    kafka_consumer_ShareCommitResult_destroy(result);

    // Close, then destroy.
    kafka_common_KafkaError_t *close_err = kafka_consumer_ShareConsumer_close(consumer);
    TEST_ASSERT_NULL(close_err);

    kafka_consumer_ShareConsumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// acknowledge_by_offset + acknowledge_with_type
// ---------------------------------------------------------------------------

void test_acknowledge_variants(void) {
    kafka_consumer_ShareConsumer_t *consumer = kafka_consumer_MockShareConsumer_new();
    const char *topics[] = {"share-topic"};
    TEST_ASSERT_NULL(kafka_consumer_ShareConsumer_subscribe(consumer, topics, 1));

    const uint8_t value[] = "v";
    kafka_common_KafkaError_t *add_err = NULL;
    kafka_consumer_MockShareConsumer_add_record(
        consumer, "share-topic", 0, NULL, -1, value, 1, 3, &add_err);
    TEST_ASSERT_NULL(add_err);

    kafka_common_KafkaError_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_ShareConsumer_poll(consumer, 0, &poll_err);
    TEST_ASSERT_NULL(poll_err);
    const kafka_consumer_ConsumerRecord_t *rec = kafka_consumer_ConsumerRecords_get(records, 0);

    TEST_ASSERT_NULL(kafka_consumer_ShareConsumer_acknowledge_with_type(
        consumer, rec, kafka_consumer_AcknowledgeType_t_RELEASE));
    kafka_consumer_ConsumerRecords_destroy(records);

    TEST_ASSERT_NULL(kafka_consumer_ShareConsumer_acknowledge_by_offset(
        consumer, "share-topic", 0, 3, kafka_consumer_AcknowledgeType_t_REJECT));

    kafka_consumer_ShareConsumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// Registered acknowledgement-commit callback register / clear
// ---------------------------------------------------------------------------

// The mock never fires this callback (its setter is a no-op), so the body only
// needs to free the owned handles it would receive; this test exercises the
// register-then-clear path over the ABI.
static void on_ack_commit(const kafka_consumer_ShareAcknowledgeOffsets_t *offsets,
                          const kafka_common_KafkaError_t *error,
                          void *user_data) {
    (void)user_data;
    kafka_consumer_ShareAcknowledgeOffsets_destroy(
        (kafka_consumer_ShareAcknowledgeOffsets_t *)offsets);
    if (error != NULL) {
        kafka_common_KafkaError_destroy((kafka_common_KafkaError_t *)error);
    }
}

void test_set_and_clear_ack_commit_callback(void) {
    kafka_consumer_ShareConsumer_t *consumer = kafka_consumer_MockShareConsumer_new();

    kafka_common_KafkaError_t *set_err =
        kafka_consumer_ShareConsumer_set_acknowledgement_commit_callback(consumer, on_ack_commit, NULL);
    TEST_ASSERT_NULL(set_err);

    kafka_common_KafkaError_t *clear_err =
        kafka_consumer_ShareConsumer_set_acknowledgement_commit_callback(consumer, NULL, NULL);
    TEST_ASSERT_NULL(clear_err);

    kafka_consumer_ShareConsumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// acquisition_lock_timeout_ms
// ---------------------------------------------------------------------------

void test_acquisition_lock_timeout_absent(void) {
    kafka_consumer_ShareConsumer_t *consumer = kafka_consumer_MockShareConsumer_new();

    int32_t out_ms = -999;
    kafka_common_KafkaError_t *lock_err = NULL;
    bool present = kafka_consumer_ShareConsumer_acquisition_lock_timeout_ms(consumer, &out_ms, &lock_err);
    TEST_ASSERT_FALSE(present);
    TEST_ASSERT_NULL(lock_err);
    TEST_ASSERT_EQUAL_INT32(-999, out_ms); // untouched when absent

    kafka_consumer_ShareConsumer_destroy(consumer);
}

// ---------------------------------------------------------------------------

int main(void) {
    UNITY_BEGIN();

    RUN_TEST(test_create_and_destroy);
    RUN_TEST(test_destroy_null);
    RUN_TEST(test_subscribe_and_read_subscription);
    RUN_TEST(test_subscribe_poll_acknowledge_commit_close);
    RUN_TEST(test_acknowledge_variants);
    RUN_TEST(test_set_and_clear_ack_commit_callback);
    RUN_TEST(test_acquisition_lock_timeout_absent);

    return UNITY_END();
}
