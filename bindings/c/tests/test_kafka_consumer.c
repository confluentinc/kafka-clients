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

// End-to-end smoke tests for the real (broker-backed) KafkaConsumer C FFI.
//
// These run without a reachable broker: KafkaConsumer construction only spawns
// the background task (it does not connect synchronously), subscribe just
// records intent, and a wakeup-interrupted poll returns promptly. They mirror
// the no-broker handling pattern of test_kafka_producer.c (localhost:9092 is
// expected to be unreachable; we never assert a successful round-trip).

#include <confluent_kafka.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

/* Helper: build ConsumerProperties for the new (KIP-848) group protocol. */
static kafka_consumer_Consumer_t *create_consumer(const char *bootstrap,
                                                  const char *group_id,
                                                  const char *group_protocol,
                                                  kafka_common_Error_t **out_err) {
    const char *configs[] = {
        "bootstrap.servers", bootstrap,
        "group.id",          group_id,
        "group.protocol",    group_protocol,
        NULL
    };
    kafka_consumer_ConsumerProperties_t *props =
        kafka_consumer_ConsumerProperties_from_configs(configs);
    kafka_consumer_Consumer_t *consumer =
        kafka_consumer_KafkaConsumer_new(props, out_err);
    kafka_consumer_ConsumerProperties_destroy(props);
    return consumer;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

void test_kafka_consumer_new_succeeds(void) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_t *consumer =
        create_consumer("localhost:9092", "test-group", "consumer", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(consumer);

    kafka_consumer_Consumer_destroy(consumer);
}

void test_kafka_consumer_new_via_put(void) {
    kafka_consumer_ConsumerProperties_t *props =
        kafka_consumer_ConsumerProperties_new();
    TEST_ASSERT_NOT_NULL(props);
    kafka_consumer_ConsumerProperties_put(props, "bootstrap.servers", "localhost:9092");
    kafka_consumer_ConsumerProperties_put(props, "group.id", "test-group");
    kafka_consumer_ConsumerProperties_put(props, "group.protocol", "consumer");

    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_t *consumer =
        kafka_consumer_KafkaConsumer_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(consumer);

    kafka_consumer_ConsumerProperties_destroy(props);
    kafka_consumer_Consumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// Classic-protocol rejection (Java parity: unsupported_version)
// ---------------------------------------------------------------------------

void test_kafka_consumer_classic_protocol_rejected(void) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_t *consumer =
        create_consumer("localhost:9092", "test-group", "classic", &err);

    /* Construction must fail: classic protocol is not supported (KIP-848 only). */
    TEST_ASSERT_NULL(consumer);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_UNSUPPORTED_VERSION,
                            kafka_common_Error_code(err));
    kafka_common_Error_destroy(err);
}

// ---------------------------------------------------------------------------
// Subscribe
// ---------------------------------------------------------------------------

void test_kafka_consumer_subscribe(void) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_t *consumer =
        create_consumer("localhost:9092", "test-group", "consumer", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(consumer);

    const char *topics[] = { "test-topic" };
    kafka_common_Error_t *sub_err =
        kafka_consumer_Consumer_subscribe(consumer, topics, 1);
    TEST_ASSERT_NULL(sub_err);

    kafka_consumer_Consumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// wakeup-interrupted poll (no reachable broker: must return cleanly, no hang)
// ---------------------------------------------------------------------------

void test_kafka_consumer_wakeup_before_poll(void) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_t *consumer =
        create_consumer("localhost:9092", "test-group", "consumer", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(consumer);

    const char *topics[] = { "test-topic" };
    kafka_consumer_Consumer_subscribe(consumer, topics, 1);

    /* Pre-arm the wakeup so the next poll returns immediately with Wakeup
       rather than blocking on the (unreachable) broker for the full timeout. */
    kafka_consumer_Consumer_wakeup(consumer);

    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_Consumer_poll(consumer, 5000, &poll_err);

    /* On a pre-armed wakeup, poll returns no records and exactly the Wakeup
       error (well before the 5s timeout), never a connection-related failure
       against the unreachable broker. */
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_NOT_NULL(poll_err);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_WAKEUP,
                            kafka_common_Error_code(poll_err));
    kafka_common_Error_destroy(poll_err);

    kafka_consumer_Consumer_destroy(consumer);
}

/* Thread body: sleep briefly, then wakeup the consumer from another thread. */
typedef struct {
    kafka_consumer_Consumer_t *consumer;
} wakeup_arg_t;

static void *wakeup_thread_body(void *arg) {
    wakeup_arg_t *wa = (wakeup_arg_t *)arg;
    struct timespec ts = { 0, 200 * 1000 * 1000 }; /* 200 ms */
    nanosleep(&ts, NULL);
    kafka_consumer_Consumer_wakeup(wa->consumer);
    return NULL;
}

void test_kafka_consumer_wakeup_from_other_thread(void) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_t *consumer =
        create_consumer("localhost:9092", "test-group", "consumer", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(consumer);

    const char *topics[] = { "test-topic" };
    kafka_consumer_Consumer_subscribe(consumer, topics, 1);

    /* Spawn a thread that wakes us up after 200ms. wakeup() bypasses the
       access guard, so it interrupts an in-flight poll held by this thread. */
    wakeup_arg_t wa = { consumer };
    pthread_t tid;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&tid, NULL, wakeup_thread_body, &wa));

    /* Poll with a long timeout; the wakeup from the other thread must cut it
       short well before 30s, returning cleanly (no hang, no crash). */
    kafka_common_Error_t *poll_err = NULL;
    kafka_consumer_ConsumerRecords_t *records =
        kafka_consumer_Consumer_poll(consumer, 30000, &poll_err);

    pthread_join(tid, NULL);

    /* The wakeup from the other thread is what cut the poll short, so the
       error is exactly Wakeup -- not a timeout or a connection failure. */
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_NOT_NULL(poll_err);
    TEST_ASSERT_EQUAL_INT32(kafka_common_ErrorCode_WAKEUP,
                            kafka_common_Error_code(poll_err));
    kafka_common_Error_destroy(poll_err);

    kafka_consumer_Consumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// Destroy without explicit close
// ---------------------------------------------------------------------------

void test_kafka_consumer_destroy_without_close(void) {
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_t *consumer =
        create_consumer("localhost:9092", "test-group", "consumer", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(consumer);

    /* Destroy without close -- Drop impl handles cleanup. */
    kafka_consumer_Consumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(void) {
    UNITY_BEGIN();

    RUN_TEST(test_kafka_consumer_new_succeeds);
    RUN_TEST(test_kafka_consumer_new_via_put);
    RUN_TEST(test_kafka_consumer_classic_protocol_rejected);
    RUN_TEST(test_kafka_consumer_subscribe);
    RUN_TEST(test_kafka_consumer_wakeup_before_poll);
    RUN_TEST(test_kafka_consumer_wakeup_from_other_thread);
    RUN_TEST(test_kafka_consumer_destroy_without_close);

    return UNITY_END();
}
