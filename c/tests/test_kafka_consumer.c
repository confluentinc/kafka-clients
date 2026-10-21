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

// KafkaConsumer through the C API, without a broker: the bootstrap server is
// unreachable, so only construction, configuration, subscription and the
// wakeup path are exercised. `kafka_consumer_KafkaConsumer_new` hands back an
// OWNED `kafka_consumer_KafkaConsumer_t`, freed with
// `kafka_consumer_KafkaConsumer_destroy`; the `Consumer` operations take its
// BORROWED `kafka_consumer_KafkaConsumer__as_Consumer` view.

#include <confluent_kafka.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include "unity.h"
#include "test_support.h"

void setUp(void) {}
void tearDown(void) {}

static void sleep_ms(long ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* A `ConsumerConfig` from the three properties every test needs; the C-built
 * map borrows the strings and is freed here. */
static kafka_consumer_ConsumerConfig_t *create_config(const char *group_protocol) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"localhost:9092");
    kafka_Map_put(props, (void *)"group.id", (void *)"test-group");
    kafka_Map_put(props, (void *)"group.protocol", (void *)group_protocol);
    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerConfig_t *config = kafka_consumer_ConsumerConfig_new(props, &err);
    kafka_Map_destroy(props);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(config);
    return config;
}

/* A KIP-848 consumer with no deserializers: records would carry
 * `kafka_Bytes_t *` keys and values. */
static kafka_consumer_KafkaConsumer_t *create_consumer(void) {
    kafka_consumer_ConsumerConfig_t *config = create_config("consumer");
    kafka_common_Error_t *err = NULL;
    kafka_consumer_KafkaConsumer_t *kc = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &err);
    kafka_consumer_ConsumerConfig_destroy(config); /* copied by the constructor */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(kc);
    return kc;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

static void test_kafka_consumer_new_succeeds(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_consumer();
    kafka_consumer_KafkaConsumer_destroy(kc);
}

// The map is the config's only input, as in Java: a missing `bootstrap.servers`
// is the `ConfigException`, and a `group.id` in the map reaches the consumer.
static void test_kafka_consumer_new_from_map(void) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_common_Error_t *err = NULL;
    kafka_consumer_ConsumerConfig_t *config = kafka_consumer_ConsumerConfig_new(props, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(config);
    TEST_ASSERT_EQUAL_STRING("Missing required configuration \"bootstrap.servers\" which has no default value.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    err = NULL;
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"localhost:9092");
    kafka_Map_put(props, (void *)"group.id", (void *)"map-group");
    kafka_Map_put(props, (void *)"group.protocol", (void *)"consumer");
    config = kafka_consumer_ConsumerConfig_new(props, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(config);
    kafka_Map_destroy(props);

    kafka_consumer_KafkaConsumer_t *kc = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &err);
    TEST_ASSERT_NULL(err);
    kafka_consumer_ConsumerConfig_destroy(config);
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
    kafka_consumer_ConsumerGroupMetadata_t *metadata = kafka_consumer_Consumer_group_metadata(consumer, &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_STRING("map-group", kafka_consumer_ConsumerGroupMetadata_group_id(metadata));
    kafka_consumer_ConsumerGroupMetadata_destroy(metadata);
    kafka_consumer_KafkaConsumer_destroy(kc);
}

// Deserializer interfaces are passed as borrowed `Deserializer_t` views of
// the concrete class handles, which the constructor takes over.
static void test_kafka_consumer_new_with_string_deserializers(void) {
    kafka_consumer_ConsumerConfig_t *config = create_config("consumer");
    kafka_common_serialization_StringDeserializer_t *key = kafka_common_serialization_StringDeserializer_new();
    kafka_common_serialization_StringDeserializer_t *value = kafka_common_serialization_StringDeserializer_new();
    kafka_common_Error_t *err = NULL;
    kafka_consumer_KafkaConsumer_t *kc =
        kafka_consumer_KafkaConsumer_new(config, kafka_common_serialization_StringDeserializer__as_Deserializer(key),
                                         kafka_common_serialization_StringDeserializer__as_Deserializer(value), &err);
    TEST_ASSERT_NULL(err);
    kafka_consumer_ConsumerConfig_destroy(config);
    TEST_ASSERT_NOT_NULL(kc);
    kafka_consumer_KafkaConsumer_destroy(kc);
    kafka_common_serialization_StringDeserializer_destroy(key);
    kafka_common_serialization_StringDeserializer_destroy(value);
}

// The classic group protocol is not supported by this client (see
// consumer-threading.md §20): the constructor fails with UnsupportedVersion.
static void test_kafka_consumer_classic_protocol_rejected(void) {
    kafka_consumer_ConsumerConfig_t *config = create_config("classic");
    kafka_common_Error_t *err = NULL;
    kafka_consumer_KafkaConsumer_t *kc = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &err);
    kafka_consumer_ConsumerConfig_destroy(config);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(kc); /* the failure value */
    TEST_ASSERT_TRUE(kafka_common_Error_is_unsupported_version_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "Classic group protocol"));
    kafka_common_Error_destroy(err);
    err = NULL;

    /* GroupProtocol is a rule-3 enum: `of` round-trips both names. */
    const kafka_consumer_GroupProtocol_t *protocol = kafka_consumer_GroupProtocol_of("classic", &err);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_PTR(kafka_consumer_GroupProtocol_classic(), protocol);
    TEST_ASSERT_EQUAL_INT(kafka_consumer_GroupProtocol_CLASSIC, kafka_consumer_GroupProtocol__enum(protocol));
    protocol = kafka_consumer_GroupProtocol_of("CONSUMER", &err); /* case-insensitive, as Java */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_EQUAL_PTR(kafka_consumer_GroupProtocol_consumer(), protocol);
    TEST_ASSERT_EQUAL_STRING("CONSUMER", kafka_consumer_GroupProtocol_name(protocol));
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

static void test_kafka_consumer_subscribe(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_consumer();
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_subscribe_with_topics(consumer, topics, &err);
    TEST_ASSERT_NULL(err);
    kafka_List_destroy(topics);

    kafka_List_t *subs = consumer_subscription(consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(subs));
    TEST_ASSERT_EQUAL_STRING("test-topic", (const char *)kafka_List_get(subs, 0));
    kafka_List_destroy(subs);
    kafka_consumer_KafkaConsumer_destroy(kc);
}

// ---------------------------------------------------------------------------
// Wakeup
// ---------------------------------------------------------------------------

// wakeup() before poll() makes that poll return Wakeup at once.
static void test_kafka_consumer_wakeup_before_poll(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_consumer();
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_subscribe_with_topics(consumer, topics, &err);
    TEST_ASSERT_NULL(err);
    kafka_List_destroy(topics);

    kafka_consumer_Consumer_wakeup(consumer);
    kafka_consumer_ConsumerRecords_t *records = kafka_consumer_Consumer_poll(consumer, 1000, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);
    kafka_consumer_KafkaConsumer_destroy(kc);
}

static void *wakeup_after_delay(void *arg) {
    sleep_ms(200);
    kafka_consumer_Consumer_wakeup((kafka_consumer_Consumer_t *)arg);
    return NULL;
}

// wakeup() from another thread interrupts a blocking poll (the broker is
// unreachable, so the poll would otherwise run out its 10s timeout).
static void test_kafka_consumer_wakeup_from_other_thread(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_consumer();
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_subscribe_with_topics(consumer, topics, &err);
    TEST_ASSERT_NULL(err);
    kafka_List_destroy(topics);

    pthread_t thread;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, NULL, wakeup_after_delay, consumer));
    kafka_consumer_ConsumerRecords_t *records = kafka_consumer_Consumer_poll(consumer, 10000, &err);
    pthread_join(thread, NULL);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);
    kafka_consumer_KafkaConsumer_destroy(kc);
}

typedef struct {
    atomic_int fired;
    kafka_common_Error_t *error;
    pthread_t thread;
} poll_completion_t;

static void on_poll(kafka_consumer_ConsumerRecords_t *records, kafka_common_Error_t *error, void *opaque) {
    poll_completion_t *c = (poll_completion_t *)opaque;
    kafka_consumer_ConsumerRecords_destroy(records); /* NULL on failure */
    c->error = error;
    c->thread = pthread_self();
    atomic_fetch_add(&c->fired, 1);
}

// A `poll_cb` runs the poll off the calling thread; `wakeup()` interrupts
// it and its completion is queued for the pump, on this thread.
static void test_kafka_consumer_poll_cb_interrupted_by_wakeup(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_consumer();
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
    callback_pump_t pump;
    consumer_callback_pump_install(&pump, consumer);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_subscribe_with_topics(consumer, topics, &err);
    TEST_ASSERT_NULL(err);
    kafka_List_destroy(topics);

    poll_completion_t completion;
    memset(&completion, 0, sizeof(completion));
    atomic_init(&completion.fired, 0);
    kafka_consumer_Consumer_poll_cb(consumer, 10000, on_poll, &completion);

    /* In flight: a blocking call is Java's ConcurrentModificationException. */
    sleep_ms(100);
    kafka_consumer_Consumer_commit_sync(consumer, &err);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_TRUE(kafka_common_Error_is_local_concurrent_modification_error(err));
    TEST_ASSERT_EQUAL_STRING("KafkaConsumer is not safe for multi-threaded access.", kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired));

    kafka_consumer_Consumer_wakeup(consumer); /* always allowed */
    TEST_ASSERT_TRUE(callback_pump_until(&pump, &completion.fired, 1));
    TEST_ASSERT_NOT_NULL(completion.error);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(completion.error));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), completion.thread));
    kafka_common_Error_destroy(completion.error);

    /* Free again. */
    kafka_List_t *subs = consumer_subscription(consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(subs));
    kafka_List_destroy(subs);
    kafka_consumer_KafkaConsumer_destroy(kc);
    callback_pump_destroy(&pump);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// destroy() without close() releases the consumer without hanging.
static void test_kafka_consumer_destroy_without_close(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_consumer();
    kafka_consumer_KafkaConsumer_destroy(kc);
}

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

// destroy() wakes a `poll_cb` with a long timeout up rather than waiting it
// out: the poll fails with the wakeup error and its completion, still queued,
// runs inside destroy, exactly once.
static void test_kafka_consumer_destroy_wakes_a_waiting_poll_cb(void) {
    kafka_consumer_KafkaConsumer_t *kc = create_consumer();
    kafka_consumer_Consumer_t *consumer = kafka_consumer_KafkaConsumer__as_Consumer(kc);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    kafka_common_Error_t *err = NULL;
    kafka_consumer_Consumer_subscribe_with_topics(consumer, topics, &err);
    TEST_ASSERT_NULL(err);
    kafka_List_destroy(topics);

    poll_completion_t completion;
    memset(&completion, 0, sizeof(completion));
    atomic_init(&completion.fired, 0);
    kafka_consumer_Consumer_poll_cb(consumer, 60000, on_poll, &completion);
    sleep_ms(100);
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&completion.fired));

    long started = now_ms();
    kafka_consumer_KafkaConsumer_destroy(kc);
    TEST_ASSERT_TRUE(now_ms() - started < 10000);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&completion.fired));
    TEST_ASSERT_NOT_NULL(completion.error);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(completion.error));
    kafka_common_Error_destroy(completion.error);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_kafka_consumer_new_succeeds);
    RUN_TEST(test_kafka_consumer_new_from_map);
    RUN_TEST(test_kafka_consumer_new_with_string_deserializers);
    RUN_TEST(test_kafka_consumer_classic_protocol_rejected);
    RUN_TEST(test_kafka_consumer_subscribe);
    RUN_TEST(test_kafka_consumer_wakeup_before_poll);
    RUN_TEST(test_kafka_consumer_wakeup_from_other_thread);
    RUN_TEST(test_kafka_consumer_poll_cb_interrupted_by_wakeup);
    RUN_TEST(test_kafka_consumer_destroy_without_close);
    RUN_TEST(test_kafka_consumer_destroy_wakes_a_waiting_poll_cb);
    return UNITY_END();
}
