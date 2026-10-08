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
// OWNED `kafka_consumer_Consumer_t` (Java's constructor returns the delegate),
// freed with `kafka_consumer_Consumer_destroy`.

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
    kafka_consumer_ConsumerConfig_t *config = NULL;
    kafka_common_Error_t *err = kafka_consumer_ConsumerConfig_new(props, &config);
    kafka_Map_destroy(props);
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(config);
    return config;
}

/* A KIP-848 consumer with no deserializers: records would carry
 * `kafka_Bytes_t *` keys and values. */
static kafka_consumer_Consumer_t *create_consumer(void) {
    kafka_consumer_ConsumerConfig_t *config = create_config("consumer");
    kafka_consumer_Consumer_t *consumer = NULL;
    kafka_common_Error_t *err = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &consumer);
    kafka_consumer_ConsumerConfig_destroy(config); /* copied by the constructor */
    TEST_ASSERT_NULL(err);
    TEST_ASSERT_NOT_NULL(consumer);
    return consumer;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

static void test_kafka_consumer_new_succeeds(void) {
    kafka_consumer_Consumer_t *consumer = create_consumer();
    TEST_ASSERT_NOT_NULL(kafka_consumer_Consumer_client_id(consumer));
    kafka_consumer_Consumer_destroy(consumer);
}

// The config's setters are the typed twins of the property map. Only
// `bootstrap.servers` has no default, so it alone must come from the map.
static void test_kafka_consumer_new_via_setters(void) {
    kafka_Map_t *props = kafka_Map_new();
    kafka_consumer_ConsumerConfig_t *config = NULL;
    kafka_common_Error_t *err = kafka_consumer_ConsumerConfig_new(props, &config);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(config);
    TEST_ASSERT_EQUAL_STRING("Missing required configuration \"bootstrap.servers\" which has no default value.",
                             kafka_common_Error_message(err));
    kafka_common_Error_destroy(err);
    kafka_Map_put(props, (void *)"bootstrap.servers", (void *)"ignored:1");
    TEST_ASSERT_NULL(kafka_consumer_ConsumerConfig_new(props, &config));
    kafka_Map_destroy(props);

    kafka_List_t *servers = kafka_List_new();
    kafka_List_add(servers, (void *)"localhost:9092");
    kafka_consumer_ConsumerConfig_set_bootstrap_servers(config, servers);
    kafka_List_destroy(servers);
    kafka_consumer_ConsumerConfig_set_group_id(config, "setter-group");
    kafka_consumer_ConsumerConfig_set_group_protocol(config, "consumer");
    kafka_consumer_ConsumerConfig_set_client_id(config, "setter-client");

    TEST_ASSERT_EQUAL_STRING("setter-group", kafka_consumer_ConsumerConfig_group_id(config));
    TEST_ASSERT_EQUAL_STRING("consumer", kafka_consumer_ConsumerConfig_group_protocol(config));
    TEST_ASSERT_EQUAL_STRING("setter-client", kafka_consumer_ConsumerConfig_client_id(config));
    kafka_List_t *got = kafka_consumer_ConsumerConfig_bootstrap_servers(config); /* owned list of owned strings */
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(got));
    TEST_ASSERT_EQUAL_STRING("localhost:9092", (const char *)kafka_List_get(got, 0));
    kafka_List_destroy(got);

    kafka_consumer_Consumer_t *consumer = NULL;
    TEST_ASSERT_NULL(kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &consumer));
    kafka_consumer_ConsumerConfig_destroy(config);
    TEST_ASSERT_EQUAL_STRING("setter-client", kafka_consumer_Consumer_client_id(consumer));
    kafka_consumer_Consumer_destroy(consumer);
}

// Deserializer interfaces are passed as borrowed `Deserializer_t` views of
// the concrete class handles, which the constructor takes over.
static void test_kafka_consumer_new_with_string_deserializers(void) {
    kafka_consumer_ConsumerConfig_t *config = create_config("consumer");
    kafka_common_serialization_StringDeserializer_t *key = kafka_common_serialization_StringDeserializer_new();
    kafka_common_serialization_StringDeserializer_t *value = kafka_common_serialization_StringDeserializer_new();
    kafka_consumer_Consumer_t *consumer = NULL;
    TEST_ASSERT_NULL(kafka_consumer_KafkaConsumer_new(config,
                                                      kafka_common_serialization_StringDeserializer__as_Deserializer(key),
                                                      kafka_common_serialization_StringDeserializer__as_Deserializer(value),
                                                      &consumer));
    kafka_consumer_ConsumerConfig_destroy(config);
    TEST_ASSERT_NOT_NULL(consumer);
    kafka_consumer_Consumer_destroy(consumer);
    kafka_common_serialization_StringDeserializer_destroy(key);
    kafka_common_serialization_StringDeserializer_destroy(value);
}

// The classic group protocol is not supported by this client (see
// consumer-threading.md §20): the constructor fails with UnsupportedVersion.
static void test_kafka_consumer_classic_protocol_rejected(void) {
    kafka_consumer_ConsumerConfig_t *config = create_config("classic");
    kafka_consumer_Consumer_t *consumer = (kafka_consumer_Consumer_t *)&config; /* sentinel */
    kafka_common_Error_t *err = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &consumer);
    kafka_consumer_ConsumerConfig_destroy(config);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_EQUAL_PTR(&config, consumer); /* untouched on failure */
    TEST_ASSERT_TRUE(kafka_common_Error_is_unsupported_version_error(err));
    TEST_ASSERT_NOT_NULL(strstr(kafka_common_Error_message(err), "Classic group protocol"));
    kafka_common_Error_destroy(err);

    /* GroupProtocol is a rule-3 enum: `of` round-trips both names. */
    const kafka_consumer_GroupProtocol_t *protocol = NULL;
    TEST_ASSERT_NULL(kafka_consumer_GroupProtocol_of("classic", &protocol));
    TEST_ASSERT_EQUAL_PTR(kafka_consumer_GroupProtocol_classic(), protocol);
    TEST_ASSERT_EQUAL_INT(kafka_consumer_GroupProtocol_CLASSIC, kafka_consumer_GroupProtocol__enum(protocol));
    TEST_ASSERT_NULL(kafka_consumer_GroupProtocol_of("CONSUMER", &protocol)); /* case-insensitive, as Java */
    TEST_ASSERT_EQUAL_PTR(kafka_consumer_GroupProtocol_consumer(), protocol);
    TEST_ASSERT_EQUAL_STRING("CONSUMER", kafka_consumer_GroupProtocol_name(protocol));
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

static void test_kafka_consumer_subscribe(void) {
    kafka_consumer_Consumer_t *consumer = create_consumer();
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_topics(consumer, topics));
    kafka_List_destroy(topics);

    kafka_List_t *subs = kafka_consumer_Consumer_subscription(consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(subs));
    TEST_ASSERT_EQUAL_STRING("test-topic", (const char *)kafka_List_get(subs, 0));
    kafka_List_destroy(subs);
    kafka_consumer_Consumer_destroy(consumer);
}

// ---------------------------------------------------------------------------
// Wakeup
// ---------------------------------------------------------------------------

// wakeup() before poll() makes that poll return Wakeup at once.
static void test_kafka_consumer_wakeup_before_poll(void) {
    kafka_consumer_Consumer_t *consumer = create_consumer();
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_topics(consumer, topics));
    kafka_List_destroy(topics);

    kafka_consumer_Consumer_wakeup(consumer);
    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_poll(consumer, 1000, &records);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);
    kafka_consumer_Consumer_destroy(consumer);
}

static void *wakeup_after_delay(void *arg) {
    sleep_ms(200);
    kafka_consumer_Consumer_wakeup((kafka_consumer_Consumer_t *)arg);
    return NULL;
}

// wakeup() from another thread interrupts a blocking poll (the broker is
// unreachable, so the poll would otherwise run out its 10s timeout).
static void test_kafka_consumer_wakeup_from_other_thread(void) {
    kafka_consumer_Consumer_t *consumer = create_consumer();
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_topics(consumer, topics));
    kafka_List_destroy(topics);

    pthread_t thread;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, NULL, wakeup_after_delay, consumer));
    kafka_consumer_ConsumerRecords_t *records = NULL;
    kafka_common_Error_t *err = kafka_consumer_Consumer_poll(consumer, 10000, &records);
    pthread_join(thread, NULL);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT_NULL(records);
    TEST_ASSERT_TRUE(kafka_common_Error_is_wakeup_error(err));
    kafka_common_Error_destroy(err);
    kafka_consumer_Consumer_destroy(consumer);
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
    kafka_consumer_Consumer_t *consumer = create_consumer();
    callback_pump_t pump;
    consumer_callback_pump_install(&pump, consumer);
    kafka_List_t *topics = kafka_List_new();
    kafka_List_add(topics, (void *)"test-topic");
    TEST_ASSERT_NULL(kafka_consumer_Consumer_subscribe_with_topics(consumer, topics));
    kafka_List_destroy(topics);

    poll_completion_t completion;
    memset(&completion, 0, sizeof(completion));
    atomic_init(&completion.fired, 0);
    kafka_consumer_Consumer_poll_cb(consumer, 10000, on_poll, &completion);

    /* In flight: a blocking call is Java's ConcurrentModificationException. */
    sleep_ms(100);
    kafka_common_Error_t *err = kafka_consumer_Consumer_commit_sync(consumer);
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
    kafka_List_t *subs = kafka_consumer_Consumer_subscription(consumer);
    TEST_ASSERT_EQUAL_INT32(1, kafka_List_size(subs));
    kafka_List_destroy(subs);
    kafka_consumer_Consumer_destroy(consumer);
    callback_pump_destroy(&pump);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// destroy() without close() releases the consumer without hanging.
static void test_kafka_consumer_destroy_without_close(void) {
    kafka_consumer_Consumer_t *consumer = create_consumer();
    kafka_consumer_Consumer_destroy(consumer);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_kafka_consumer_new_succeeds);
    RUN_TEST(test_kafka_consumer_new_via_setters);
    RUN_TEST(test_kafka_consumer_new_with_string_deserializers);
    RUN_TEST(test_kafka_consumer_classic_protocol_rejected);
    RUN_TEST(test_kafka_consumer_subscribe);
    RUN_TEST(test_kafka_consumer_wakeup_before_poll);
    RUN_TEST(test_kafka_consumer_wakeup_from_other_thread);
    RUN_TEST(test_kafka_consumer_poll_cb_interrupted_by_wakeup);
    RUN_TEST(test_kafka_consumer_destroy_without_close);
    return UNITY_END();
}
