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

// `org.apache.kafka.common.serialization` through the C FFI (CLAUDE.md §4):
//
// - a C implementation of `Serializer` / `Deserializer` registered with
//   `_new(void *self, <typed fn pointers>)` and reached through the invokers,
//   with a NULL optional method standing for the Java default;
// - an asynchronous C implementation registered with `_new_async`, reporting
//   through `__set_callback_result` during the call or later from another
//   thread, reached through the blocking invokers and their `_cb` twins;
// - the built-in `StringSerializer`, `ByteArraySerializer`,
//   `StringDeserializer`, `ByteArrayDeserializer` and `BytesDeserializer`
//   classes used as interfaces through their `__as_` borrowed views.
//
// The generic `void *` is what each class documents: a `const char *` for
// `StringSerializer`, a `const kafka_Bytes_t *` for `ByteArraySerializer`, an
// owned `char *` (`kafka_string_destroy`) from `StringDeserializer` and an
// owned `kafka_Bytes_t *` (`kafka_Bytes_destroy`) from the two byte
// deserializers.

#include <confluent_kafka.h>
#include <ctype.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

static kafka_Bytes_t bytes_of(const char *s) {
    kafka_Bytes_t b = {(const uint8_t *)s, (int32_t)strlen(s)};
    return b;
}

static void assert_bytes(const char *expected, kafka_Bytes_t actual) {
    TEST_ASSERT_NOT_NULL(actual.data);
    TEST_ASSERT_EQUAL_INT32((int32_t)strlen(expected), actual.len);
    TEST_ASSERT_EQUAL_MEMORY(expected, actual.data, (uint32_t)actual.len);
}

// ---------------------------------------------------------------------------
// A C serializer: `data` is a `const char *`, serialized upper-cased into a
// buffer the implementation owns and reuses, so the test can tell whether
// Rust copied it.
// ---------------------------------------------------------------------------

typedef struct {
    char buffer[32];
    int serialize_calls;
    int with_headers_calls;
} upper_serializer_t;

static kafka_Bytes_t upper_serialize(void *self, const char *topic, const void *data, kafka_common_Error_t **out_err) {
    upper_serializer_t *imp = self;
    imp->serialize_calls++;
    TEST_ASSERT_EQUAL_STRING("t", topic);
    kafka_Bytes_t out = {NULL, 0};
    if (data == NULL) {
        return out;
    }
    const char *text = data;
    if (strcmp(text, "fail") == 0) {
        *out_err = kafka_common_Error_serialization("cannot serialize fail");
        return out;
    }
    size_t len = strlen(text);
    TEST_ASSERT_TRUE(len < sizeof(imp->buffer));
    for (size_t i = 0; i < len; i++) {
        imp->buffer[i] = (char)toupper((unsigned char)text[i]);
    }
    out.data = (const uint8_t *)imp->buffer;
    out.len = (int32_t)len;
    return out;
}

static kafka_Bytes_t upper_serialize_with_headers(void *self, const char *topic,
                                                  const kafka_common_header_internals_RecordHeaders_t *headers,
                                                  const void *data, kafka_common_Error_t **out_err) {
    upper_serializer_t *imp = self;
    imp->with_headers_calls++;
    TEST_ASSERT_NOT_NULL(headers);
    return upper_serialize(self, topic, data, out_err);
}

static void test_c_serializer_is_reached_through_the_invokers(void) {
    upper_serializer_t imp = {{0}, 0, 0};
    kafka_common_serialization_Serializer_t *serializer =
        kafka_common_serialization_Serializer_new(&imp, upper_serialize, upper_serialize_with_headers);
    kafka_common_header_internals_RecordHeaders_t *headers = kafka_common_header_internals_RecordHeaders_new();

    kafka_common_Error_t *error = NULL;
    kafka_Bytes_t out = kafka_common_serialization_Serializer_serialize(serializer, "t", "hello", &error);
    TEST_ASSERT_NULL(error);
    assert_bytes("HELLO", out);
    // Rust copied the bytes: the implementation's buffer can be reused.
    TEST_ASSERT_TRUE(out.data != (const uint8_t *)imp.buffer);
    memset(imp.buffer, 'x', sizeof(imp.buffer));
    assert_bytes("HELLO", out);

    out = kafka_common_serialization_Serializer_serialize_with_headers(serializer, "t", headers, "abc", &error);
    TEST_ASSERT_NULL(error);
    assert_bytes("ABC", out);

    TEST_ASSERT_EQUAL_INT(2, imp.serialize_calls);
    TEST_ASSERT_EQUAL_INT(1, imp.with_headers_calls);

    // Java's null serializes to a null array.
    out = kafka_common_serialization_Serializer_serialize(serializer, "t", NULL, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(out.data);

    // The implementation's error is stored owned in `*out_err`, with a NULL
    // array as the failure value.
    out = kafka_common_serialization_Serializer_serialize(serializer, "t", "fail", &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("cannot serialize fail", kafka_common_Error_message(error));
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_Error_is_serialization_error(error));
    TEST_ASSERT_NULL(out.data);
    TEST_ASSERT_EQUAL_INT32(0, out.len);
    kafka_common_Error_destroy(error);
    error = NULL;

    kafka_common_header_internals_RecordHeaders_destroy(headers);
    kafka_common_serialization_Serializer_destroy(serializer);
    kafka_common_serialization_Serializer_destroy(NULL);
}

// ---------------------------------------------------------------------------
// A C deserializer producing an owned, upper-cased C string.
// ---------------------------------------------------------------------------

typedef struct {
    int deserialize_calls;
    int with_headers_calls;
    int saw_header_k;
    int configured_entries;
    int8_t configured_is_key;
    int close_calls;
} upper_deserializer_t;

static void *upper_deserialize(void *self, const char *topic, kafka_Bytes_t data, kafka_common_Error_t **out_err) {
    upper_deserializer_t *imp = self;
    imp->deserialize_calls++;
    TEST_ASSERT_EQUAL_STRING("t", topic);
    if (data.len == 4 && memcmp(data.data, "fail", 4) == 0) {
        *out_err = kafka_common_Error_serialization("cannot deserialize fail");
        return NULL;
    }
    char *text = malloc((size_t)data.len + 1);
    for (int32_t i = 0; i < data.len; i++) {
        text[i] = (char)toupper(data.data[i]);
    }
    text[data.len] = '\0';
    return text;
}

static void *upper_deserialize_with_headers(void *self, const char *topic,
                                            const kafka_common_header_internals_RecordHeaders_t *headers,
                                            kafka_Bytes_t data, kafka_common_Error_t **out_err) {
    upper_deserializer_t *imp = self;
    imp->with_headers_calls++;
    // The `Headers` methods are reached through the class handle's interface
    // view; the copy is the implementation's for the call, so the cast is
    // the one `__as_Headers` asks for.
    kafka_common_header_Headers_t *view =
        kafka_common_header_internals_RecordHeaders__as_Headers((kafka_common_header_internals_RecordHeaders_t *)headers);
    imp->saw_header_k = kafka_common_header_Headers_last_header(view, "k") != NULL;
    return upper_deserialize(self, topic, data, out_err);
}

static void upper_configure(void *self, const kafka_Map_t *configs, int8_t is_key) {
    upper_deserializer_t *imp = self;
    imp->configured_entries = kafka_Map_size(configs);
    imp->configured_is_key = is_key;
}

static void upper_close(void *self) {
    upper_deserializer_t *imp = self;
    imp->close_calls++;
}

static void test_c_deserializer_is_reached_through_the_invokers(void) {
    upper_deserializer_t imp = {0, 0, 0, 0, 0, 0};
    // `deserialize_with_headers_bytes` left NULL: the Java default forwards to
    // `deserialize_with_headers`.
    kafka_common_serialization_Deserializer_t *deserializer = kafka_common_serialization_Deserializer_new(
        &imp, upper_deserialize, upper_deserialize_with_headers, NULL, upper_configure, upper_close);
    kafka_common_header_internals_RecordHeaders_t *headers = kafka_common_header_internals_RecordHeaders_new();
    kafka_common_Error_t *error = NULL;
    kafka_common_header_Headers_add_with_key_value(kafka_common_header_internals_RecordHeaders__as_Headers(headers), "k",
                                                  bytes_of("v"), &error);
    TEST_ASSERT_NULL(error);

    void *out = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("hello"), &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("HELLO", (const char *)out);
    free(out);

    out = kafka_common_serialization_Deserializer_deserialize_with_headers(deserializer, "t", headers, bytes_of("abc"),
                                                                           &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("ABC", (const char *)out);
    TEST_ASSERT_TRUE(imp.saw_header_k);
    free(out);

    imp.saw_header_k = 0;
    const char *source = "hello world";
    kafka_Bytes_t data = {(const uint8_t *)source + 6, 5};
    out = kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(deserializer, "t", headers, data, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("WORLD", (const char *)out);
    TEST_ASSERT_TRUE(imp.saw_header_k);
    free(out);
    TEST_ASSERT_EQUAL_INT(3, imp.deserialize_calls);
    TEST_ASSERT_EQUAL_INT(2, imp.with_headers_calls);

    // The implementation's error is stored owned in `*out_err`, with NULL as
    // the failure value.
    out = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("fail"), &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("cannot deserialize fail", kafka_common_Error_message(error));
    TEST_ASSERT_NULL(out);
    kafka_common_Error_destroy(error);

    // `configure` receives the configs as a map of strings, `close` is
    // forwarded.
    kafka_Map_t *configs = kafka_Map_new();
    char key_a[] = "alpha", value_a[] = "1", key_b[] = "beta", value_b[] = "2";
    kafka_Map_put(configs, key_a, value_a);
    kafka_Map_put(configs, key_b, value_b);
    kafka_common_serialization_Deserializer_configure(deserializer, configs, 1);
    TEST_ASSERT_EQUAL_INT(2, imp.configured_entries);
    TEST_ASSERT_EQUAL_INT8(1, imp.configured_is_key);
    kafka_Map_destroy(configs);
    kafka_common_serialization_Deserializer_close(deserializer);
    TEST_ASSERT_EQUAL_INT(1, imp.close_calls);

    kafka_common_header_internals_RecordHeaders_destroy(headers);
    kafka_common_serialization_Deserializer_destroy(deserializer);
    kafka_common_serialization_Deserializer_destroy(NULL);
}

// ---------------------------------------------------------------------------
// Built-in classes through their `__as_` views.
// ---------------------------------------------------------------------------

static void test_string_serde_round_trip_through_the_views(void) {
    kafka_common_serialization_StringSerializer_t *string_serializer = kafka_common_serialization_StringSerializer_new();
    kafka_common_serialization_StringDeserializer_t *string_deserializer =
        kafka_common_serialization_StringDeserializer_new();
    // The view is cached: one pointer per class handle, valid as long as it.
    const kafka_common_serialization_Serializer_t *serializer =
        kafka_common_serialization_StringSerializer__as_Serializer(string_serializer);
    TEST_ASSERT_EQUAL_PTR(serializer, kafka_common_serialization_StringSerializer__as_Serializer(string_serializer));
    kafka_common_serialization_Deserializer_t *deserializer =
        kafka_common_serialization_StringDeserializer__as_Deserializer(string_deserializer);
    TEST_ASSERT_EQUAL_PTR(deserializer,
                          kafka_common_serialization_StringDeserializer__as_Deserializer(string_deserializer));

    kafka_common_Error_t *error = NULL;
    kafka_Bytes_t encoded = kafka_common_serialization_Serializer_serialize(serializer, "t", "h\xc3\xa9llo", &error);
    TEST_ASSERT_NULL(error);
    assert_bytes("h\xc3\xa9llo", encoded);

    void *decoded = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", encoded, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("h\xc3\xa9llo", (const char *)decoded);
    kafka_string_destroy(decoded);

    // Java's null round-trips as a null array on the serializer side; the
    // deserializer turns a null array into an empty string, as the Rust
    // `Deserializer` takes a slice.
    encoded = kafka_common_serialization_Serializer_serialize(serializer, "t", NULL, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(encoded.data);
    decoded = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", encoded, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("", (const char *)decoded);
    kafka_string_destroy(decoded);

    // Invalid UTF-8 is the translation of Java's `SerializationException`.
    encoded = kafka_common_serialization_Serializer_serialize(serializer, "t", "h\xe9llo", &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_NULL(encoded.data);
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_Error_is_serialization_error(error));
    TEST_ASSERT_EQUAL_STRING_LEN("Error when serializing string to byte[]: ", kafka_common_Error_message(error), 41);
    kafka_common_Error_destroy(error);

    // The views are never destroyed, only the class handles.
    kafka_common_serialization_StringSerializer_destroy(string_serializer);
    kafka_common_serialization_StringDeserializer_destroy(string_deserializer);
    kafka_common_serialization_StringSerializer_destroy(NULL);
    kafka_common_serialization_StringDeserializer_destroy(NULL);
}

static void test_byte_array_serde_round_trip_through_the_views(void) {
    kafka_common_serialization_ByteArraySerializer_t *array_serializer =
        kafka_common_serialization_ByteArraySerializer_new();
    kafka_common_serialization_ByteArrayDeserializer_t *array_deserializer =
        kafka_common_serialization_ByteArrayDeserializer_new();
    const kafka_common_serialization_Serializer_t *serializer =
        kafka_common_serialization_ByteArraySerializer__as_Serializer(array_serializer);
    kafka_common_serialization_Deserializer_t *deserializer =
        kafka_common_serialization_ByteArrayDeserializer__as_Deserializer(array_deserializer);

    uint8_t raw[] = {0, 1, 2, 255};
    kafka_Bytes_t payload = {raw, 4};
    kafka_common_Error_t *error = NULL;
    kafka_Bytes_t encoded = kafka_common_serialization_Serializer_serialize(serializer, "t", &payload, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_INT32(4, encoded.len);
    TEST_ASSERT_EQUAL_MEMORY(raw, encoded.data, 4);
    TEST_ASSERT_TRUE(encoded.data != raw);

    // The deserializer's `void *` is an owned `kafka_Bytes_t *`, a copy of
    // the input, freed with `kafka_Bytes_destroy`.
    void *decoded = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", encoded, &error);
    TEST_ASSERT_NULL(error);
    kafka_Bytes_t *decoded_bytes = decoded;
    TEST_ASSERT_EQUAL_INT32(4, decoded_bytes->len);
    TEST_ASSERT_EQUAL_MEMORY(raw, decoded_bytes->data, 4);
    TEST_ASSERT_TRUE(decoded_bytes->data != encoded.data);
    kafka_Bytes_destroy(decoded_bytes);

    // A NULL pointer and a null array are both Java's null.
    kafka_Bytes_t null_array = {NULL, 0};
    encoded = kafka_common_serialization_Serializer_serialize(serializer, "t", NULL, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(encoded.data);
    encoded = kafka_common_serialization_Serializer_serialize(serializer, "t", &null_array, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(encoded.data);

    kafka_common_serialization_ByteArraySerializer_destroy(array_serializer);
    kafka_common_serialization_ByteArrayDeserializer_destroy(array_deserializer);
    kafka_common_serialization_ByteArraySerializer_destroy(NULL);
    kafka_common_serialization_ByteArrayDeserializer_destroy(NULL);
}

static void test_bytes_deserializer_through_the_view(void) {
    kafka_common_serialization_BytesDeserializer_t *bytes_deserializer = kafka_common_serialization_BytesDeserializer_new();
    kafka_common_serialization_Deserializer_t *deserializer =
        kafka_common_serialization_BytesDeserializer__as_Deserializer(bytes_deserializer);
    TEST_ASSERT_EQUAL_PTR(deserializer, kafka_common_serialization_BytesDeserializer__as_Deserializer(bytes_deserializer));
    kafka_common_header_internals_RecordHeaders_t *headers = kafka_common_header_internals_RecordHeaders_new();

    // The invoker copies `data`: the decoded bytes do not point into it.
    const char *source = "hello world";
    kafka_Bytes_t data = {(const uint8_t *)source + 6, 5};
    kafka_common_Error_t *error = NULL;
    void *decoded =
        kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(deserializer, "t", headers, data, &error);
    TEST_ASSERT_NULL(error);
    kafka_Bytes_t *decoded_bytes = decoded;
    assert_bytes("world", *decoded_bytes);
    TEST_ASSERT_TRUE(decoded_bytes->data != data.data);
    kafka_Bytes_destroy(decoded_bytes);

    decoded = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("raw"), &error);
    TEST_ASSERT_NULL(error);
    decoded_bytes = decoded;
    assert_bytes("raw", *decoded_bytes);
    kafka_Bytes_destroy(decoded_bytes);
    kafka_Bytes_destroy(NULL);

    kafka_common_header_internals_RecordHeaders_destroy(headers);
    kafka_common_serialization_BytesDeserializer_destroy(bytes_deserializer);
    kafka_common_serialization_BytesDeserializer_destroy(NULL);
}

// ---------------------------------------------------------------------------
// Asynchronous C implementations: each method returns at once and reports
// through `__set_callback_result`, either inside the call (`inline_report`) or
// later, from a reporter thread that picks up the pending call.
// ---------------------------------------------------------------------------

typedef struct {
    int fired;
    pthread_t thread;
    kafka_Bytes_t bytes;
    char copy[64];
    void *value;
    kafka_common_Error_t *error;
} completion_t;

static void record_completion_thread(completion_t *c) {
    c->thread = pthread_self();
    __atomic_store_n(&c->fired, 1, __ATOMIC_RELEASE);
}

static int completion_fired(completion_t *c) { return __atomic_load_n(&c->fired, __ATOMIC_ACQUIRE); }

static void upper_into(char *out, const uint8_t *in, int32_t len) {
    for (int32_t i = 0; i < len; i++) {
        out[i] = (char)toupper(in[i]);
    }
    out[len] = '\0';
}

typedef struct {
    const kafka_common_serialization_Serializer_t *handle;
    int inline_report;
    int pending;
    int64_t callback_id;
    const char *topic;
    const char *data;
    int serialize_calls;
    int with_headers_calls;
    int configured_entries;
    int8_t configured_is_key;
    int close_calls;
} async_upper_serializer_t;

static void async_serializer_report(async_upper_serializer_t *imp, int64_t id, const char *topic, const char *data) {
    TEST_ASSERT_EQUAL_STRING("t", topic);
    if (data == NULL) {
        // A NULL result is Java's null array.
        kafka_common_serialization_Serializer__set_callback_result(imp->handle, id, NULL, NULL);
        return;
    }
    if (strcmp(data, "fail") == 0) {
        kafka_common_serialization_Serializer__set_callback_result(
            imp->handle, id, NULL, kafka_common_Error_serialization("cannot serialize fail"));
        return;
    }
    // The bytes are on this stack frame: Rust copies them before the report
    // returns.
    char buffer[64];
    size_t len = strlen(data);
    TEST_ASSERT_TRUE(len < sizeof(buffer));
    upper_into(buffer, (const uint8_t *)data, (int32_t)len);
    kafka_Bytes_t out = {(const uint8_t *)buffer, (int32_t)len};
    kafka_common_serialization_Serializer__set_callback_result(imp->handle, id, &out, NULL);
    memset(buffer, 'x', sizeof(buffer));
}

static void async_upper_serialize(void *self, const char *topic, const void *data, int64_t callback_id) {
    async_upper_serializer_t *imp = self;
    imp->serialize_calls++;
    if (imp->inline_report) {
        async_serializer_report(imp, callback_id, topic, data);
        return;
    }
    imp->callback_id = callback_id;
    imp->topic = topic;
    imp->data = data;
    __atomic_store_n(&imp->pending, 1, __ATOMIC_RELEASE);
}

static void async_upper_serialize_with_headers(void *self, const char *topic,
                                               const kafka_common_header_internals_RecordHeaders_t *headers,
                                               const void *data, int64_t callback_id) {
    async_upper_serializer_t *imp = self;
    imp->with_headers_calls++;
    TEST_ASSERT_NOT_NULL(headers);
    async_upper_serialize(self, topic, data, callback_id);
}

static void async_upper_serializer_configure(void *self, const kafka_Map_t *configs, int8_t is_key,
                                             int64_t callback_id) {
    async_upper_serializer_t *imp = self;
    imp->configured_entries = kafka_Map_size(configs);
    imp->configured_is_key = is_key;
    kafka_common_serialization_Serializer__set_callback_result(imp->handle, callback_id, NULL, NULL);
}

static void async_upper_serializer_close(void *self, int64_t callback_id) {
    async_upper_serializer_t *imp = self;
    imp->close_calls++;
    kafka_common_serialization_Serializer__set_callback_result(imp->handle, callback_id, NULL, NULL);
}

static void on_serializer_done(void *opaque) { record_completion_thread(opaque); }

// Waits for the pending call and reports it from this thread.
static void *async_serializer_reporter(void *arg) {
    async_upper_serializer_t *imp = arg;
    while (!__atomic_load_n(&imp->pending, __ATOMIC_ACQUIRE)) {
        sched_yield();
    }
    imp->pending = 0;
    async_serializer_report(imp, imp->callback_id, imp->topic, imp->data);
    return NULL;
}

static void on_serialized(kafka_Bytes_t value, kafka_common_Error_t *error, void *opaque) {
    completion_t *c = opaque;
    c->bytes.len = value.len;
    c->bytes.data = NULL;
    if (value.data != NULL) {
        // The bytes are valid only during the completion.
        TEST_ASSERT_TRUE((size_t)value.len < sizeof(c->copy));
        memcpy(c->copy, value.data, (size_t)value.len);
        c->bytes.data = (const uint8_t *)c->copy;
    }
    c->error = error;
    record_completion_thread(c);
}

static void test_async_c_serializer_reporting_inside_the_call(void) {
    async_upper_serializer_t imp = {0};
    kafka_common_serialization_Serializer_t *serializer = kafka_common_serialization_Serializer_new_async(
        &imp, async_upper_serialize, async_upper_serialize_with_headers, async_upper_serializer_configure,
        async_upper_serializer_close);
    imp.handle = serializer;
    imp.inline_report = 1;
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_serialization_Serializer__is_async(serializer));
    kafka_common_header_internals_RecordHeaders_t *headers = kafka_common_header_internals_RecordHeaders_new();

    kafka_common_Error_t *error = NULL;
    kafka_Bytes_t out = kafka_common_serialization_Serializer_serialize(serializer, "t", "hello", &error);
    TEST_ASSERT_NULL(error);
    assert_bytes("HELLO", out);
    out = kafka_common_serialization_Serializer_serialize_with_headers(serializer, "t", headers, "abc", &error);
    TEST_ASSERT_NULL(error);
    assert_bytes("ABC", out);
    out = kafka_common_serialization_Serializer_serialize(serializer, "t", NULL, &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(out.data);
    out = kafka_common_serialization_Serializer_serialize(serializer, "t", "fail", &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("cannot serialize fail", kafka_common_Error_message(error));
    TEST_ASSERT_NULL(out.data);
    kafka_common_Error_destroy(error);
    error = NULL;

    // The `_cb` twin fires on the calling thread, before it returns, when the
    // implementation reports inside the call.
    completion_t c = {0};
    kafka_common_serialization_Serializer_serialize_cb(serializer, "t", "cb", on_serialized, &c);
    TEST_ASSERT_TRUE(completion_fired(&c));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), c.thread));
    TEST_ASSERT_NULL(c.error);
    assert_bytes("CB", c.bytes);

    completion_t h = {0};
    kafka_common_serialization_Serializer_serialize_with_headers_cb(serializer, "t", headers, "fail", on_serialized,
                                                                    &h);
    TEST_ASSERT_TRUE(completion_fired(&h));
    TEST_ASSERT_NOT_NULL(h.error);
    TEST_ASSERT_EQUAL_STRING("cannot serialize fail", kafka_common_Error_message(h.error));
    TEST_ASSERT_NULL(h.bytes.data);
    kafka_common_Error_destroy(h.error);

    TEST_ASSERT_EQUAL_INT(6, imp.serialize_calls);
    TEST_ASSERT_EQUAL_INT(2, imp.with_headers_calls);

    // `configure` and `close`, blocking and `_cb`.
    kafka_Map_t *configs = kafka_Map_new();
    char key_a[] = "alpha", value_a[] = "1";
    kafka_Map_put(configs, key_a, value_a);
    kafka_common_serialization_Serializer_configure(serializer, configs, 1);
    TEST_ASSERT_EQUAL_INT(1, imp.configured_entries);
    TEST_ASSERT_EQUAL_INT8(1, imp.configured_is_key);
    completion_t configured = {0};
    kafka_common_serialization_Serializer_configure_cb(serializer, configs, 0, on_serializer_done, &configured);
    TEST_ASSERT_TRUE(completion_fired(&configured));
    TEST_ASSERT_EQUAL_INT8(0, imp.configured_is_key);
    kafka_Map_destroy(configs);
    kafka_common_serialization_Serializer_close(serializer);
    completion_t closed = {0};
    kafka_common_serialization_Serializer_close_cb(serializer, on_serializer_done, &closed);
    TEST_ASSERT_TRUE(completion_fired(&closed));
    TEST_ASSERT_EQUAL_INT(2, imp.close_calls);

    // A report for an id that is not pending (here, already reported) is
    // ignored and its error freed.
    kafka_common_serialization_Serializer__set_callback_result(serializer, 0, NULL,
                                                               kafka_common_Error_serialization("late"));

    kafka_common_header_internals_RecordHeaders_destroy(headers);
    kafka_common_serialization_Serializer_destroy(serializer);
}

static void test_async_c_serializer_reporting_later_from_another_thread(void) {
    async_upper_serializer_t imp = {0};
    kafka_common_serialization_Serializer_t *serializer =
        kafka_common_serialization_Serializer_new_async(&imp, async_upper_serialize, NULL, NULL, NULL);
    imp.handle = serializer;

    // `configure` and `close` left NULL are the Java defaults: nothing is
    // called, and each `_cb` completion fires before it returns.
    kafka_common_serialization_Serializer_configure(serializer, NULL, 1);
    completion_t configured = {0};
    kafka_common_serialization_Serializer_configure_cb(serializer, NULL, 1, on_serializer_done, &configured);
    TEST_ASSERT_TRUE(completion_fired(&configured));
    kafka_common_serialization_Serializer_close(serializer);
    completion_t closed = {0};
    kafka_common_serialization_Serializer_close_cb(serializer, on_serializer_done, &closed);
    TEST_ASSERT_TRUE(completion_fired(&closed));
    TEST_ASSERT_EQUAL_INT(0, imp.serialize_calls);

    // The blocking invoker parks the calling thread until the report.
    pthread_t reporter;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&reporter, NULL, async_serializer_reporter, &imp));
    kafka_common_Error_t *error = NULL;
    kafka_Bytes_t out = kafka_common_serialization_Serializer_serialize(serializer, "t", "later", &error);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(reporter, NULL));
    TEST_ASSERT_NULL(error);
    assert_bytes("LATER", out);

    // The `_cb` twin returns before the report and fires on the reporting
    // thread. `data` stays valid until then, as the twin documents.
    completion_t c = {0};
    kafka_common_serialization_Serializer_serialize_cb(serializer, "t", "async", on_serialized, &c);
    TEST_ASSERT_FALSE(completion_fired(&c));
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&reporter, NULL, async_serializer_reporter, &imp));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(reporter, NULL));
    TEST_ASSERT_TRUE(completion_fired(&c));
    TEST_ASSERT_TRUE(pthread_equal(reporter, c.thread));
    TEST_ASSERT_NULL(c.error);
    assert_bytes("ASYNC", c.bytes);

    // The default `serialize_with_headers` forwards to `serialize`.
    completion_t h = {0};
    kafka_common_header_internals_RecordHeaders_t *headers = kafka_common_header_internals_RecordHeaders_new();
    kafka_common_serialization_Serializer_serialize_with_headers_cb(serializer, "t", headers, "hdr", on_serialized, &h);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&reporter, NULL, async_serializer_reporter, &imp));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(reporter, NULL));
    TEST_ASSERT_TRUE(completion_fired(&h));
    assert_bytes("HDR", h.bytes);
    TEST_ASSERT_EQUAL_INT(3, imp.serialize_calls);

    // A second report for the same id is ignored and its error freed: the
    // completion has already fired once.
    int64_t reported = imp.callback_id;
    kafka_common_serialization_Serializer__set_callback_result(serializer, reported, NULL,
                                                               kafka_common_Error_serialization("again"));
    TEST_ASSERT_NULL(h.error);

    kafka_common_header_internals_RecordHeaders_destroy(headers);
    kafka_common_serialization_Serializer_destroy(serializer);
}

typedef struct {
    const kafka_common_serialization_Deserializer_t *handle;
    int inline_report;
    int pending;
    int64_t callback_id;
    const char *topic;
    kafka_Bytes_t data;
    int deserialize_calls;
    int with_headers_calls;
    int configured_entries;
    int8_t configured_is_key;
    int close_calls;
} async_upper_deserializer_t;

static void async_deserializer_report(async_upper_deserializer_t *imp, int64_t id, const char *topic,
                                      kafka_Bytes_t data) {
    TEST_ASSERT_EQUAL_STRING("t", topic);
    if (data.len == 4 && memcmp(data.data, "fail", 4) == 0) {
        kafka_common_serialization_Deserializer__set_callback_result(
            imp->handle, id, NULL, kafka_common_Error_serialization("cannot deserialize fail"));
        return;
    }
    char *text = malloc((size_t)data.len + 1);
    upper_into(text, data.data, data.len);
    kafka_common_serialization_Deserializer__set_callback_result(imp->handle, id, text, NULL);
}

static void async_upper_deserialize(void *self, const char *topic, kafka_Bytes_t data, int64_t callback_id) {
    async_upper_deserializer_t *imp = self;
    imp->deserialize_calls++;
    if (imp->inline_report) {
        async_deserializer_report(imp, callback_id, topic, data);
        return;
    }
    imp->callback_id = callback_id;
    imp->topic = topic;
    imp->data = data;
    __atomic_store_n(&imp->pending, 1, __ATOMIC_RELEASE);
}

static void async_upper_deserialize_with_headers(void *self, const char *topic,
                                                 const kafka_common_header_internals_RecordHeaders_t *headers,
                                                 kafka_Bytes_t data, int64_t callback_id) {
    async_upper_deserializer_t *imp = self;
    imp->with_headers_calls++;
    TEST_ASSERT_NOT_NULL(headers);
    async_upper_deserialize(self, topic, data, callback_id);
}

static void async_upper_configure(void *self, const kafka_Map_t *configs, int8_t is_key, int64_t callback_id) {
    async_upper_deserializer_t *imp = self;
    imp->configured_entries = kafka_Map_size(configs);
    imp->configured_is_key = is_key;
    kafka_common_serialization_Deserializer__set_callback_result(imp->handle, callback_id, NULL, NULL);
}

static void async_upper_close(void *self, int64_t callback_id) {
    async_upper_deserializer_t *imp = self;
    imp->close_calls++;
    kafka_common_serialization_Deserializer__set_callback_result(imp->handle, callback_id, NULL, NULL);
}

static void *async_deserializer_reporter(void *arg) {
    async_upper_deserializer_t *imp = arg;
    while (!__atomic_load_n(&imp->pending, __ATOMIC_ACQUIRE)) {
        sched_yield();
    }
    imp->pending = 0;
    // `topic` and `data` are still valid: Rust keeps them until the report.
    async_deserializer_report(imp, imp->callback_id, imp->topic, imp->data);
    return NULL;
}

static void on_deserialized(void *value, kafka_common_Error_t *error, void *opaque) {
    completion_t *c = opaque;
    c->value = value;
    c->error = error;
    record_completion_thread(c);
}

static void on_done(void *opaque) { record_completion_thread(opaque); }

static void test_async_c_deserializer_reporting_inside_the_call(void) {
    async_upper_deserializer_t imp = {0};
    // `deserialize_with_headers_bytes` left NULL: the Java default forwards to
    // `deserialize_with_headers`.
    kafka_common_serialization_Deserializer_t *deserializer = kafka_common_serialization_Deserializer_new_async(
        &imp, async_upper_deserialize, async_upper_deserialize_with_headers, NULL, async_upper_configure,
        async_upper_close);
    imp.handle = deserializer;
    imp.inline_report = 1;
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_serialization_Deserializer__is_async(deserializer));
    kafka_common_header_internals_RecordHeaders_t *headers = kafka_common_header_internals_RecordHeaders_new();

    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("hello"), &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("HELLO", (const char *)out);
    free(out);
    out = kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(deserializer, "t", headers,
                                                                                 bytes_of("abc"), &error);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("ABC", (const char *)out);
    free(out);
    out = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("fail"), &error);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("cannot deserialize fail", kafka_common_Error_message(error));
    TEST_ASSERT_NULL(out);
    kafka_common_Error_destroy(error);
    error = NULL;

    completion_t c = {0};
    kafka_common_serialization_Deserializer_deserialize_cb(deserializer, "t", bytes_of("cb"), on_deserialized, &c);
    TEST_ASSERT_TRUE(completion_fired(&c));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), c.thread));
    TEST_ASSERT_NULL(c.error);
    TEST_ASSERT_EQUAL_STRING("CB", (const char *)c.value);
    free(c.value);

    completion_t h = {0};
    kafka_common_serialization_Deserializer_deserialize_with_headers_cb(deserializer, "t", headers, bytes_of("fail"),
                                                                        on_deserialized, &h);
    TEST_ASSERT_TRUE(completion_fired(&h));
    TEST_ASSERT_NULL(h.value);
    TEST_ASSERT_NOT_NULL(h.error);
    TEST_ASSERT_EQUAL_STRING("cannot deserialize fail", kafka_common_Error_message(h.error));
    kafka_common_Error_destroy(h.error);

    completion_t b = {0};
    kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_cb(deserializer, "t", headers,
                                                                              bytes_of("bytes"), on_deserialized, &b);
    TEST_ASSERT_TRUE(completion_fired(&b));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), b.thread));
    TEST_ASSERT_NULL(b.error);
    TEST_ASSERT_EQUAL_STRING("BYTES", (const char *)b.value);
    free(b.value);

    TEST_ASSERT_EQUAL_INT(6, imp.deserialize_calls);
    TEST_ASSERT_EQUAL_INT(3, imp.with_headers_calls);

    // `configure` and `close`, blocking and `_cb`.
    kafka_Map_t *configs = kafka_Map_new();
    char key_a[] = "alpha", value_a[] = "1";
    kafka_Map_put(configs, key_a, value_a);
    kafka_common_serialization_Deserializer_configure(deserializer, configs, 1);
    TEST_ASSERT_EQUAL_INT(1, imp.configured_entries);
    TEST_ASSERT_EQUAL_INT8(1, imp.configured_is_key);
    completion_t configured = {0};
    kafka_common_serialization_Deserializer_configure_cb(deserializer, configs, 0, on_done, &configured);
    TEST_ASSERT_TRUE(completion_fired(&configured));
    TEST_ASSERT_EQUAL_INT8(0, imp.configured_is_key);
    kafka_Map_destroy(configs);
    kafka_common_serialization_Deserializer_close(deserializer);
    completion_t closed = {0};
    kafka_common_serialization_Deserializer_close_cb(deserializer, on_done, &closed);
    TEST_ASSERT_TRUE(completion_fired(&closed));
    TEST_ASSERT_EQUAL_INT(2, imp.close_calls);

    kafka_common_header_internals_RecordHeaders_destroy(headers);
    kafka_common_serialization_Deserializer_destroy(deserializer);
}

static void test_async_c_deserializer_reporting_later_from_another_thread(void) {
    async_upper_deserializer_t imp = {0};
    kafka_common_serialization_Deserializer_t *deserializer =
        kafka_common_serialization_Deserializer_new_async(&imp, async_upper_deserialize, NULL, NULL, NULL, NULL);
    imp.handle = deserializer;

    // The caller's buffer is reused right after the call: the reporter reads
    // the copy Rust keeps until the report.
    char source[] = "later";
    pthread_t reporter;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&reporter, NULL, async_deserializer_reporter, &imp));
    kafka_common_Error_t *error = NULL;
    void *out = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of(source), &error);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(reporter, NULL));
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("LATER", (const char *)out);
    free(out);

    completion_t c = {0};
    char buffer[] = "async";
    kafka_common_serialization_Deserializer_deserialize_cb(deserializer, "t", bytes_of(buffer), on_deserialized, &c);
    TEST_ASSERT_FALSE(completion_fired(&c));
    memset(buffer, 'x', sizeof(buffer) - 1);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&reporter, NULL, async_deserializer_reporter, &imp));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(reporter, NULL));
    TEST_ASSERT_TRUE(completion_fired(&c));
    TEST_ASSERT_TRUE(pthread_equal(reporter, c.thread));
    TEST_ASSERT_NULL(c.error);
    TEST_ASSERT_EQUAL_STRING("ASYNC", (const char *)c.value);
    free(c.value);

    // A second report for the same id is ignored and its error freed.
    kafka_common_serialization_Deserializer__set_callback_result(deserializer, imp.callback_id, NULL,
                                                                 kafka_common_Error_serialization("again"));

    // `configure` / `close` left NULL are the Java defaults, completing at
    // once.
    completion_t closed = {0};
    kafka_common_serialization_Deserializer_close_cb(deserializer, on_done, &closed);
    TEST_ASSERT_TRUE(completion_fired(&closed));

    kafka_common_serialization_Deserializer_destroy(deserializer);
}

static void test_is_async_is_zero_for_sync_and_built_in_serdes(void) {
    upper_serializer_t ser = {{0}, 0, 0};
    kafka_common_serialization_Serializer_t *serializer =
        kafka_common_serialization_Serializer_new(&ser, upper_serialize, NULL);
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_serialization_Serializer__is_async(serializer));
    upper_deserializer_t de = {0, 0, 0, 0, 0, 0};
    kafka_common_serialization_Deserializer_t *deserializer =
        kafka_common_serialization_Deserializer_new(&de, upper_deserialize, NULL, NULL, NULL, NULL);
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_serialization_Deserializer__is_async(deserializer));

    // A report on a synchronous handle is ignored and its error freed.
    kafka_common_serialization_Serializer__set_callback_result(serializer, 0, NULL,
                                                               kafka_common_Error_serialization("x"));
    kafka_common_serialization_Deserializer__set_callback_result(deserializer, 0, NULL,
                                                                 kafka_common_Error_serialization("x"));

    // A synchronous implementation's `_cb` fires on the calling thread.
    completion_t c = {0};
    kafka_common_serialization_Serializer_serialize_cb(serializer, "t", "sync", on_serialized, &c);
    TEST_ASSERT_TRUE(completion_fired(&c));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), c.thread));
    assert_bytes("SYNC", c.bytes);
    completion_t d = {0};
    kafka_common_serialization_Deserializer_deserialize_cb(deserializer, "t", bytes_of("sync"), on_deserialized, &d);
    TEST_ASSERT_TRUE(completion_fired(&d));
    TEST_ASSERT_TRUE(pthread_equal(pthread_self(), d.thread));
    TEST_ASSERT_EQUAL_STRING("SYNC", (const char *)d.value);
    free(d.value);

    kafka_common_serialization_StringSerializer_t *string_serializer = kafka_common_serialization_StringSerializer_new();
    kafka_common_serialization_StringDeserializer_t *string_deserializer =
        kafka_common_serialization_StringDeserializer_new();
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_serialization_Serializer__is_async(
                                  kafka_common_serialization_StringSerializer__as_Serializer(string_serializer)));
    TEST_ASSERT_EQUAL_INT8(0, kafka_common_serialization_Deserializer__is_async(
                                  kafka_common_serialization_StringDeserializer__as_Deserializer(string_deserializer)));

    kafka_common_serialization_StringSerializer_destroy(string_serializer);
    kafka_common_serialization_StringDeserializer_destroy(string_deserializer);
    kafka_common_serialization_Serializer_destroy(serializer);
    kafka_common_serialization_Deserializer_destroy(deserializer);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_c_serializer_is_reached_through_the_invokers);
    RUN_TEST(test_c_deserializer_is_reached_through_the_invokers);
    RUN_TEST(test_string_serde_round_trip_through_the_views);
    RUN_TEST(test_byte_array_serde_round_trip_through_the_views);
    RUN_TEST(test_bytes_deserializer_through_the_view);
    RUN_TEST(test_async_c_serializer_reporting_inside_the_call);
    RUN_TEST(test_async_c_serializer_reporting_later_from_another_thread);
    RUN_TEST(test_async_c_deserializer_reporting_inside_the_call);
    RUN_TEST(test_async_c_deserializer_reporting_later_from_another_thread);
    RUN_TEST(test_is_async_is_zero_for_sync_and_built_in_serdes);
    return UNITY_END();
}
