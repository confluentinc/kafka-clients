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

static kafka_common_Error_t *upper_serialize(void *self, const char *topic, const void *data,
                                             kafka_Bytes_t *out) {
    upper_serializer_t *imp = self;
    imp->serialize_calls++;
    TEST_ASSERT_EQUAL_STRING("t", topic);
    if (data == NULL) {
        out->data = NULL;
        out->len = 0;
        return NULL;
    }
    const char *text = data;
    if (strcmp(text, "fail") == 0) {
        return kafka_common_Error_serialization("cannot serialize fail");
    }
    size_t len = strlen(text);
    TEST_ASSERT_TRUE(len < sizeof(imp->buffer));
    for (size_t i = 0; i < len; i++) {
        imp->buffer[i] = (char)toupper((unsigned char)text[i]);
    }
    out->data = (const uint8_t *)imp->buffer;
    out->len = (int32_t)len;
    return NULL;
}

static kafka_common_Error_t *upper_serialize_with_headers(void *self, const char *topic,
                                                          const kafka_common_header_internals_RecordHeaders_t *headers,
                                                          const void *data, kafka_Bytes_t *out) {
    upper_serializer_t *imp = self;
    imp->with_headers_calls++;
    TEST_ASSERT_NOT_NULL(headers);
    return upper_serialize(self, topic, data, out);
}

static void test_c_serializer_is_reached_through_the_invokers(void) {
    upper_serializer_t imp = {{0}, 0, 0};
    // `serialize_owned_with_headers` left NULL: the Java default forwards to
    // `serialize_with_headers`.
    kafka_common_serialization_Serializer_t *serializer =
        kafka_common_serialization_Serializer_new(&imp, upper_serialize, upper_serialize_with_headers, NULL);
    kafka_common_header_internals_RecordHeaders_t *headers = kafka_common_header_internals_RecordHeaders_new();

    kafka_Bytes_t out = {NULL, 0};
    kafka_common_Error_t *error = kafka_common_serialization_Serializer_serialize(serializer, "t", "hello", &out);
    TEST_ASSERT_NULL(error);
    assert_bytes("HELLO", out);
    // Rust copied the bytes: the implementation's buffer can be reused.
    TEST_ASSERT_TRUE(out.data != (const uint8_t *)imp.buffer);
    memset(imp.buffer, 'x', sizeof(imp.buffer));
    assert_bytes("HELLO", out);

    error = kafka_common_serialization_Serializer_serialize_with_headers(serializer, "t", headers, "abc", &out);
    TEST_ASSERT_NULL(error);
    assert_bytes("ABC", out);

    error = kafka_common_serialization_Serializer_serialize_owned_with_headers(serializer, "t", headers, "def", &out);
    TEST_ASSERT_NULL(error);
    assert_bytes("DEF", out);
    TEST_ASSERT_EQUAL_INT(3, imp.serialize_calls);
    TEST_ASSERT_EQUAL_INT(2, imp.with_headers_calls);

    // Java's null serializes to a null array.
    error = kafka_common_serialization_Serializer_serialize(serializer, "t", NULL, &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(out.data);

    // The implementation's error is returned owned; the slot is left alone.
    out = bytes_of("untouched");
    error = kafka_common_serialization_Serializer_serialize(serializer, "t", "fail", &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("cannot serialize fail", kafka_common_Error_message(error));
    TEST_ASSERT_EQUAL_INT8(1, kafka_common_Error_is_serialization_error(error));
    assert_bytes("untouched", out);
    kafka_common_Error_destroy(error);

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

static kafka_common_Error_t *upper_deserialize(void *self, const char *topic, kafka_Bytes_t data, void **out) {
    upper_deserializer_t *imp = self;
    imp->deserialize_calls++;
    TEST_ASSERT_EQUAL_STRING("t", topic);
    if (data.len == 4 && memcmp(data.data, "fail", 4) == 0) {
        return kafka_common_Error_serialization("cannot deserialize fail");
    }
    char *text = malloc((size_t)data.len + 1);
    for (int32_t i = 0; i < data.len; i++) {
        text[i] = (char)toupper(data.data[i]);
    }
    text[data.len] = '\0';
    *out = text;
    return NULL;
}

static kafka_common_Error_t *upper_deserialize_with_headers(void *self, const char *topic,
                                                            const kafka_common_header_Headers_t *headers,
                                                            kafka_Bytes_t data, void **out) {
    upper_deserializer_t *imp = self;
    imp->with_headers_calls++;
    imp->saw_header_k = kafka_common_header_Headers_last_header(headers, "k") != NULL;
    return upper_deserialize(self, topic, data, out);
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
    // Both `deserialize_from_shared*` left NULL: the Java defaults forward to
    // the non-shared methods.
    kafka_common_serialization_Deserializer_t *deserializer = kafka_common_serialization_Deserializer_new(
        &imp, upper_deserialize, upper_deserialize_with_headers, NULL, NULL, upper_configure, upper_close);
    kafka_common_header_internals_RecordHeaders_t *record_headers = kafka_common_header_internals_RecordHeaders_new();
    kafka_common_header_Headers_t *headers = kafka_common_header_internals_RecordHeaders__as_Headers(record_headers);
    kafka_common_Error_t *error = kafka_common_header_Headers_add_with_key_value(headers, "k", bytes_of("v"));
    TEST_ASSERT_NULL(error);

    void *out = NULL;
    error = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("hello"), &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("HELLO", (const char *)out);
    free(out);

    error = kafka_common_serialization_Deserializer_deserialize_with_headers(deserializer, "t", headers, bytes_of("abc"),
                                                                             &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("ABC", (const char *)out);
    TEST_ASSERT_TRUE(imp.saw_header_k);
    free(out);

    const char *source = "hello world";
    kafka_Bytes_t data = {(const uint8_t *)source + 6, 5};
    error = kafka_common_serialization_Deserializer_deserialize_from_shared(deserializer, "t", bytes_of(source), data, &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("WORLD", (const char *)out);
    free(out);

    error = kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(deserializer, "t", headers,
                                                                                         bytes_of(source), data, &out);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("WORLD", (const char *)out);
    free(out);
    TEST_ASSERT_EQUAL_INT(4, imp.deserialize_calls);
    TEST_ASSERT_EQUAL_INT(2, imp.with_headers_calls);

    // Data outside the source is rejected before reaching the implementation.
    error = kafka_common_serialization_Deserializer_deserialize_from_shared(deserializer, "t", bytes_of(source),
                                                                            bytes_of("elsewhere"), &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("data must be a sub-array of source", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);
    TEST_ASSERT_EQUAL_INT(4, imp.deserialize_calls);

    // The implementation's error is returned owned; the slot is left alone.
    out = &imp;
    error = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("fail"), &out);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("cannot deserialize fail", kafka_common_Error_message(error));
    TEST_ASSERT_EQUAL_PTR(&imp, out);
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

    kafka_common_header_internals_RecordHeaders_destroy(record_headers);
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

    kafka_Bytes_t encoded = {NULL, 0};
    kafka_common_Error_t *error = kafka_common_serialization_Serializer_serialize(serializer, "t", "h\xc3\xa9llo", &encoded);
    TEST_ASSERT_NULL(error);
    assert_bytes("h\xc3\xa9llo", encoded);

    void *decoded = NULL;
    error = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", encoded, &decoded);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("h\xc3\xa9llo", (const char *)decoded);
    kafka_string_destroy(decoded);

    // Java's null round-trips as a null array on the serializer side; the
    // deserializer turns a null array into an empty string, as the Rust
    // `Deserializer` takes a slice.
    error = kafka_common_serialization_Serializer_serialize(serializer, "t", NULL, &encoded);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(encoded.data);
    error = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", encoded, &decoded);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("", (const char *)decoded);
    kafka_string_destroy(decoded);

    // Invalid UTF-8 is the translation of Java's `SerializationException`.
    error = kafka_common_serialization_Serializer_serialize(serializer, "t", "h\xe9llo", &encoded);
    TEST_ASSERT_NOT_NULL(error);
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
    kafka_Bytes_t encoded = {NULL, 0};
    kafka_common_Error_t *error = kafka_common_serialization_Serializer_serialize(serializer, "t", &payload, &encoded);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_EQUAL_INT32(4, encoded.len);
    TEST_ASSERT_EQUAL_MEMORY(raw, encoded.data, 4);
    TEST_ASSERT_TRUE(encoded.data != raw);

    // The deserializer's `void *` is an owned `kafka_Bytes_t *`, a copy of
    // the input, freed with `kafka_Bytes_destroy`.
    void *decoded = NULL;
    error = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", encoded, &decoded);
    TEST_ASSERT_NULL(error);
    kafka_Bytes_t *decoded_bytes = decoded;
    TEST_ASSERT_EQUAL_INT32(4, decoded_bytes->len);
    TEST_ASSERT_EQUAL_MEMORY(raw, decoded_bytes->data, 4);
    TEST_ASSERT_TRUE(decoded_bytes->data != encoded.data);
    kafka_Bytes_destroy(decoded_bytes);

    // A NULL pointer and a null array are both Java's null.
    kafka_Bytes_t null_array = {NULL, 0};
    error = kafka_common_serialization_Serializer_serialize(serializer, "t", NULL, &encoded);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(encoded.data);
    error = kafka_common_serialization_Serializer_serialize(serializer, "t", &null_array, &encoded);
    TEST_ASSERT_NULL(error);
    TEST_ASSERT_NULL(encoded.data);

    kafka_common_serialization_ByteArraySerializer_destroy(array_serializer);
    kafka_common_serialization_ByteArrayDeserializer_destroy(array_deserializer);
    kafka_common_serialization_ByteArraySerializer_destroy(NULL);
    kafka_common_serialization_ByteArrayDeserializer_destroy(NULL);
}

static void test_bytes_deserializer_shares_the_source_through_the_view(void) {
    kafka_common_serialization_BytesDeserializer_t *bytes_deserializer = kafka_common_serialization_BytesDeserializer_new();
    kafka_common_serialization_Deserializer_t *deserializer =
        kafka_common_serialization_BytesDeserializer__as_Deserializer(bytes_deserializer);
    TEST_ASSERT_EQUAL_PTR(deserializer, kafka_common_serialization_BytesDeserializer__as_Deserializer(bytes_deserializer));

    const char *source = "hello world";
    kafka_Bytes_t data = {(const uint8_t *)source + 6, 5};
    void *decoded = NULL;
    kafka_common_Error_t *error =
        kafka_common_serialization_Deserializer_deserialize_from_shared(deserializer, "t", bytes_of(source), data, &decoded);
    TEST_ASSERT_NULL(error);
    kafka_Bytes_t *decoded_bytes = decoded;
    assert_bytes("world", *decoded_bytes);
    kafka_Bytes_destroy(decoded_bytes);

    error = kafka_common_serialization_Deserializer_deserialize(deserializer, "t", bytes_of("raw"), &decoded);
    TEST_ASSERT_NULL(error);
    decoded_bytes = decoded;
    assert_bytes("raw", *decoded_bytes);
    kafka_Bytes_destroy(decoded_bytes);
    kafka_Bytes_destroy(NULL);

    // Data outside the source is rejected with the same error as for a C
    // implementation.
    error = kafka_common_serialization_Deserializer_deserialize_from_shared(deserializer, "t", bytes_of(source),
                                                                            bytes_of("elsewhere"), &decoded);
    TEST_ASSERT_NOT_NULL(error);
    TEST_ASSERT_EQUAL_STRING("data must be a sub-array of source", kafka_common_Error_message(error));
    kafka_common_Error_destroy(error);

    kafka_common_serialization_BytesDeserializer_destroy(bytes_deserializer);
    kafka_common_serialization_BytesDeserializer_destroy(NULL);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_c_serializer_is_reached_through_the_invokers);
    RUN_TEST(test_c_deserializer_is_reached_through_the_invokers);
    RUN_TEST(test_string_serde_round_trip_through_the_views);
    RUN_TEST(test_byte_array_serde_round_trip_through_the_views);
    RUN_TEST(test_bytes_deserializer_shares_the_source_through_the_view);
    return UNITY_END();
}
