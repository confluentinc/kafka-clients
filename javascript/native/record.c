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

// JS values -> the C ProducerRecord. src/producer/producer_record.ts has
// already turned strings into Buffers of the client's own (src/internal/
// encode.ts), header keys included, and checked the types Java's constructor
// checks, so this reads Uint8Arrays (a Buffer is one) as kafka_Bytes_t. A key
// or value the app handed in is copied once into memory the addon owns
// (js_bytes_copy), freed when the record completes, so the app may change or
// detach its Buffer as soon as send() returns; the client's own Buffers are
// read over their memory, kept by a reference until then (javascript/CLAUDE.md,
// rule 8). Header values are read over their memory during the call, in which
// the core copies them. The
// producer was built with NULL serializers, so the record's `void *` key and
// value are `kafka_Bytes_t *`.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

// What an empty array points at: `data == NULL` is Java's null.
static const uint8_t EMPTY_BYTES[1] = {0};

int js_bytes(napi_env env, napi_value value, const char* what, JsBytes* out) {
    memset(out, 0, sizeof(*out));
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok) {
        js_throw_last_napi_error(env, "napi_typeof");
        return -1;
    }
    if (type == napi_null || type == napi_undefined) {
        out->is_null = 1;
        return 0;
    }
    bool is_typed = false;
    if (napi_is_typedarray(env, value, &is_typed) != napi_ok || !is_typed) {
        napi_throw_type_error(env, NULL, what);
        return -1;
    }
    napi_typedarray_type kind;
    size_t length = 0;
    void* data = NULL;
    if (napi_get_typedarray_info(env, value, &kind, &length, &data, NULL, NULL) != napi_ok) {
        js_throw_last_napi_error(env, "napi_get_typedarray_info");
        return -1;
    }
    if (kind != napi_uint8_array) {
        napi_throw_type_error(env, NULL, what);
        return -1;
    }
    if (length > INT32_MAX) {
        napi_throw_range_error(env, NULL, "a byte array crossing the C FFI is limited to 2^31 - 1 bytes");
        return -1;
    }
    out->bytes.data = (length == 0 || data == NULL) ? EMPTY_BYTES : (const uint8_t*)data;
    out->bytes.len = (int32_t)length;
    return 0;
}

int js_bytes_copy(napi_env env, JsBytes* key, JsBytes* value, int owned, uint8_t** copy) {
    *copy = NULL;
    // Only the app's non-empty bytes: an empty array points at EMPTY_BYTES.
    size_t key_len = (!(owned & 1) && !key->is_null) ? (size_t)key->bytes.len : 0;
    size_t value_len = (!(owned & 2) && !value->is_null) ? (size_t)value->bytes.len : 0;
    if (key_len + value_len == 0) return 0;
    uint8_t* bytes = (uint8_t*)malloc(key_len + value_len);
    if (bytes == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return -1;
    }
    if (key_len > 0) {
        memcpy(bytes, key->bytes.data, key_len);
        key->bytes.data = bytes;
    }
    if (value_len > 0) {
        memcpy(bytes + key_len, value->bytes.data, value_len);
        value->bytes.data = bytes + key_len;
    }
    *copy = bytes;
    return 0;
}

char* js_string_dup(napi_env env, napi_value value, const char* what) {
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok || type != napi_string) {
        napi_throw_type_error(env, NULL, what);
        return NULL;
    }
    size_t len = 0;
    if (napi_get_value_string_utf8(env, value, NULL, 0, &len) != napi_ok) {
        js_throw_last_napi_error(env, "napi_get_value_string_utf8");
        return NULL;
    }
    char* s = (char*)malloc(len + 1);
    if (s == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    if (napi_get_value_string_utf8(env, value, s, len + 1, &len) != napi_ok) {
        free(s);
        js_throw_last_napi_error(env, "napi_get_value_string_utf8");
        return NULL;
    }
    // Every C reader stops at the first NUL, so a string holding one would be
    // silently truncated at the ABI (javascript/CLAUDE.md, rule 1): refuse it,
    // in Node's own words for the same limit (ERR_INVALID_ARG_VALUE).
    if (memchr(s, 0, len) != NULL) {
        free(s);
        char message[256];
        snprintf(message, sizeof(message), "%s without null bytes", what);
        napi_throw_type_error(env, NULL, message);
        return NULL;
    }
    return s;
}

char* js_bytes_cstring(napi_env env, napi_value value, const char* what) {
    JsBytes bytes;
    if (js_bytes(env, value, what, &bytes)) return NULL;
    if (bytes.is_null) {
        napi_throw_type_error(env, NULL, what);
        return NULL;
    }
    size_t len = (size_t)bytes.bytes.len;
    // Every C reader stops at the first NUL, so bytes holding one would be
    // silently truncated at the ABI (javascript/CLAUDE.md, rule 1): refuse
    // them, in Node's own words for the same limit (ERR_INVALID_ARG_VALUE).
    if (len > 0 && memchr(bytes.bytes.data, 0, len) != NULL) {
        char message[256];
        snprintf(message, sizeof(message), "%s without null bytes", what);
        napi_throw_type_error(env, NULL, message);
        return NULL;
    }
    char* s = (char*)malloc(len + 1);
    if (s == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    if (len > 0) memcpy(s, bytes.bytes.data, len);
    s[len] = '\0';
    return s;
}

// Fills `view` from the `[key, value]` pairs; the core copies each header.
// Returns NULL, or the owned error of the first failing call; throws (and
// returns NULL with `*thrown` set) for a value of the wrong JS type.
static kafka_common_Error_t* add_headers(napi_env env, kafka_common_header_Headers_t* view, napi_value headers,
                                         int* thrown) {
    kafka_common_Error_t* ffi_err = NULL;
    uint32_t count = 0;
    if (napi_get_array_length(env, headers, &count) != napi_ok) {
        napi_throw_type_error(env, NULL, "headers must be an array");
        *thrown = 1;
        return NULL;
    }
    for (uint32_t i = 0; i < count; i++) {
        napi_value pair, k, v;
        if (napi_get_element(env, headers, i, &pair) != napi_ok
            || napi_get_element(env, pair, 0, &k) != napi_ok
            || napi_get_element(env, pair, 1, &v) != napi_ok) {
            js_throw_last_napi_error(env, "reading a header");
            *thrown = 1;
            return NULL;
        }
        // The key's bytes, encodeLikeJava's (src/internal/encode.ts).
        char* key = js_bytes_cstring(env, k, "a header key must be a string");
        if (key == NULL) {
            *thrown = 1;
            return NULL;
        }
        JsBytes value;
        if (js_bytes(env, v, "a header value must be a Buffer, a Uint8Array or null", &value)) {
            free(key);
            *thrown = 1;
            return NULL;
        }
        // Headers_add_with_key_value takes an out_err of its own.
        kafka_common_Error_t* err = FFI_ERR(kafka_common_header_Headers_add_with_key_value(view, key, value.bytes, &ffi_err));
        free(key);
        if (err != NULL) return err;
    }
    return NULL;
}

// The record with headers: ProducerRecord(topic, partition, timestamp, key,
// value, headers) through its options builder.
static kafka_producer_ProducerRecord_t* record_with_headers(napi_env env, const char* topic, int32_t partition,
                                                            int64_t timestamp, const JsBytes* key,
                                                            const JsBytes* value, napi_value headers) {
    kafka_common_Error_t* ffi_err = NULL;
    int thrown = 0;
    kafka_common_header_internals_RecordHeaders_t* record_headers = NULL;
    kafka_common_header_Headers_t* view = NULL;
    kafka_producer_ProducerRecordOptionsBuilder_t* builder = NULL;
    kafka_producer_ProducerRecordOptions_t* options = NULL;
    kafka_producer_ProducerRecord_t* record = NULL;

    kafka_common_Error_t* err =
        FFI_SET(record_headers, kafka_common_header_internals_RecordHeaders_new_err(&ffi_err));
    if (err == NULL) err = FFI_SET(view, kafka_common_header_internals_RecordHeaders__as_Headers_err(record_headers, &ffi_err));
    if (err == NULL) err = add_headers(env, view, headers, &thrown);
    if (err == NULL && !thrown) err = FFI_SET(builder, kafka_producer_ProducerRecordOptionsBuilder_new_err(&ffi_err));
    if (err == NULL && !thrown)
        err = FFI_ERR(kafka_producer_ProducerRecordOptionsBuilder_set_topic_err(builder, topic, &ffi_err));
    if (err == NULL && !thrown)
        err = FFI_ERR(kafka_producer_ProducerRecordOptionsBuilder_set_partition_err(builder, partition, &ffi_err));
    if (err == NULL && !thrown)
        err = FFI_ERR(kafka_producer_ProducerRecordOptionsBuilder_set_timestamp_err(builder, timestamp, &ffi_err));
    if (err == NULL && !thrown)
        err = FFI_ERR(kafka_producer_ProducerRecordOptionsBuilder_set_key_err(
            builder, key->is_null ? NULL : (const void*)&key->bytes, &ffi_err));
    if (err == NULL && !thrown)
        err = FFI_ERR(kafka_producer_ProducerRecordOptionsBuilder_set_value_err(
            builder, value->is_null ? NULL : (const void*)&value->bytes, &ffi_err));
    if (err == NULL && !thrown)
        err = FFI_ERR(kafka_producer_ProducerRecordOptionsBuilder_set_headers_err(builder, record_headers, &ffi_err));
    // _build and ProducerRecord_with_options take an out_err of their own.
    if (err == NULL && !thrown) err = FFI_SET(options, kafka_producer_ProducerRecordOptionsBuilder_build(builder, &ffi_err));
    if (err == NULL && !thrown) err = FFI_SET(record, kafka_producer_ProducerRecord_with_options(options, &ffi_err));

    if (options != NULL) error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecordOptions_destroy_err(options, &ffi_err)));
    if (builder != NULL)
        error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecordOptionsBuilder_destroy_err(builder, &ffi_err)));
    if (record_headers != NULL)
        error_warn(env, 1, FFI_ERR(kafka_common_header_internals_RecordHeaders_destroy_err(record_headers, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return thrown ? NULL : record;
}

kafka_producer_ProducerRecord_t* record_build(napi_env env, const char* topic, int32_t partition,
                                              int64_t timestamp, const JsBytes* key, const JsBytes* value,
                                              napi_value headers) {
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, headers, &type), NULL);
    if (type != napi_null && type != napi_undefined) {
        return record_with_headers(env, topic, partition, timestamp, key, value, headers);
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_producer_ProducerRecord_t* record = NULL;
    kafka_common_Error_t* err = FFI_SET(record, kafka_producer_ProducerRecord_with_partition_timestamp_key(
                                                    topic, partition, timestamp,
                                                    key->is_null ? NULL : (const void*)&key->bytes,
                                                    value->is_null ? NULL : (const void*)&value->bytes, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return record;
}
