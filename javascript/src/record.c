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

// JS values -> the C ProducerRecord. lib/producer/producer_record.js has
// already turned strings into Buffers and checked the types Java's
// constructor checks, so this reads Uint8Arrays (a Buffer is one) as
// kafka_Bytes_t over their own memory: no copy of a key or value is made here
// (javascript/CLAUDE.md, rule 8). The producer was built with NULL
// serializers, so the record's `void *` key and value are `kafka_Bytes_t *`.

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
        char* key = js_string_dup(env, k, "a header key must be a string");
        if (key == NULL) {
            *thrown = 1;
            return NULL;
        }
        JsBytes value;
        if (js_bytes(env, v, "a header value must be a Buffer, a Uint8Array, a string or null", &value)) {
            free(key);
            *thrown = 1;
            return NULL;
        }
        kafka_common_Error_t* err =
            (kafka_common_header_Headers_add_with_key_value(view, key, value.bytes, &ffi_err), ffi_take(&ffi_err));
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
        (record_headers = kafka_common_header_internals_RecordHeaders_new_err(&ffi_err), ffi_take(&ffi_err));
    if (err == NULL)
        err = (view = kafka_common_header_internals_RecordHeaders__as_Headers_err(record_headers, &ffi_err),
               ffi_take(&ffi_err));
    if (err == NULL) err = add_headers(env, view, headers, &thrown);
    if (err == NULL && !thrown)
        err = (builder = kafka_producer_ProducerRecordOptionsBuilder_new_err(&ffi_err), ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_topic_err(builder, topic, &ffi_err),
               ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_partition_err(builder, partition, &ffi_err),
               ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_timestamp_err(builder, timestamp, &ffi_err),
               ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_key_err(
                   builder, key->is_null ? NULL : (const void*)&key->bytes, &ffi_err),
               ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_value_err(
                   builder, value->is_null ? NULL : (const void*)&value->bytes, &ffi_err),
               ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_headers_err(builder, record_headers, &ffi_err),
               ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (options = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &ffi_err), ffi_take(&ffi_err));
    if (err == NULL && !thrown)
        err = (record = kafka_producer_ProducerRecord_with_options(options, &ffi_err), ffi_take(&ffi_err));

    if (options != NULL) error_discard((kafka_producer_ProducerRecordOptions_destroy_err(options, &ffi_err), ffi_take(&ffi_err)));
    if (builder != NULL)
        error_discard((kafka_producer_ProducerRecordOptionsBuilder_destroy_err(builder, &ffi_err), ffi_take(&ffi_err)));
    if (record_headers != NULL)
        error_discard(
            (kafka_common_header_internals_RecordHeaders_destroy_err(record_headers, &ffi_err), ffi_take(&ffi_err)));
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
    kafka_common_Error_t* err = (record = kafka_producer_ProducerRecord_with_partition_timestamp_key(
                                     topic, partition, timestamp, key->is_null ? NULL : (const void*)&key->bytes,
                                     value->is_null ? NULL : (const void*)&value->bytes, &ffi_err),
                                 ffi_take(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return record;
}
