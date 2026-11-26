// Copyright 2026 Confluent Inc.
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

// The record a send built, read back through the C API, for the tests only
// (recordSpy()): what a KafkaProducer send handed the core, its key and value
// compared by address with the caller's typed arrays, and its headers, in
// order, from the native record's own RecordHeaders.

#include "util.h"

// recordSpy(arm) keeps, while armed, what each KafkaProducer send built
// (record_spy_capture, from js_producer_send): the key and value src/ passed
// (keyArg, valueArg), whether the record's key and value point into those
// typed arrays' own memory, and the record read back through the C API. Test-only: nothing is kept unless a test armed it.
static int spy_armed;
static napi_env spy_env;
static napi_ref spy_records;

// Whether `b`, the bytes the record holds, is the memory of typed array `arr`.
static int spy_in_place(napi_env env, napi_value arr, const kafka_Bytes_t* b) {
    bool typed = false;
    if (b == NULL || napi_is_typedarray(env, arr, &typed) != napi_ok || !typed) return 0;
    napi_typedarray_type kind;
    size_t length = 0;
    void* data = NULL;
    if (napi_get_typedarray_info(env, arr, &kind, &length, &data, NULL, NULL) != napi_ok) return 0;
    return data != NULL && (const void*)b->data == data && (size_t)b->len == length;
}

// A copy of `b` as a Buffer, or null for Java's null.
static napi_value spy_bytes(napi_env env, const kafka_Bytes_t* b) {
    napi_value v = NULL;
    void* ignored = NULL;
    if (b == NULL || b->data == NULL) {
        if (napi_get_null(env, &v) != napi_ok) return NULL;
    } else if (napi_create_buffer_copy(env, (size_t)b->len, b->data, &ignored, &v) != napi_ok) {
        return NULL;
    }
    return v;
}

static int spy_set(napi_env env, napi_value obj, const char* name, napi_value v) {
    return v != NULL && napi_set_named_property(env, obj, name, v) == napi_ok ? 0 : -1;
}

// The record's headers, in order, as [key, value | null] pairs.
static napi_value spy_headers(napi_env env, const kafka_producer_ProducerRecord_t* record) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_header_internals_RecordHeaders_t* headers = NULL;
    kafka_common_header_Headers_t* view = NULL;
    kafka_List_t* list = NULL;
    error_discard(FFI_SET(headers, kafka_producer_ProducerRecord_headers_err(record, &ffi_err)));
    if (headers != NULL)
        error_discard(FFI_SET(view, kafka_common_header_internals_RecordHeaders__as_Headers_err(headers, &ffi_err)));
    if (view != NULL) error_discard(FFI_SET(list, kafka_common_header_Headers_to_array_err(view, &ffi_err)));
    napi_value arr = NULL;
    int32_t size = 0;
    if (list != NULL) error_discard(FFI_SET(size, kafka_List_size_err(list, &ffi_err)));
    if (napi_create_array_with_length(env, (size_t)size, &arr) != napi_ok) arr = NULL;
    for (int32_t i = 0; i < size && arr != NULL; i++) {
        const kafka_common_header_internals_RecordHeader_t* rh = NULL;
        const kafka_common_header_Header_t* h = NULL;
        const char* key = NULL;
        kafka_Bytes_t value = {NULL, 0};
        error_discard(FFI_SET(rh, (const kafka_common_header_internals_RecordHeader_t*)kafka_List_get_err(list, i, &ffi_err)));
        if (rh != NULL) error_discard(FFI_SET(h, kafka_common_header_internals_RecordHeader__as_Header_err(rh, &ffi_err)));
        if (h != NULL) {
            error_discard(FFI_SET(key, kafka_common_header_Header_key_err(h, &ffi_err)));
            error_discard(FFI_SET(value, kafka_common_header_Header_value_err(h, &ffi_err)));
        }
        napi_value pair, k, v = spy_bytes(env, &value);
        if (v == NULL || napi_create_array_with_length(env, 2, &pair) != napi_ok
            || napi_create_string_utf8(env, key != NULL ? key : "", NAPI_AUTO_LENGTH, &k) != napi_ok
            || napi_set_element(env, pair, 0, k) != napi_ok || napi_set_element(env, pair, 1, v) != napi_ok
            || napi_set_element(env, arr, (uint32_t)i, pair) != napi_ok)
            arr = NULL;
    }
    if (list != NULL) error_discard(FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    return arr;
}

void record_spy_capture(napi_env env, const kafka_producer_ProducerRecord_t* record, napi_value key, napi_value value) {
    if (!spy_armed || spy_env != env || spy_records == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = -1;
    int64_t timestamp = -1;
    const kafka_Bytes_t* k = NULL;
    const kafka_Bytes_t* v = NULL;
    error_discard(FFI_SET(topic, kafka_producer_ProducerRecord_topic_err(record, &ffi_err)));
    error_discard(FFI_SET(partition, kafka_producer_ProducerRecord_partition_err(record, &ffi_err)));
    error_discard(FFI_SET(timestamp, kafka_producer_ProducerRecord_timestamp_err(record, &ffi_err)));
    error_discard(FFI_SET(k, (const kafka_Bytes_t*)kafka_producer_ProducerRecord_key_err(record, &ffi_err)));
    error_discard(FFI_SET(v, (const kafka_Bytes_t*)kafka_producer_ProducerRecord_value_err(record, &ffi_err)));
    napi_value obj, list, x;
    uint32_t n = 0;
    int failed = napi_create_object(env, &obj) != napi_ok
                 || napi_create_string_utf8(env, topic != NULL ? topic : "", NAPI_AUTO_LENGTH, &x) != napi_ok
                 || spy_set(env, obj, "topic", x) || napi_create_int32(env, partition, &x) != napi_ok
                 || spy_set(env, obj, "partition", x) || napi_create_bigint_int64(env, timestamp, &x) != napi_ok
                 || spy_set(env, obj, "timestamp", x) || spy_set(env, obj, "key", spy_bytes(env, k))
                 || spy_set(env, obj, "value", spy_bytes(env, v))
                 || napi_get_boolean(env, spy_in_place(env, key, k), &x) != napi_ok || spy_set(env, obj, "keyInPlace", x)
                 || napi_get_boolean(env, spy_in_place(env, value, v), &x) != napi_ok
                 || spy_set(env, obj, "valueInPlace", x) || spy_set(env, obj, "keyArg", key)
                 || spy_set(env, obj, "valueArg", value) || spy_set(env, obj, "headers", spy_headers(env, record))
                 || napi_get_reference_value(env, spy_records, &list) != napi_ok
                 || napi_get_array_length(env, list, &n) != napi_ok || napi_set_element(env, list, n, obj) != napi_ok;
    if (failed) {
        // The send goes on as if nothing were armed.
        napi_value ignored;
        napi_get_and_clear_last_exception(env, &ignored);
    }
}

// recordSpy(arm): the records captured since the last call (an empty array
// when none was armed), then arms the capture again for `arm` true, or stops
// it. Test-only.
napi_value js_record_spy(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool arm = false;
    if (argc != 1 || napi_get_value_bool(env, argv[0], &arm) != napi_ok) {
        napi_throw_type_error(env, NULL, "recordSpy(arm)");
        return NULL;
    }
    napi_value result = NULL;
    if (spy_records != NULL && spy_env == env) {
        NAPI_CHECK(env, napi_get_reference_value(env, spy_records, &result), NULL);
        napi_delete_reference(env, spy_records);
        spy_records = NULL;
    }
    if (result == NULL) NAPI_CHECK(env, napi_create_array(env, &result), NULL);
    spy_armed = 0;
    if (arm) {
        napi_value list;
        NAPI_CHECK(env, napi_create_array(env, &list), NULL);
        NAPI_CHECK(env, napi_create_reference(env, list, 1, &spy_records), NULL);
        spy_env = env;
        spy_armed = 1;
    }
    return result;
}
