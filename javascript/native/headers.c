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

// The copy of a record's headers that a send hands the core: Headers and
// RecordHeader are plain TypeScript classes, and the core gets a copy of the
// headers only at send() (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Serialization). Each key and value goes into a native
// RecordHeaders made for the call, which the record's options builder copies
// and native/record.c then destroys.

#include <stdatomic.h>
#include <stdlib.h>

#include "util.h"

// The counts headerStats() reads, for the memory tests.
static atomic_long stat_made;
static atomic_long stat_freed;

kafka_common_header_internals_RecordHeaders_t* headers_build(napi_env env, napi_value pairs) {
    uint32_t length = 0;
    NAPI_CHECK(env, napi_get_array_length(env, pairs, &length), NULL);
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_header_internals_RecordHeaders_t* headers = NULL;
    kafka_common_Error_t* err = FFI_SET(headers, kafka_common_header_internals_RecordHeaders_new_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    atomic_fetch_add(&stat_made, 1);
    kafka_common_header_Headers_t* view = NULL;
    err = FFI_SET(view, kafka_common_header_internals_RecordHeaders__as_Headers_err(headers, &ffi_err));
    int thrown = 0;
    for (uint32_t i = 0; err == NULL && !thrown && i + 1 < length; i += 2) {
        napi_value key_value;
        napi_value value_value;
        if (napi_get_element(env, pairs, i, &key_value) != napi_ok
            || napi_get_element(env, pairs, i + 1, &value_value) != napi_ok) {
            js_throw_last_napi_error(env, "napi_get_element");
            thrown = 1;
            break;
        }
        // The key's C string: a NUL in it is refused (javascript/CLAUDE.md, rule 1).
        char* key = js_bytes_cstring(env, key_value, "a header key must be a string");
        if (key == NULL) {
            thrown = 1;
            break;
        }
        JsBytes value;
        if (js_bytes(env, value_value, "a header value must be a Buffer, a Uint8Array or null", &value)) {
            free(key);
            thrown = 1;
            break;
        }
        // Headers_add_with_key_value takes an out_err of its own; it copies the
        // key and the value.
        err = FFI_ERR(kafka_common_header_Headers_add_with_key_value(view, key, value.bytes, &ffi_err));
        free(key);
    }
    if (err == NULL && !thrown) return headers;
    headers_destroy(env, headers);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

void headers_destroy(napi_env env, kafka_common_header_internals_RecordHeaders_t* headers) {
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 1, FFI_ERR(kafka_common_header_internals_RecordHeaders_destroy_err(headers, &ffi_err)));
    atomic_fetch_add(&stat_freed, 1);
}

// headerStats(): the counts of the native RecordHeaders the sends made and
// destroyed. Test-only.
napi_value js_header_stats(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value obj;
    napi_value made;
    napi_value freed;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_create_int64(env, (int64_t)atomic_load(&stat_made), &made), NULL);
    NAPI_CHECK(env, napi_create_int64(env, (int64_t)atomic_load(&stat_freed), &freed), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "made", made), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "freed", freed), NULL);
    return obj;
}
