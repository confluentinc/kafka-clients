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

// kafka_common_Error_t -> a JS error. The C layer reads the error through its
// accessors into a plain object {code, message, apiError, payload, cause} and hands it to the factory src/internal/native.ts registered:
// src/internal/errors.ts `fromNative` constructs the class of its code (KafkaError or a
// subclass, or a Java built-in such as IllegalStateError). An OWNED error is
// destroyed once read; a BORROWED one (a delivery callback's, a cause) never.

#include <stdlib.h>

#include "util.h"

// Causes deeper than this are cut: the chain is finite, this only bounds the
// recursion.
#define MAX_CAUSE_DEPTH 32

void error_discard(kafka_common_Error_t* e) {
    if (e == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_destroy_err(e, &ffi_err);
    // A panic inside the destroy is reported as one more owned error; freeing
    // that one cannot be retried further.
    kafka_common_Error_t* panic = ffi_take(&ffi_err);
    if (panic != NULL) kafka_common_Error_destroy_err(panic, &ffi_err);
}

void error_warn(napi_env env, int js, kafka_common_Error_t* e) {
    if (e == NULL) return;
    AddonData* data = addon_data(env);
    if (!js || data == NULL || data->warn == NULL) {
        error_discard(e);
        return;
    }
    // A JS exception already pending (the caller is throwing) is kept and
    // thrown again once the warning is out.
    bool pending = false;
    napi_value kept = NULL;
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) napi_get_and_clear_last_exception(env, &kept);
    napi_value value = error_take_to_js(env, e);
    napi_value fn, global, ignored;
    if (value != NULL && napi_get_reference_value(env, data->warn, &fn) == napi_ok && fn != NULL
        && napi_get_global(env, &global) == napi_ok) {
        napi_call_function(env, global, fn, 1, &value, &ignored);
    }
    // Neither the conversion nor the warning can be reported further.
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) napi_get_and_clear_last_exception(env, &ignored);
    if (kept != NULL) napi_throw(env, kept);
}

static int set_named(napi_env env, napi_value obj, const char* name, napi_value value) {
    return napi_set_named_property(env, obj, name, value) == napi_ok ? 0 : -1;
}

// The plain object for `e` (non-NULL), with its cause chain.
static napi_value error_object(napi_env env, const kafka_common_Error_t* e, int depth) {
    kafka_common_Error_t* ffi_err = NULL;
    int code = 0;
    const char* message = NULL;
    int8_t api = 0;
    const kafka_common_Error_t* source = NULL;
    error_discard(FFI_SET(code, (int)kafka_common_Error__enum_err(e, &ffi_err)));
    error_discard(FFI_SET(message, kafka_common_Error_message_err(e, &ffi_err)));
    error_discard(FFI_SET(api, kafka_common_Error_is_api_error_err(e, &ffi_err)));
    error_discard(FFI_SET(source, kafka_common_Error_source_err(e, &ffi_err)));

    // An error JS gave a mock comes back as the very same value (root
    // CLAUDE.md, JavaScript Binding Conventions, Errors, Q38).
    if (code == kafka_common_Error_LOCAL_CALLBACK) {
        const kafka_common_LocalCallbackError_t* local = NULL;
        void* opaque = NULL;
        error_discard(FFI_SET(local, kafka_common_Error_local_callback_error_err(e, &ffi_err)));
        if (local != NULL) error_discard(FFI_SET(opaque, kafka_common_LocalCallbackError_opaque_err(local, &ffi_err)));
        napi_value injected = opaque != NULL ? injected_error_value(env, opaque) : NULL;
        if (injected != NULL) return injected;
    }

    // apiError tells a bare KafkaException (UNKNOWN_SERVER_ERROR) from
    // UnknownServerError, which share the id.
    napi_value obj, v;
    if (napi_create_object(env, &obj) != napi_ok
        || napi_create_int32(env, code, &v) != napi_ok || set_named(env, obj, "code", v)
        || napi_create_string_utf8(env, message != NULL ? message : "", NAPI_AUTO_LENGTH, &v) != napi_ok
        || set_named(env, obj, "message", v)
        || napi_get_boolean(env, api != 0, &v) != napi_ok || set_named(env, obj, "apiError", v)) {
        return NULL;
    }
    // The payload the FFI's view of the class gives (Q39): its fields, keyed
    // by Java field name, which src/internal/throwable.ts reads.
    napi_value payload = error_payload_to_js(env, code, e);
    if (payload != NULL && set_named(env, obj, "payload", payload)) return NULL;
    if (source != NULL && depth < MAX_CAUSE_DEPTH) {
        napi_value cause = error_object(env, source, depth + 1);
        if (cause == NULL || set_named(env, obj, "cause", cause)) return NULL;
    }
    return obj;
}

// --- The payload helpers native/error_payload.c calls (util.h) ---

// Whether the accessor failed (`failed`, its owned error): the error is
// discarded, and the field left out.
static int accessor_failed(kafka_common_Error_t* failed) {
    if (failed == NULL) return 0;
    error_discard(failed);
    return 1;
}

// A borrowed C string as a JS string, NULL as null.
static napi_value string_or_null(napi_env env, const char* s) {
    napi_value v;
    if (s == NULL) return napi_get_null(env, &v) == napi_ok ? v : NULL;
    return napi_create_string_utf8(env, s, NAPI_AUTO_LENGTH, &v) == napi_ok ? v : NULL;
}

// A kafka_common_TopicPartition_t as [topic, partition].
static napi_value topic_partition_pair(napi_env env, const kafka_common_TopicPartition_t* tp) {
    kafka_common_Error_t* ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = -1;
    error_discard(FFI_SET(topic, kafka_common_TopicPartition_topic_err(tp, &ffi_err)));
    error_discard(FFI_SET(partition, kafka_common_TopicPartition_partition_err(tp, &ffi_err)));
    napi_value pair, t, p;
    if (napi_create_array_with_length(env, 2, &pair) != napi_ok || (t = string_or_null(env, topic)) == NULL
        || napi_create_int32(env, partition, &p) != napi_ok || napi_set_element(env, pair, 0, t) != napi_ok
        || napi_set_element(env, pair, 1, p) != napi_ok) {
        return NULL;
    }
    return pair;
}

// An owned kafka_List_t as an array, each element read by `element`; the list freed.
static int put_list(napi_env env, napi_value obj, const char* name, kafka_List_t* list,
                    napi_value (*element)(napi_env, const void*)) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    error_discard(FFI_SET(size, kafka_List_size_err(list, &ffi_err)));
    napi_value arr;
    int rc = napi_create_array_with_length(env, (size_t)size, &arr) == napi_ok ? 0 : -1;
    for (int32_t i = 0; i < size && rc == 0; i++) {
        const void* item = NULL;
        error_discard(FFI_SET(item, kafka_List_get_err(list, i, &ffi_err)));
        napi_value v = element(env, item);
        if (v == NULL || napi_set_element(env, arr, (uint32_t)i, v) != napi_ok) rc = -1;
    }
    error_discard(FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    if (rc == 0) rc = set_named(env, obj, name, arr);
    return rc;
}

static napi_value list_string(napi_env env, const void* item) {
    return string_or_null(env, (const char*)item);
}

static napi_value list_topic_partition(napi_env env, const void* item) {
    return topic_partition_pair(env, (const kafka_common_TopicPartition_t*)item);
}

int payload_string_list(napi_env env, napi_value obj, const char* name, kafka_List_t* list, kafka_common_Error_t* failed) {
    if (accessor_failed(failed) || list == NULL) {
        if (list != NULL) {
            kafka_common_Error_t* ffi_err = NULL;
            error_discard(FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
        }
        return 0;
    }
    return put_list(env, obj, name, list, list_string);
}

int payload_topic_partition_list(napi_env env, napi_value obj, const char* name, kafka_List_t* list,
                                 kafka_common_Error_t* failed) {
    if (accessor_failed(failed) || list == NULL) {
        if (list != NULL) {
            kafka_common_Error_t* ffi_err = NULL;
            error_discard(FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
        }
        return 0;
    }
    return put_list(env, obj, name, list, list_topic_partition);
}

// A map of kafka_common_TopicPartition_t * to int64_t * as an array of
// [topic, partition, offset] (a bigint); a NULL map, Java's null field, as null.
int payload_topic_partition_long_map(napi_env env, napi_value obj, const char* name, kafka_Map_t* map,
                                     kafka_common_Error_t* failed) {
    if (accessor_failed(failed)) {
        if (map != NULL) {
            kafka_common_Error_t* ffi_err = NULL;
            error_discard(FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
        }
        return 0;
    }
    napi_value arr;
    if (map == NULL) return napi_get_null(env, &arr) == napi_ok ? set_named(env, obj, name, arr) : -1;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    error_discard(FFI_SET(size, kafka_Map_size_err(map, &ffi_err)));
    int rc = napi_create_array_with_length(env, (size_t)size, &arr) == napi_ok ? 0 : -1;
    for (int32_t i = 0; i < size && rc == 0; i++) {
        const kafka_common_TopicPartition_t* tp = NULL;
        const int64_t* offset = NULL;
        error_discard(FFI_SET(tp, (const kafka_common_TopicPartition_t*)kafka_Map_key_err(map, i, &ffi_err)));
        error_discard(FFI_SET(offset, (const int64_t*)kafka_Map_value_err(map, i, &ffi_err)));
        napi_value entry = tp == NULL ? NULL : topic_partition_pair(env, tp);
        napi_value value;
        if (entry == NULL
            || (offset == NULL ? napi_get_null(env, &value) : napi_create_bigint_int64(env, *offset, &value)) != napi_ok
            || napi_set_element(env, entry, 2, value) != napi_ok || napi_set_element(env, arr, (uint32_t)i, entry) != napi_ok) {
            rc = -1;
        }
    }
    error_discard(FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
    if (rc == 0) rc = set_named(env, obj, name, arr);
    return rc;
}

int payload_string(napi_env env, napi_value obj, const char* name, const char* value, kafka_common_Error_t* failed) {
    if (accessor_failed(failed)) return 0;
    napi_value v = string_or_null(env, value);
    return v == NULL ? -1 : set_named(env, obj, name, v);
}

int payload_int32(napi_env env, napi_value obj, const char* name, int32_t value, kafka_common_Error_t* failed) {
    if (accessor_failed(failed)) return 0;
    napi_value v;
    return napi_create_int32(env, value, &v) == napi_ok ? set_named(env, obj, name, v) : -1;
}

int payload_double(napi_env env, napi_value obj, const char* name, double value, kafka_common_Error_t* failed) {
    if (accessor_failed(failed)) return 0;
    napi_value v;
    return napi_create_double(env, value, &v) == napi_ok ? set_named(env, obj, name, v) : -1;
}

// Wraps a plain error object through the registered factory, which also turns
// its `cause` into an error too.
static napi_value through_factory(napi_env env, napi_value obj) {
    AddonData* data = addon_data(env);
    if (data == NULL || data->error_factory == NULL) return obj;
    napi_value factory, global, result;
    if (napi_get_reference_value(env, data->error_factory, &factory) != napi_ok || factory == NULL
        || napi_get_global(env, &global) != napi_ok
        || napi_call_function(env, global, factory, 1, &obj, &result) != napi_ok) {
        return NULL;
    }
    return result;
}

napi_value error_to_js(napi_env env, const kafka_common_Error_t* e) {
    napi_value result;
    if (e == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &result), NULL);
        return result;
    }
    napi_value obj = error_object(env, e, 0);
    if (obj == NULL) {
        js_throw_last_napi_error(env, "converting a kafka_common_Error_t");
        return NULL;
    }
    return through_factory(env, obj);
}

napi_value error_take_to_js(napi_env env, kafka_common_Error_t* e) {
    napi_value result = error_to_js(env, e);
    error_discard(e);
    return result;
}

void error_throw_owned(napi_env env, kafka_common_Error_t* e) {
    napi_value err = error_take_to_js(env, e);
    if (err != NULL) napi_throw(env, err);
}

// setFactories(errorFactory, metadataFactory, warn): the JS constructors the
// C layer hands its plain objects to, and the function it hands an error with
// nowhere else to go (process.emitWarning). Called once, by src/, when it
// loads the addon.
napi_value js_set_factories(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    AddonData* data = addon_data(env);
    if (argc != 3 || data == NULL) {
        napi_throw_type_error(env, NULL, "setFactories(errorFactory, metadataFactory, warn)");
        return NULL;
    }
    delete_ref(env, &data->error_factory);
    delete_ref(env, &data->metadata_factory);
    delete_ref(env, &data->warn);
    NAPI_CHECK(env, napi_create_reference(env, argv[0], 1, &data->error_factory), NULL);
    NAPI_CHECK(env, napi_create_reference(env, argv[1], 1, &data->metadata_factory), NULL);
    NAPI_CHECK(env, napi_create_reference(env, argv[2], 1, &data->warn), NULL);
    return NULL;
}
