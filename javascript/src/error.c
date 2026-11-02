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

// kafka_common_Error_t -> KafkaError. The C layer reads the error through its
// accessors into a plain object {code, name, message, retriable, apiError,
// cause} and hands it to the factory lib/common/kafka_error.js registered, so
// the JS class is the only place a KafkaError is built. An OWNED error is
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

static int set_named(napi_env env, napi_value obj, const char* name, napi_value value) {
    return napi_set_named_property(env, obj, name, value) == napi_ok ? 0 : -1;
}

// The plain object for `e` (non-NULL), with its cause chain.
static napi_value error_object(napi_env env, const kafka_common_Error_t* e, int depth) {
    kafka_common_Error_t* ffi_err = NULL;
    int code = (int)kafka_common_Error__enum_err(e, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    const char* message = kafka_common_Error_message_err(e, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    int8_t retriable = kafka_common_Error_is_retriable_error_err(e, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    int8_t api = kafka_common_Error_is_api_error_err(e, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    const kafka_common_Error_t* source = kafka_common_Error_source_err(e, &ffi_err);
    error_discard(ffi_take(&ffi_err));

    napi_value obj, v;
    const char* name = error_name(code);
    if (napi_create_object(env, &obj) != napi_ok
        || napi_create_int32(env, code, &v) != napi_ok || set_named(env, obj, "code", v)
        || (name != NULL ? napi_create_string_utf8(env, name, NAPI_AUTO_LENGTH, &v) : napi_get_null(env, &v)) != napi_ok
        || set_named(env, obj, "name", v)
        || napi_create_string_utf8(env, message != NULL ? message : "", NAPI_AUTO_LENGTH, &v) != napi_ok
        || set_named(env, obj, "message", v)
        || napi_get_boolean(env, retriable != 0, &v) != napi_ok || set_named(env, obj, "retriable", v)
        || napi_get_boolean(env, api != 0, &v) != napi_ok || set_named(env, obj, "apiError", v)) {
        return NULL;
    }
    if (source != NULL && depth < MAX_CAUSE_DEPTH) {
        napi_value cause = error_object(env, source, depth + 1);
        if (cause == NULL || set_named(env, obj, "cause", cause)) return NULL;
    }
    return obj;
}

// Wraps a plain error object through the registered factory, which also turns
// its `cause` into a KafkaError.
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

// setFactories(errorFactory, metadataFactory): the JS constructors the C layer
// hands its plain objects to. Called once, by lib/, when it loads the addon.
napi_value js_set_factories(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    AddonData* data = addon_data(env);
    if (argc != 2 || data == NULL) {
        napi_throw_type_error(env, NULL, "setFactories(errorFactory, metadataFactory)");
        return NULL;
    }
    if (data->error_factory != NULL) napi_delete_reference(env, data->error_factory);
    if (data->metadata_factory != NULL) napi_delete_reference(env, data->metadata_factory);
    data->error_factory = NULL;
    data->metadata_factory = NULL;
    NAPI_CHECK(env, napi_create_reference(env, argv[0], 1, &data->error_factory), NULL);
    NAPI_CHECK(env, napi_create_reference(env, argv[1], 1, &data->metadata_factory), NULL);
    return NULL;
}
