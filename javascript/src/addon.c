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

// The entry point of the Node-API addon `confluent-kafka-javascript`: the instance
// data, the helpers every client uses, and the registration of every native
// function. The public JS surface lives in lib/; the addon exports only what
// lib/ calls, all of it registered here.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

AddonData* addon_data(napi_env env) {
    void* data = NULL;
    if (napi_get_instance_data(env, &data) != napi_ok) return NULL;
    return (AddonData*)data;
}

void js_throw_last_napi_error(napi_env env, const char* what) {
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) return;
    const napi_extended_error_info* info = NULL;
    napi_get_last_error_info(env, &info);
    char message[512];
    snprintf(message, sizeof(message), "Node-API call failed: %s (%s)", what,
             (info != NULL && info->error_message != NULL) ? info->error_message : "unknown error");
    napi_throw_error(env, NULL, message);
}

void delete_ref(napi_env env, napi_ref* ref) {
    if (*ref != NULL) {
        napi_delete_reference(env, *ref);
        *ref = NULL;
    }
}

napi_value call_ref(napi_env env, napi_ref ref, size_t argc, napi_value* argv, char** thrown) {
    napi_value fn, global, result = NULL;
    if (napi_get_reference_value(env, ref, &fn) != napi_ok || fn == NULL || napi_get_global(env, &global) != napi_ok)
        return NULL;
    napi_status status = napi_call_function(env, global, fn, argc, argv, &result);
    if (status == napi_ok) return result;
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) {
        napi_value exc, text;
        napi_get_and_clear_last_exception(env, &exc);
        if (thrown != NULL && napi_coerce_to_string(env, exc, &text) == napi_ok) {
            size_t len = 0;
            if (napi_get_value_string_utf8(env, text, NULL, 0, &len) == napi_ok) {
                char* s = (char*)malloc(len + 1);
                if (s != NULL && napi_get_value_string_utf8(env, text, s, len + 1, &len) == napi_ok) *thrown = s;
                else free(s);
            }
        }
    }
    return NULL;
}

static void addon_data_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    AddonData* d = (AddonData*)data;
    if (d->error_factory != NULL) napi_delete_reference(env, d->error_factory);
    if (d->metadata_factory != NULL) napi_delete_reference(env, d->metadata_factory);
    free(d);
}

// probe() -> true: proves the addon loaded and its link to libconfluent_kafka
// resolves, by building and freeing an empty kafka_Map_t.
static napi_value js_probe(napi_env env, napi_callback_info info) {
    (void)info;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    err = FFI_ERR(kafka_Map_destroy_err(map, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, true, &result), NULL);
    return result;
}

static napi_value addon_init(napi_env env, napi_value exports) {
    AddonData* data = (AddonData*)calloc(1, sizeof(AddonData));
    if (data == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    if (napi_set_instance_data(env, data, addon_data_finalize, NULL) != napi_ok) {
        free(data);
        js_throw_last_napi_error(env, "napi_set_instance_data");
        return NULL;
    }
    napi_property_descriptor props[] = {
        {"probe", NULL, js_probe, NULL, NULL, NULL, napi_default, NULL},
        {"setFactories", NULL, js_set_factories, NULL, NULL, NULL, napi_default, NULL},
        // producer.c
        {"producerCreate", NULL, js_producer_create, NULL, NULL, NULL, napi_default, NULL},
        {"producerSend", NULL, js_producer_send, NULL, NULL, NULL, napi_default, NULL},
        {"producerFlush", NULL, js_producer_flush, NULL, NULL, NULL, napi_default, NULL},
        {"producerClose", NULL, js_producer_close, NULL, NULL, NULL, napi_default, NULL},
        {"producerPump", NULL, js_producer_pump, NULL, NULL, NULL, napi_default, NULL},
    };
    NAPI_CHECK(env, napi_define_properties(env, exports, sizeof(props) / sizeof(props[0]), props), NULL);
    return exports;
}

NAPI_MODULE_INIT() {
    return addon_init(env, exports);
}
