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

// Module initialisation of the Node-API addon `confluent_kafka_js`: the
// instance data, the probe and the producer's native functions. The public JS
// surface lives in lib/; this module exports only what lib/ calls.

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
    kafka_common_Error_t* err = (map = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    err = (kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err));
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
    };
    NAPI_CHECK(env, napi_define_properties(env, exports, sizeof(props) / sizeof(props[0]), props), NULL);
    return producer_init(env, exports);
}

NAPI_MODULE_INIT() {
    return addon_init(env, exports);
}
