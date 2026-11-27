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

// A client's parsed configuration, a ProducerConfig or ConsumerConfig handle,
// as a JS handle object: the clients build it first, as Java's constructors
// parse their config first, read the client.id it generated for the serde
// config functions, and then build the client from it (ProducerConfig_new and
// KafkaProducer_new, ConsumerConfig_new and KafkaConsumer_new). JS frees it
// with configFree once the client is built or its construction failed; the
// handle object's finalizer frees one JS never freed, running no JS.

#include <stdatomic.h>
#include <stdlib.h>

#include "util.h"

typedef struct ConfigHandle {
    int consumer;  // a ConsumerConfig, else a ProducerConfig
    void* config;  // NULL once freed
} ConfigHandle;

static const napi_type_tag CONFIG_TAG = {0x6b636f6e66696731ULL, 0x6a7363666768646cULL};

static atomic_long stat_configs_made = 0;
static atomic_long stat_configs_freed = 0;

// Destroys the configuration a handle holds, once: an error the destroy
// reports is warned when `js` (JS may run), else only freed.
static void config_destroy(napi_env env, ConfigHandle* h, int js) {
    if (h->config == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err =
        h->consumer ? FFI_ERR(kafka_consumer_ConsumerConfig_destroy_err((kafka_consumer_ConsumerConfig_t*)h->config, &ffi_err))
                    : FFI_ERR(kafka_producer_ProducerConfig_destroy_err((kafka_producer_ProducerConfig_t*)h->config, &ffi_err));
    h->config = NULL;
    atomic_fetch_add(&stat_configs_freed, 1);
    error_warn(env, js, err);
}

static void config_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    ConfigHandle* h = (ConfigHandle*)data;
    config_destroy(env, h, 0);
    free(h);
}

// The configuration handle object `handle` holds, of the kind `consumer`
// asks, or NULL with a TypeError pending (another value, or one freed).
static ConfigHandle* config_unwrap(napi_env env, napi_value handle) {
    bool tagged = false;
    void* data = NULL;
    napi_valuetype type = napi_undefined;
    if (napi_typeof(env, handle, &type) != napi_ok || type != napi_object
        || napi_check_object_type_tag(env, handle, &CONFIG_TAG, &tagged) != napi_ok || !tagged
        || napi_unwrap(env, handle, &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, "not a config handle");
        return NULL;
    }
    return (ConfigHandle*)data;
}

void* config_handle_config(napi_env env, napi_value handle, int consumer) {
    ConfigHandle* h = config_unwrap(env, handle);
    if (h == NULL) return NULL;
    if (h->consumer != consumer || h->config == NULL) {
        napi_throw_type_error(env, NULL, h->config == NULL ? "the config handle was freed" : "a config handle of the other client");
        return NULL;
    }
    return h->config;
}

// configCreate(consumer, entries): the parsed configuration of a consumer (or
// a producer), `entries` an array of [key, value] byte pairs, as a handle
// object. Throws the core's error when the configuration is rejected.
napi_value js_config_create(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool consumer = false;
    bool is_array = false;
    if (argc != 2 || napi_get_value_bool(env, argv[0], &consumer) != napi_ok || napi_is_array(env, argv[1], &is_array) != napi_ok
        || !is_array) {
        napi_throw_type_error(env, NULL, "configCreate(consumer, entries)");
        return NULL;
    }
    ConfigMap props;
    if (config_map_build(env, argv[1], &props) != 0) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    void* config = NULL;
    // ConsumerConfig_new and ProducerConfig_new take an out_err of their own.
    kafka_common_Error_t* err = consumer ? FFI_SET(config, kafka_consumer_ConsumerConfig_new(props.map, &ffi_err))
                                         : FFI_SET(config, kafka_producer_ProducerConfig_new(props.map, &ffi_err));
    config_map_free(env, &props);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    atomic_fetch_add(&stat_configs_made, 1);
    ConfigHandle* h = (ConfigHandle*)calloc(1, sizeof(ConfigHandle));
    ConfigHandle stack = {consumer ? 1 : 0, config};
    if (h == NULL) {
        config_destroy(env, &stack, 1);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    *h = stack;
    napi_value obj;
    if (napi_create_object(env, &obj) != napi_ok || napi_type_tag_object(env, obj, &CONFIG_TAG) != napi_ok
        || napi_wrap(env, obj, h, config_finalize, NULL, NULL) != napi_ok) {
        js_throw_last_napi_error(env, "creating a config handle");
        config_destroy(env, h, 1);
        free(h);
        return NULL;
    }
    return obj;
}

// configGetString(handle, key): the STRING configuration `key` of a parsed
// configuration (`client.id` is the resolved id, generated when it was
// unset), or null for none. Throws the core's error for a key of another type
// or no such key.
napi_value js_config_get_string(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2) {
        napi_throw_type_error(env, NULL, "configGetString(handle, key)");
        return NULL;
    }
    ConfigHandle* h = config_unwrap(env, argv[0]);
    if (h == NULL) return NULL;
    if (h->config == NULL) {
        napi_throw_type_error(env, NULL, "the config handle was freed");
        return NULL;
    }
    char* key = js_string_dup(env, argv[1], "key must be a string");
    if (key == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const char* value = NULL;
    // The string is borrowed from the handle, which outlives this call.
    kafka_common_Error_t* err =
        h->consumer
            ? FFI_SET(value, kafka_consumer_ConsumerConfig_get_string((const kafka_consumer_ConsumerConfig_t*)h->config, key, &ffi_err))
            : FFI_SET(value, kafka_producer_ProducerConfig_get_string((const kafka_producer_ProducerConfig_t*)h->config, key, &ffi_err));
    free(key);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    if (value == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &result), NULL);
    } else {
        NAPI_CHECK(env, napi_create_string_utf8(env, value, NAPI_AUTO_LENGTH, &result), NULL);
    }
    return result;
}

// configFree(handle): frees a parsed configuration now; a second call does
// nothing.
napi_value js_config_free(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "configFree(handle)");
        return NULL;
    }
    ConfigHandle* h = config_unwrap(env, argv[0]);
    if (h == NULL) return NULL;
    config_destroy(env, h, 1);
    return NULL;
}

// configStats(): the counts of the parsed configurations made and freed.
// Test-only.
napi_value js_config_stats(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value obj, made, freed;
    if (napi_create_object(env, &obj) != napi_ok || napi_create_int64(env, (int64_t)atomic_load(&stat_configs_made), &made) != napi_ok
        || napi_create_int64(env, (int64_t)atomic_load(&stat_configs_freed), &freed) != napi_ok
        || napi_set_named_property(env, obj, "configsMade", made) != napi_ok
        || napi_set_named_property(env, obj, "configsFreed", freed) != napi_ok) {
        js_throw_last_napi_error(env, "configStats");
        return NULL;
    }
    return obj;
}
