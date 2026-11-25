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

// The values every client takes from JS the same way: its configuration as a
// kafka_Map_t of C strings, and lists of TopicPartitions.

#include <stdlib.h>

#include "util.h"

int config_map_build(napi_env env, napi_value entries, ConfigMap* out) {
    out->map = NULL;
    out->strings = NULL;
    out->count = 0;
    uint32_t count = 0;
    NAPI_CHECK(env, napi_get_array_length(env, entries, &count), -1);
    out->strings = (char**)calloc((size_t)count * 2 + 1, sizeof(char*));
    if (out->strings == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return -1;
    }
    out->count = count;
    kafka_common_Error_t* ffi_err = NULL;
    int thrown = 0;
    // A C-built kafka_Map_t borrows its char * keys and values: they are freed
    // after the map (config_map_free).
    kafka_common_Error_t* err = FFI_SET(out->map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && !thrown && i < count; i++) {
        napi_value entry, k, v;
        if (napi_get_element(env, entries, i, &entry) != napi_ok || napi_get_element(env, entry, 0, &k) != napi_ok
            || napi_get_element(env, entry, 1, &v) != napi_ok) {
            js_throw_last_napi_error(env, "reading a config entry");
            thrown = 1;
            break;
        }
        // Bytes encoded by encodeLikeJava (src/internal/config.ts).
        out->strings[2 * i] = js_bytes_cstring(env, k, "config keys must be strings");
        out->strings[2 * i + 1] = out->strings[2 * i] != NULL ? js_bytes_cstring(env, v, "config values must be strings") : NULL;
        if (out->strings[2 * i + 1] == NULL) {
            thrown = 1;
            break;
        }
        err = FFI_ERR(kafka_Map_put_err(out->map, out->strings[2 * i], out->strings[2 * i + 1], &ffi_err));
    }
    if (err == NULL && !thrown) return 0;
    config_map_free(env, out);
    if (err != NULL) error_throw_owned(env, err);
    return -1;
}

void config_map_free(napi_env env, ConfigMap* map) {
    kafka_common_Error_t* ffi_err = NULL;
    if (map->map != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map->map, &ffi_err)));
    map->map = NULL;
    if (map->strings != NULL)
        for (uint32_t i = 0; i < map->count * 2; i++) free(map->strings[i]);
    free(map->strings);
    map->strings = NULL;
    map->count = 0;
}
