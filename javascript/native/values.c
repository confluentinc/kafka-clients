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

// The values the clients take from JS the same way: a configuration as a
// kafka_Map_t of C strings, lists of TopicPartitions, 64-bit integers, and the
// offsets a commit or a seek takes.

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

void tp_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_TopicPartition_destroy_err((kafka_common_TopicPartition_t*)item, out_err);
}

kafka_common_TopicPartition_t* tp_from_js(napi_env env, napi_value pair) {
    napi_value t, p;
    if (napi_get_element(env, pair, 0, &t) != napi_ok || napi_get_element(env, pair, 1, &p) != napi_ok) {
        js_throw_last_napi_error(env, "napi_get_element");
        return NULL;
    }
    int32_t partition = 0;
    if (napi_get_value_int32(env, p, &partition) != napi_ok) {
        napi_throw_type_error(env, NULL, "a partition's index must be a number");
        return NULL;
    }
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, t, &type), NULL);
    // A null topic crosses as NULL, which the FFI reports.
    char* topic = type == napi_null ? NULL : js_string_dup(env, t, "a partition's topic must be a string");
    if (type != napi_null && topic == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_TopicPartition_t* tp = NULL;
    kafka_common_Error_t* err = FFI_SET(tp, kafka_common_TopicPartition_new_err(topic, partition, &ffi_err));
    free(topic);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return tp;
}

void list_free(napi_env env, kafka_List_t* list, void (*destroy)(void*, kafka_common_Error_t**)) {
    if (list == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    for (int32_t i = 0; err == NULL && destroy != NULL && i < n; i++) {
        void* item = NULL;
        err = FFI_SET(item, kafka_List_get_err(list, i, &ffi_err));
        if (err == NULL && item != NULL) err = FFI_ERR(destroy(item, &ffi_err));
    }
    error_warn(env, 1, err);
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
}

kafka_List_t* tp_list_from_js(napi_env env, napi_value array) {
    uint32_t n = 0;
    if (napi_get_array_length(env, array, &n) != napi_ok) {
        napi_throw_type_error(env, NULL, "partitions must be an array");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_List_new_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    for (uint32_t i = 0; i < n; i++) {
        napi_value v;
        napi_valuetype type = napi_undefined;
        if (napi_get_element(env, array, i, &v) != napi_ok || napi_typeof(env, v, &type) != napi_ok) {
            js_throw_last_napi_error(env, "napi_get_element");
            list_free(env, list, tp_item_destroy);
            return NULL;
        }
        // A null element crosses as NULL, which the FFI reports.
        kafka_common_TopicPartition_t* tp = NULL;
        if (type != napi_null && (tp = tp_from_js(env, v)) == NULL) {
            list_free(env, list, tp_item_destroy);
            return NULL;
        }
        err = FFI_ERR(kafka_List_add_err(list, tp, &ffi_err));
        if (err != NULL) {
            if (tp != NULL) FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            list_free(env, list, tp_item_destroy);
            error_throw_owned(env, err);
            return NULL;
        }
    }
    return list;
}

napi_value tp_list_take_to_js(napi_env env, kafka_List_t* list) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    napi_value arr = NULL;
    if (err == NULL && napi_create_array_with_length(env, (size_t)n, &arr) != napi_ok) {
        js_throw_last_napi_error(env, "napi_create_array_with_length");
        arr = NULL;
    }
    for (int32_t i = 0; err == NULL && arr != NULL && i < n; i++) {
        const kafka_common_TopicPartition_t* tp = NULL;
        err = FFI_SET(tp, (const kafka_common_TopicPartition_t*)kafka_List_get_err(list, i, &ffi_err));
        if (err != NULL) break;
        napi_value pair = topic_partition_to_js(env, tp);
        if (pair == NULL || napi_set_element(env, arr, (uint32_t)i, pair) != napi_ok) {
            js_throw_last_napi_error(env, "napi_set_element");
            arr = NULL;
        }
    }
    // The list owns its elements: destroying it frees them.
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return arr;
}

int js_int64(napi_env env, napi_value value, const char* what, int64_t* out) {
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, value, &type), -1);
    if (type == napi_bigint) {
        bool lossless = false;
        NAPI_CHECK(env, napi_get_value_bigint_int64(env, value, out, &lossless), -1);
        if (!lossless) {
            napi_throw_range_error(env, NULL, what);
            return -1;
        }
        return 0;
    }
    if (type != napi_number) {
        napi_throw_type_error(env, NULL, what);
        return -1;
    }
    NAPI_CHECK(env, napi_get_value_int64(env, value, out), -1);
    return 0;
}

kafka_consumer_OffsetAndMetadata_t* offset_and_metadata_from_js(napi_env env, napi_value fields) {
    napi_value o, e, m;
    if (napi_get_element(env, fields, 0, &o) != napi_ok || napi_get_element(env, fields, 1, &e) != napi_ok
        || napi_get_element(env, fields, 2, &m) != napi_ok) {
        js_throw_last_napi_error(env, "napi_get_element");
        return NULL;
    }
    int64_t offset = 0;
    int32_t epoch = -1;
    if (js_int64(env, o, "an offset must be a bigint or a number", &offset) != 0) return NULL;
    if (napi_get_value_int32(env, e, &epoch) != napi_ok) {
        napi_throw_type_error(env, NULL, "a leader epoch must be a number");
        return NULL;
    }
    char* metadata = js_string_dup(env, m, "metadata must be a string");
    if (metadata == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_consumer_OffsetAndMetadata_t* om = NULL;
    // A negative epoch is Java's Optional.empty().
    kafka_common_Error_t* err =
        FFI_SET(om, kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(offset, epoch, metadata, &ffi_err));
    free(metadata);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return om;
}

void offset_and_metadata_free(napi_env env, kafka_consumer_OffsetAndMetadata_t* om) {
    kafka_common_Error_t* ffi_err = NULL;
    if (om != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_OffsetAndMetadata_destroy_err(om, &ffi_err)));
}

// The value an entry of a C-built map points at: an OffsetAndMetadata, or an
// int64_t the builder allocated.
typedef enum { MAP_OFFSETS, MAP_LONGS } MapValues;

// Destroys a C-built map of TopicPartition keys, which borrows its keys and
// values: each key and value is freed, then the map.
static void tp_map_free(napi_env env, kafka_Map_t* map, MapValues values) {
    if (map == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    error_warn(env, 1, FFI_SET(n, kafka_Map_size_err(map, &ffi_err)));
    for (int32_t i = 0; i < n; i++) {
        void* key = NULL;
        void* value = NULL;
        error_warn(env, 1, FFI_SET(key, kafka_Map_key_err(map, i, &ffi_err)));
        error_warn(env, 1, FFI_SET(value, kafka_Map_value_err(map, i, &ffi_err)));
        if (key != NULL) error_warn(env, 1, FFI_ERR(kafka_common_TopicPartition_destroy_err((kafka_common_TopicPartition_t*)key, &ffi_err)));
        if (values == MAP_OFFSETS) offset_and_metadata_free(env, (kafka_consumer_OffsetAndMetadata_t*)value);
        else free(value);
    }
    error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
}

// A map of TopicPartition keys from JS [pair, value] entries, a null pair a
// NULL key, each value read by `values`. NULL with a JS exception pending.
static kafka_Map_t* tp_map_from_js(napi_env env, napi_value entries, MapValues values) {
    uint32_t n = 0;
    if (napi_get_array_length(env, entries, &n) != napi_ok) {
        napi_throw_type_error(env, NULL, "a map's entries must be an array");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    for (uint32_t i = 0; i < n; i++) {
        napi_value entry, pair, v;
        napi_valuetype pair_type = napi_undefined, value_type = napi_undefined;
        if (napi_get_element(env, entries, i, &entry) != napi_ok || napi_get_element(env, entry, 0, &pair) != napi_ok
            || napi_get_element(env, entry, 1, &v) != napi_ok || napi_typeof(env, pair, &pair_type) != napi_ok
            || napi_typeof(env, v, &value_type) != napi_ok) {
            js_throw_last_napi_error(env, "napi_get_element");
            tp_map_free(env, map, values);
            return NULL;
        }
        // A null key or a null OffsetAndMetadata crosses as NULL, which the FFI reports.
        kafka_common_TopicPartition_t* tp = NULL;
        if (pair_type != napi_null && (tp = tp_from_js(env, pair)) == NULL) {
            tp_map_free(env, map, values);
            return NULL;
        }
        void* value = NULL;
        if (values == MAP_OFFSETS) {
            if (value_type != napi_null && (value = offset_and_metadata_from_js(env, v)) == NULL) {
                if (tp != NULL) FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
                tp_map_free(env, map, values);
                return NULL;
            }
        } else {
            int64_t* slot = (int64_t*)malloc(sizeof(int64_t));
            if (slot == NULL || js_int64(env, v, "a value must be a bigint or a number", slot) != 0) {
                if (slot == NULL) napi_throw_error(env, NULL, "out of memory");
                free(slot);
                if (tp != NULL) FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
                tp_map_free(env, map, values);
                return NULL;
            }
            value = slot;
        }
        err = FFI_ERR(kafka_Map_put_err(map, tp, value, &ffi_err));
        if (err != NULL) {
            if (tp != NULL) FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            if (values == MAP_OFFSETS) offset_and_metadata_free(env, (kafka_consumer_OffsetAndMetadata_t*)value);
            else free(value);
            tp_map_free(env, map, values);
            error_throw_owned(env, err);
            return NULL;
        }
    }
    return map;
}

kafka_Map_t* offset_map_from_js(napi_env env, napi_value entries) {
    return tp_map_from_js(env, entries, MAP_OFFSETS);
}

void offset_map_free(napi_env env, kafka_Map_t* map) {
    tp_map_free(env, map, MAP_OFFSETS);
}
