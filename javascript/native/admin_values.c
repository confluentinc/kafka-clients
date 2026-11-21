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

// The admin client's RPCs and their values (javascript/CLAUDE.md): for each
// RPC, the descriptor of its options (the setters JS may have run) and of its
// result (its accessors and each one's value converter), and its entry point,
// which builds the C inputs from plain JS values, calls
// `kafka_admin_Admin_<rpc>_with_options` and returns the result's box
// (admin.c). The converters read a BORROWED value through its accessors into
// plain JS values (nested values nested, a `long` as a bigint); TypeScript
// builds the classes from them. Python's admin marshaling is the reference.

#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "admin.h"

// ---- plain values -------------------------------------------------------------

static napi_value js_null(napi_env env) {
    napi_value v = NULL;
    napi_get_null(env, &v);
    return v;
}

static napi_value js_string_or_null(napi_env env, const char* s) {
    if (s == NULL) return js_null(env);
    napi_value v = NULL;
    if (napi_create_string_utf8(env, s, NAPI_AUTO_LENGTH, &v) != napi_ok) js_throw_last_napi_error(env, "a string");
    return v;
}

// A borrowed Uuid as [mostSigBits, leastSigBits] bigints, `null` for NULL.
napi_value admin_uuid_to_js(napi_env env, const kafka_common_Uuid_t* id) {
    if (id == NULL) return js_null(env);
    kafka_common_Error_t* ffi_err = NULL;
    int64_t most = 0, least = 0;
    kafka_common_Error_t* err = FFI_SET(most, kafka_common_Uuid_most_significant_bits_err(id, &ffi_err));
    if (err == NULL) err = FFI_SET(least, kafka_common_Uuid_least_significant_bits_err(id, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value pair, m, l;
    NAPI_CHECK(env, napi_create_array_with_length(env, 2, &pair), NULL);
    NAPI_CHECK(env, napi_create_bigint_int64(env, most, &m), NULL);
    NAPI_CHECK(env, napi_create_bigint_int64(env, least, &l), NULL);
    NAPI_CHECK(env, napi_set_element(env, pair, 0, m), NULL);
    NAPI_CHECK(env, napi_set_element(env, pair, 1, l), NULL);
    return pair;
}

// A borrowed list as an array of item(env, item); NULL as `null` when
// `null_for_null`, else as an empty array.
napi_value admin_list_to_js(napi_env env, const kafka_List_t* list, napi_value (*item)(napi_env, const void*), int null_for_null) {
    if (list == NULL && null_for_null) return js_null(env);
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = list == NULL ? NULL : FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    napi_value out;
    NAPI_CHECK(env, napi_create_array_with_length(env, (size_t)(n < 0 ? 0 : n), &out), NULL);
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* v = NULL;
        err = FFI_SET(v, kafka_List_get_err(list, i, &ffi_err));
        if (err != NULL) break;
        napi_value js = item(env, v);
        if (js == NULL) return NULL;
        NAPI_CHECK(env, napi_set_element(env, out, (uint32_t)i, js), NULL);
    }
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return out;
}

// A borrowed map as an array of [key(k), value(v)] pairs, in the map's order;
// NULL as an empty array.
napi_value admin_map_to_js(napi_env env, const kafka_Map_t* map, napi_value (*key)(napi_env, const void*),
                           napi_value (*value)(napi_env, const void*)) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = map == NULL ? NULL : FFI_SET(n, kafka_Map_size_err(map, &ffi_err));
    napi_value out;
    NAPI_CHECK(env, napi_create_array_with_length(env, (size_t)(n < 0 ? 0 : n), &out), NULL);
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* k = NULL;
        void* v = NULL;
        err = FFI_SET(k, kafka_Map_key_err(map, i, &ffi_err));
        if (err == NULL) err = FFI_SET(v, kafka_Map_value_err(map, i, &ffi_err));
        if (err != NULL) break;
        napi_value jk = key(env, k);
        if (jk == NULL) return NULL;
        napi_value jv = value(env, v);
        if (jv == NULL) return NULL;
        napi_value pair;
        NAPI_CHECK(env, napi_create_array_with_length(env, 2, &pair), NULL);
        NAPI_CHECK(env, napi_set_element(env, pair, 0, jk), NULL);
        NAPI_CHECK(env, napi_set_element(env, pair, 1, jv), NULL);
        NAPI_CHECK(env, napi_set_element(env, out, (uint32_t)i, pair), NULL);
    }
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return out;
}

napi_value admin_item_string(napi_env env, const void* item) {
    return js_string_or_null(env, (const char*)item);
}

static napi_value item_node(napi_env env, const void* item) {
    return node_to_js(env, (const kafka_common_Node_t*)item);
}

// A borrowed AclOperation singleton as its code.
static napi_value item_acl_operation(napi_env env, const void* item) {
    kafka_common_Error_t* ffi_err = NULL;
    int8_t code = 0;
    kafka_common_Error_t* err = FFI_SET(code, kafka_common_acl_AclOperation_code_err((const kafka_common_acl_AclOperation_t*)item, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value v;
    NAPI_CHECK(env, napi_create_int32(env, code, &v), NULL);
    return v;
}

// A borrowed list of AclOperation singletons as their codes, `null` for NULL
// (Java's null: the operations were not requested).
napi_value admin_acl_operations_to_js(napi_env env, const kafka_List_t* list) {
    return admin_list_to_js(env, list, item_acl_operation, 1);
}

// ---- converters -----------------------------------------------------------------

static napi_value conv_string(napi_env env, void* v) {
    return js_string_or_null(env, (const char*)v);
}

static napi_value conv_nodes(napi_env env, void* v) {
    return admin_list_to_js(env, (const kafka_List_t*)v, item_node, 0);
}

static napi_value conv_node(napi_env env, void* v) {
    return node_to_js(env, (const kafka_common_Node_t*)v);
}

static napi_value conv_acl_operations(napi_env env, void* v) {
    return admin_acl_operations_to_js(env, (const kafka_List_t*)v);
}

// A TopicListing as { name, topicId, internal }.
static napi_value item_topic_listing(napi_env env, const void* item) {
    const kafka_admin_TopicListing_t* t = (const kafka_admin_TopicListing_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const char* name = NULL;
    kafka_common_Uuid_t* id = NULL;
    int8_t internal = 0;
    kafka_common_Error_t* err = FFI_SET(name, kafka_admin_TopicListing_name_err(t, &ffi_err));
    if (err == NULL) err = FFI_SET(id, kafka_admin_TopicListing_topic_id_err(t, &ffi_err));
    if (err == NULL) err = FFI_SET(internal, kafka_admin_TopicListing_is_internal_err(t, &ffi_err));
    if (err != NULL) {
        if (id != NULL) FFI_DISCARD(kafka_common_Uuid_destroy_err(id, &ffi_err));
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj, v;
    napi_value topic_id = admin_uuid_to_js(env, id);
    if (id != NULL) error_warn(env, 1, FFI_ERR(kafka_common_Uuid_destroy_err(id, &ffi_err)));
    if (topic_id == NULL) return NULL;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if ((v = js_string_or_null(env, name)) == NULL) return NULL;
    NAPI_CHECK(env, napi_set_named_property(env, obj, "name", v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "topicId", topic_id), NULL);
    NAPI_CHECK(env, napi_get_boolean(env, internal != 0, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "internal", v), NULL);
    return obj;
}

// [[name, { name, topicId, internal }]], sorted by name.
static napi_value conv_topic_listings(napi_env env, void* v) {
    return admin_map_to_js(env, (const kafka_Map_t*)v, admin_item_string, item_topic_listing);
}

// ---- describeCluster ------------------------------------------------------------

#define ADMIN_RPC DescribeCluster

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeClusterOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(includeAuthorizedOperations, kafka_admin_DescribeClusterOptions_set_include_authorized_operations_err)
ADMIN_OPT_BOOL(includeFencedBrokers, kafka_admin_DescribeClusterOptions_set_include_fenced_brokers_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeClusterOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeClusterOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(includeAuthorizedOperations),
     ADMIN_SETTER(includeFencedBrokers), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeClusterResult_destroy_err),
    {ADMIN_ACC_ONE(nodes, kafka_admin_DescribeClusterResult_nodes_err, conv_nodes),
     ADMIN_ACC_ONE(controller, kafka_admin_DescribeClusterResult_controller_err, conv_node),
     ADMIN_ACC_ONE(cluster_id, kafka_admin_DescribeClusterResult_cluster_id_err, conv_string),
     ADMIN_ACC_ONE(authorized_operations, kafka_admin_DescribeClusterResult_authorized_operations_err, conv_acl_operations),
     ADMIN_ACC_END},
};

// adminDescribeCluster(handle, options): the result's box; its futures are
// `nodes`, `controller`, `cluster_id` and `authorized_operations`.
napi_value js_admin_describe_cluster(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 2 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[1]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_describe_cluster_with_options_err(a->view, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeClusterOptions_desc DescribeClusterResult_desc

// ---- listTopics -------------------------------------------------------------------

#define ADMIN_RPC ListTopics

ADMIN_OPT_TIMEOUT(kafka_admin_ListTopicsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(listInternal, kafka_admin_ListTopicsOptions_set_list_internal_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ListTopicsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListTopicsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(listInternal), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ListTopicsResult_destroy_err),
    {ADMIN_ACC_ONE(names_to_listings, kafka_admin_ListTopicsResult_names_to_listings_err, conv_topic_listings), ADMIN_ACC_END},
};

// adminListTopics(handle, options): the result's box; its one future is
// `names_to_listings`.
napi_value js_admin_list_topics(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 2 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[1]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_list_topics_with_options_err(a->view, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ListTopicsOptions_desc ListTopicsResult_desc

// ---- inputs ---------------------------------------------------------------------

// A JS string or null as a malloc'ed C string, NULL for null (*ok 1), or NULL
// with a TypeError pending (*ok 0).
static char* string_or_null(napi_env env, napi_value value, const char* what, int* ok) {
    napi_valuetype type;
    *ok = 1;
    if (napi_typeof(env, value, &type) != napi_ok) {
        *ok = 0;
        return NULL;
    }
    if (type == napi_null) return NULL;
    char* s = js_string_dup(env, value, what);
    if (s == NULL) *ok = 0;
    return s;
}

static napi_value element(napi_env env, napi_value array, uint32_t i) {
    napi_value v = NULL;
    if (napi_get_element(env, array, i, &v) != napi_ok) js_throw_last_napi_error(env, "napi_get_element");
    return v;
}

static napi_value property(napi_env env, napi_value obj, const char* name) {
    napi_value v = NULL;
    if (napi_get_named_property(env, obj, name, &v) != napi_ok) js_throw_last_napi_error(env, name);
    return v;
}

static uint32_t length_of(napi_env env, napi_value array, int* ok) {
    uint32_t n = 0;
    *ok = napi_get_array_length(env, array, &n) == napi_ok;
    if (!*ok) napi_throw_type_error(env, NULL, "an array");
    return n;
}

static void free_item(void* item, kafka_common_Error_t** out_err) {
    (void)out_err;
    free(item);
}

static void node_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_Node_destroy_err((kafka_common_Node_t*)item, out_err);
}

static void int_list_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_List_t* list = (kafka_List_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* v = NULL;
        err = FFI_SET(v, kafka_List_get_err(list, i, &ffi_err));
        if (err == NULL) free(v);
    }
    error_discard(err);
    kafka_List_destroy_err(list, out_err);
}

static void uuid_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_Uuid_destroy_err((kafka_common_Uuid_t*)item, out_err);
}

static void tpi_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_TopicPartitionInfo_destroy_err((kafka_common_TopicPartitionInfo_t*)item, out_err);
}

static void new_topic_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_NewTopic_destroy_err((kafka_admin_NewTopic_t*)item, out_err);
}

// A new list, or NULL with the core's error thrown.
static kafka_List_t* list_new(napi_env env) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_List_new_err(&ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return list;
}

// Adds an owned `item` to `list`, destroying it through `destroy` when the
// add fails. 0, or -1 with the core's error thrown.
static int list_add(napi_env env, kafka_List_t* list, void* item, void (*destroy)(void*, kafka_common_Error_t**)) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_List_add_err(list, item, &ffi_err));
    if (err == NULL) return 0;
    if (destroy != NULL) FFI_DISCARD(destroy(item, &ffi_err));
    error_throw_owned(env, err);
    return -1;
}

// An array of numbers as an owned list of malloc'ed int32_t, or NULL with a
// JS exception pending.
static kafka_List_t* int_list(napi_env env, napi_value array) {
    int ok = 0;
    uint32_t n = length_of(env, array, &ok);
    if (!ok) return NULL;
    kafka_List_t* list = list_new(env);
    if (list == NULL) return NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value v = element(env, array, i);
        int32_t* box = (int32_t*)malloc(sizeof(int32_t));
        if (v == NULL || box == NULL || napi_get_value_int32(env, v, box) != napi_ok) {
            free(box);
            admin_list_free(env, list, free_item);
            if (v != NULL) napi_throw_type_error(env, NULL, "a broker id must be a number");
            return NULL;
        }
        if (list_add(env, list, box, free_item) < 0) {
            admin_list_free(env, list, free_item);
            return NULL;
        }
    }
    return list;
}

// An array of strings as an owned list of malloc'ed C strings (a null element
// NULL), or NULL with a JS exception pending.
static kafka_List_t* string_list(napi_env env, napi_value array) {
    int ok = 0;
    uint32_t n = length_of(env, array, &ok);
    if (!ok) return NULL;
    kafka_List_t* list = list_new(env);
    if (list == NULL) return NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value v = element(env, array, i);
        char* s = v == NULL ? NULL : string_or_null(env, v, "a topic name must be a string", &ok);
        if (v == NULL || !ok || list_add(env, list, s, free_item) < 0) {
            admin_list_free(env, list, free_item);
            return NULL;
        }
    }
    return list;
}

// An array of [mostSigBits, leastSigBits] bigints as an owned list of owned
// Uuids, or NULL with a JS exception pending.
static kafka_List_t* uuid_list(napi_env env, napi_value array) {
    int ok = 0;
    uint32_t n = length_of(env, array, &ok);
    if (!ok) return NULL;
    kafka_List_t* list = list_new(env);
    if (list == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value pair = element(env, array, i);
        napi_value m = pair == NULL ? NULL : element(env, pair, 0);
        napi_value l = pair == NULL ? NULL : element(env, pair, 1);
        int64_t most = 0, least = 0;
        bool lossless = false;
        if (m == NULL || l == NULL || napi_get_value_bigint_int64(env, m, &most, &lossless) != napi_ok
            || napi_get_value_bigint_int64(env, l, &least, &lossless) != napi_ok) {
            admin_list_free(env, list, uuid_item_destroy);
            napi_throw_type_error(env, NULL, "a topic id's halves must be bigints");
            return NULL;
        }
        kafka_common_Uuid_t* id = NULL;
        kafka_common_Error_t* err = FFI_SET(id, kafka_common_Uuid_new_err(most, least, &ffi_err));
        if (err != NULL) {
            admin_list_free(env, list, uuid_item_destroy);
            error_throw_owned(env, err);
            return NULL;
        }
        if (list_add(env, list, id, uuid_item_destroy) < 0) {
            admin_list_free(env, list, uuid_item_destroy);
            return NULL;
        }
    }
    return list;
}

// `form` 1 (names) or 2 (ids) and their items as an owned TopicCollection, or
// NULL with a JS exception pending.
static kafka_common_TopicCollection_t* topic_collection(napi_env env, int32_t form, napi_value items) {
    kafka_List_t* list = form == 2 ? uuid_list(env, items) : string_list(env, items);
    if (list == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_TopicCollection_t* topics = NULL;
    kafka_common_Error_t* err = form == 2 ? FFI_SET(topics, kafka_common_TopicCollection_of_topic_ids_err(list, &ffi_err))
                                          : FFI_SET(topics, kafka_common_TopicCollection_of_topic_names_err(list, &ffi_err));
    admin_list_free(env, list, form == 2 ? uuid_item_destroy : free_item);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return topics;
}

// An array of [key, value] strings as an owned map of malloc'ed C strings, or
// NULL with a JS exception pending; `null` is NULL with none (*is_null).
static kafka_Map_t* string_map(napi_env env, napi_value pairs, int* is_null) {
    napi_valuetype type;
    *is_null = 0;
    if (napi_typeof(env, pairs, &type) != napi_ok) return NULL;
    if (type == napi_null) {
        *is_null = 1;
        return NULL;
    }
    int ok = 0;
    uint32_t n = length_of(env, pairs, &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && i < n; i++) {
        napi_value pair = element(env, pairs, i);
        napi_value k = pair == NULL ? NULL : element(env, pair, 0);
        napi_value v = pair == NULL ? NULL : element(env, pair, 1);
        char* key = k == NULL ? NULL : string_or_null(env, k, "a config name must be a string", &ok);
        if (k == NULL || !ok) {
            admin_map_free(env, map);
            return NULL;
        }
        char* value = v == NULL ? NULL : string_or_null(env, v, "a config value must be a string or null", &ok);
        if (v == NULL || !ok) {
            free(key);
            admin_map_free(env, map);
            return NULL;
        }
        err = FFI_ERR(kafka_Map_put_err(map, key, value, &ffi_err));
        if (err != NULL) {
            free(key);
            free(value);
        }
    }
    if (err != NULL) {
        admin_map_free(env, map);
        error_throw_owned(env, err);
        return NULL;
    }
    return map;
}

// A list of node fields as an owned list of owned nodes; NULL with a JS
// exception pending, or NULL for `null` (*is_null).
static kafka_List_t* node_list(napi_env env, napi_value array, int* is_null) {
    return admin_nodes_from_js(env, array, is_null);
}

// A JS TopicPartitionInfo's fields ({ partition, leader, replicas, isr, elr,
// lastKnownElr }) as an owned TopicPartitionInfo, or NULL with a JS exception
// pending.
static kafka_common_TopicPartitionInfo_t* topic_partition_info_from_js(napi_env env, napi_value obj) {
    napi_value v = property(env, obj, "partition");
    int32_t partition = 0;
    if (v == NULL || napi_get_value_int32(env, v, &partition) != napi_ok) {
        napi_throw_type_error(env, NULL, "a partition's fields");
        return NULL;
    }
    napi_valuetype type;
    kafka_common_Node_t* leader = NULL;
    v = property(env, obj, "leader");
    if (v == NULL || napi_typeof(env, v, &type) != napi_ok) return NULL;
    if (type != napi_null && (leader = admin_node_from_js(env, v)) == NULL) return NULL;
    kafka_List_t* lists[4] = {NULL, NULL, NULL, NULL};
    const char* names[4] = {"replicas", "isr", "elr", "lastKnownElr"};
    int nulls[4] = {0, 0, 0, 0};
    kafka_common_Error_t* ffi_err = NULL;
    int failed = 0;
    for (int i = 0; i < 4 && !failed; i++) {
        v = property(env, obj, names[i]);
        if (v == NULL) failed = 1;
        else if ((lists[i] = node_list(env, v, &nulls[i])) == NULL && !nulls[i]) failed = 1;
    }
    kafka_common_TopicPartitionInfo_t* info = NULL;
    kafka_common_Error_t* err = NULL;
    if (!failed) {
        err = nulls[2] || nulls[3]
            ? FFI_SET(info, kafka_common_TopicPartitionInfo_new_err(partition, leader, lists[0], lists[1], &ffi_err))
            : FFI_SET(info, kafka_common_TopicPartitionInfo_with_elr_last_known_elr_err(partition, leader, lists[0], lists[1],
                                                                                      lists[2], lists[3], &ffi_err));
    }
    for (int i = 0; i < 4; i++) admin_list_free(env, lists[i], node_item_destroy);
    if (leader != NULL) error_warn(env, 1, FFI_ERR(kafka_common_Node_destroy_err(leader, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return failed ? NULL : info;
}

// ---- outputs --------------------------------------------------------------------

static napi_value js_int32(napi_env env, int32_t n) {
    napi_value v = NULL;
    if (napi_create_int32(env, n, &v) != napi_ok) js_throw_last_napi_error(env, "napi_create_int32");
    return v;
}

static napi_value js_bool(napi_env env, int b) {
    napi_value v = NULL;
    if (napi_get_boolean(env, b != 0, &v) != napi_ok) js_throw_last_napi_error(env, "napi_get_boolean");
    return v;
}

static int set(napi_env env, napi_value obj, const char* name, napi_value v) {
    if (v == NULL) return -1;
    if (napi_set_named_property(env, obj, name, v) != napi_ok) {
        js_throw_last_napi_error(env, name);
        return -1;
    }
    return 0;
}

// An OWNED list as admin_list_to_js reads it, the list then destroyed (with
// its elements, as the accessors' lists are freed).
static napi_value owned_list_to_js(napi_env env, kafka_List_t* list, napi_value (*item)(napi_env, const void*), int null_for_null) {
    napi_value out = admin_list_to_js(env, list, item, null_for_null);
    kafka_common_Error_t* ffi_err = NULL;
    if (list != NULL) error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    return out;
}

// A TopicPartitionInfo as { partition, leader, replicas, isr, elr, lastKnownElr },
// elr and lastKnownElr null when the broker did not report them.
static napi_value item_topic_partition_info(napi_env env, const void* item) {
    const kafka_common_TopicPartitionInfo_t* info = (const kafka_common_TopicPartitionInfo_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t partition = 0;
    const kafka_common_Node_t* leader = NULL;
    kafka_List_t *replicas = NULL, *isr = NULL, *elr = NULL, *last_known_elr = NULL;
    kafka_common_Error_t* err = FFI_SET(partition, kafka_common_TopicPartitionInfo_partition_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(leader, kafka_common_TopicPartitionInfo_leader_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(replicas, kafka_common_TopicPartitionInfo_replicas_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(isr, kafka_common_TopicPartitionInfo_isr_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(elr, kafka_common_TopicPartitionInfo_elr_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(last_known_elr, kafka_common_TopicPartitionInfo_last_known_elr_err(info, &ffi_err));
    napi_value obj = NULL;
    if (err == NULL && napi_create_object(env, &obj) != napi_ok) obj = NULL;
    int bad = err != NULL || obj == NULL;
    if (!bad) bad = set(env, obj, "partition", js_int32(env, partition)) < 0 || set(env, obj, "leader", node_to_js(env, leader)) < 0;
    // Each list converted, then destroyed, whatever happened before.
    napi_value lists[4] = {NULL, NULL, NULL, NULL};
    lists[0] = owned_list_to_js(env, replicas, item_node, 0);
    lists[1] = owned_list_to_js(env, isr, item_node, 0);
    lists[2] = owned_list_to_js(env, elr, item_node, 1);
    lists[3] = owned_list_to_js(env, last_known_elr, item_node, 1);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (bad || set(env, obj, "replicas", lists[0]) < 0 || set(env, obj, "isr", lists[1]) < 0 || set(env, obj, "elr", lists[2]) < 0
        || set(env, obj, "lastKnownElr", lists[3]) < 0)
        return NULL;
    return obj;
}

// A TopicDescription as { name, internal, topicId, partitions, authorizedOperations }.
static napi_value topic_description_to_js(napi_env env, const kafka_admin_TopicDescription_t* d) {
    kafka_common_Error_t* ffi_err = NULL;
    const char* name = NULL;
    int8_t internal = 0;
    kafka_common_Uuid_t* id = NULL;
    kafka_List_t *partitions = NULL, *operations = NULL;
    kafka_common_Error_t* err = FFI_SET(name, kafka_admin_TopicDescription_name_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(internal, kafka_admin_TopicDescription_is_internal_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(id, kafka_admin_TopicDescription_topic_id_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(partitions, kafka_admin_TopicDescription_partitions_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(operations, kafka_admin_TopicDescription_authorized_operations_err(d, &ffi_err));
    napi_value topic_id = err == NULL ? admin_uuid_to_js(env, id) : NULL;
    napi_value parts = owned_list_to_js(env, partitions, item_topic_partition_info, 0);
    napi_value ops = admin_acl_operations_to_js(env, operations);
    if (operations != NULL) error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(operations, &ffi_err)));
    if (id != NULL) error_warn(env, 1, FFI_ERR(kafka_common_Uuid_destroy_err(id, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "name", js_string_or_null(env, name)) < 0 || set(env, obj, "internal", js_bool(env, internal)) < 0
        || set(env, obj, "topicId", topic_id) < 0 || set(env, obj, "partitions", parts) < 0
        || set(env, obj, "authorizedOperations", ops) < 0)
        return NULL;
    return obj;
}

// A ConfigSynonym as { name, value, source }, the source its ordinal (-1 for
// Java's null).
static napi_value item_config_synonym(napi_env env, const void* item) {
    const kafka_admin_ConfigEntry_ConfigSynonym_t* s = (const kafka_admin_ConfigEntry_ConfigSynonym_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const char *name = NULL, *value = NULL;
    const kafka_admin_ConfigEntry_ConfigSource_t* source = NULL;
    int source_ordinal = 0;
    kafka_common_Error_t* err = FFI_SET(name, kafka_admin_ConfigEntry_ConfigSynonym_name_err(s, &ffi_err));
    if (err == NULL) err = FFI_SET(value, kafka_admin_ConfigEntry_ConfigSynonym_value_err(s, &ffi_err));
    if (err == NULL) err = FFI_SET(source, kafka_admin_ConfigEntry_ConfigSynonym_source_err(s, &ffi_err));
    if (err == NULL) source_ordinal = -1;
    if (err == NULL && source != NULL)
        err = FFI_SET(source_ordinal, (int)kafka_admin_ConfigEntry_ConfigSource__enum_err(source, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "name", js_string_or_null(env, name)) < 0 || set(env, obj, "value", js_string_or_null(env, value)) < 0
        || set(env, obj, "source", js_int32(env, source_ordinal)) < 0)
        return NULL;
    return obj;
}

// A ConfigEntry as { name, value, source, isSensitive, isReadOnly, synonyms,
// type, documentation }, the source and the type their ordinals (-1 for
// Java's null).
static napi_value item_config_entry(napi_env env, const void* item) {
    const kafka_admin_ConfigEntry_t* e = (const kafka_admin_ConfigEntry_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const char *name = NULL, *value = NULL, *documentation = NULL;
    const kafka_admin_ConfigEntry_ConfigSource_t* source = NULL;
    const kafka_admin_ConfigEntry_ConfigType_t* type = NULL;
    int source_ordinal = 0, type_ordinal = 0;
    int8_t sensitive = 0, read_only = 0;
    kafka_List_t* synonyms = NULL;
    kafka_common_Error_t* err = FFI_SET(name, kafka_admin_ConfigEntry_name_err(e, &ffi_err));
    if (err == NULL) err = FFI_SET(value, kafka_admin_ConfigEntry_value_err(e, &ffi_err));
    if (err == NULL) err = FFI_SET(source, kafka_admin_ConfigEntry_source_err(e, &ffi_err));
    // Java's null source: -1, null in JS.
    if (err == NULL) source_ordinal = -1;
    if (err == NULL && source != NULL)
        err = FFI_SET(source_ordinal, (int)kafka_admin_ConfigEntry_ConfigSource__enum_err(source, &ffi_err));
    if (err == NULL) err = FFI_SET(sensitive, kafka_admin_ConfigEntry_is_sensitive_err(e, &ffi_err));
    if (err == NULL) err = FFI_SET(read_only, kafka_admin_ConfigEntry_is_read_only_err(e, &ffi_err));
    if (err == NULL) err = FFI_SET(type, kafka_admin_ConfigEntry_type_err(e, &ffi_err));
    // Java's null type (a createTopics config has none): -1, null in JS.
    if (err == NULL) type_ordinal = -1;
    if (err == NULL && type != NULL) err = FFI_SET(type_ordinal, (int)kafka_admin_ConfigEntry_ConfigType__enum_err(type, &ffi_err));
    if (err == NULL) err = FFI_SET(documentation, kafka_admin_ConfigEntry_documentation_err(e, &ffi_err));
    if (err == NULL) err = FFI_SET(synonyms, kafka_admin_ConfigEntry_synonyms_err(e, &ffi_err));
    napi_value syn = owned_list_to_js(env, synonyms, item_config_synonym, 0);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "name", js_string_or_null(env, name)) < 0 || set(env, obj, "value", js_string_or_null(env, value)) < 0
        || set(env, obj, "source", js_int32(env, source_ordinal)) < 0 || set(env, obj, "isSensitive", js_bool(env, sensitive)) < 0
        || set(env, obj, "isReadOnly", js_bool(env, read_only)) < 0 || set(env, obj, "synonyms", syn) < 0
        || set(env, obj, "type", js_int32(env, type_ordinal)) < 0
        || set(env, obj, "documentation", js_string_or_null(env, documentation)) < 0)
        return NULL;
    return obj;
}

// A Config as the array of its entries' fields.
static napi_value conv_config(napi_env env, void* v) {
    if (v == NULL) return js_null(env);
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* entries = NULL;
    kafka_common_Error_t* err = FFI_SET(entries, kafka_admin_Config_entries_err((const kafka_admin_Config_t*)v, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return owned_list_to_js(env, entries, item_config_entry, 0);
}

static napi_value conv_uuid(napi_env env, void* v) {
    return admin_uuid_to_js(env, (const kafka_common_Uuid_t*)v);
}

// An `int32_t *` (or `int16_t *` widened by the core) as a number.
static napi_value conv_int32(napi_env env, void* v) {
    return v == NULL ? js_null(env) : js_int32(env, *(const int32_t*)v);
}

static napi_value conv_topic_description(napi_env env, void* v) {
    return v == NULL ? js_null(env) : topic_description_to_js(env, (const kafka_admin_TopicDescription_t*)v);
}

static napi_value conv_deleted_records(napi_env env, void* v) {
    if (v == NULL) return js_null(env);
    kafka_common_Error_t* ffi_err = NULL;
    int64_t low = 0;
    kafka_common_Error_t* err = FFI_SET(low, kafka_admin_DeletedRecords_low_watermark_err((const kafka_admin_DeletedRecords_t*)v, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value out;
    NAPI_CHECK(env, napi_create_bigint_int64(env, low, &out), NULL);
    return out;
}

static napi_value key_string(napi_env env, const void* k) {
    return js_string_or_null(env, (const char*)k);
}

static napi_value key_uuid(napi_env env, const void* k) {
    return admin_uuid_to_js(env, (const kafka_common_Uuid_t*)k);
}

// A borrowed TopicPartition as [topic, partition].
static napi_value key_topic_partition(napi_env env, const void* k) {
    const kafka_common_TopicPartition_t* tp = (const kafka_common_TopicPartition_t*)k;
    kafka_common_Error_t* ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = 0;
    kafka_common_Error_t* err = FFI_SET(topic, kafka_common_TopicPartition_topic_err(tp, &ffi_err));
    if (err == NULL) err = FFI_SET(partition, kafka_common_TopicPartition_partition_err(tp, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value pair;
    NAPI_CHECK(env, napi_create_array_with_length(env, 2, &pair), NULL);
    napi_value t = js_string_or_null(env, topic), p = js_int32(env, partition);
    if (t == NULL || p == NULL) return NULL;
    NAPI_CHECK(env, napi_set_element(env, pair, 0, t), NULL);
    NAPI_CHECK(env, napi_set_element(env, pair, 1, p), NULL);
    return pair;
}

// ---- createTopics -------------------------------------------------------------------

#define ADMIN_RPC CreateTopics

ADMIN_OPT_TIMEOUT(kafka_admin_CreateTopicsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(validateOnly, kafka_admin_CreateTopicsOptions_set_validate_only_err)
ADMIN_OPT_BOOL(retryOnQuotaViolation, kafka_admin_CreateTopicsOptions_set_retry_on_quota_violation_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_CreateTopicsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_CreateTopicsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(validateOnly), ADMIN_SETTER(retryOnQuotaViolation),
     {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_CreateTopicsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_CreateTopicsResult_values_err, key_string, NULL),
     ADMIN_ACC_STR(config, kafka_admin_CreateTopicsResult_config_err, conv_config),
     ADMIN_ACC_STR(topic_id, kafka_admin_CreateTopicsResult_topic_id_err, conv_uuid),
     ADMIN_ACC_STR(num_partitions, kafka_admin_CreateTopicsResult_num_partitions_err, conv_int32),
     ADMIN_ACC_STR(replication_factor, kafka_admin_CreateTopicsResult_replication_factor_err, conv_int32), ADMIN_ACC_END},
};

// A JS NewTopic's fields ({ name, numPartitions, replicationFactor,
// assignments, configs }: -1 for an empty optional, assignments an array of
// [partition, [broker]] or null, configs an array of [name, value] or null)
// as an owned NewTopic, or NULL with a JS exception pending.
static kafka_admin_NewTopic_t* new_topic_from_js(napi_env env, napi_value obj) {
    int ok = 0;
    napi_value v = property(env, obj, "name");
    char* name = v == NULL ? NULL : string_or_null(env, v, "a topic name must be a string", &ok);
    if (v == NULL || !ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = NULL;
    kafka_admin_NewTopic_t* topic = NULL;
    napi_value assignments = property(env, obj, "assignments");
    napi_valuetype type;
    if (assignments == NULL || napi_typeof(env, assignments, &type) != napi_ok) {
        free(name);
        return NULL;
    }
    if (type == napi_null) {
        int32_t partitions = -1, replication = -1;
        napi_value p = property(env, obj, "numPartitions"), r = property(env, obj, "replicationFactor");
        if (p == NULL || r == NULL || napi_get_value_int32(env, p, &partitions) != napi_ok
            || napi_get_value_int32(env, r, &replication) != napi_ok) {
            free(name);
            napi_throw_type_error(env, NULL, "a new topic's counts");
            return NULL;
        }
        err = FFI_SET(topic, kafka_admin_NewTopic_with_num_partitions_replication_factor_err(name, partitions, (int16_t)replication, &ffi_err));
    } else {
        uint32_t n = length_of(env, assignments, &ok);
        kafka_Map_t* map = NULL;
        if (ok) err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
        for (uint32_t i = 0; ok && err == NULL && i < n; i++) {
            napi_value pair = element(env, assignments, i);
            napi_value k = pair == NULL ? NULL : element(env, pair, 0);
            napi_value brokers = pair == NULL ? NULL : element(env, pair, 1);
            int32_t* key = (int32_t*)malloc(sizeof(int32_t));
            kafka_List_t* ids = NULL;
            if (k == NULL || brokers == NULL || key == NULL || napi_get_value_int32(env, k, key) != napi_ok
                || (ids = int_list(env, brokers)) == NULL) {
                free(key);
                ok = 0;
                break;
            }
            err = FFI_ERR(kafka_Map_put_err(map, key, ids, &ffi_err));
            if (err != NULL) {
                free(key);
                int_list_destroy(ids, &ffi_err);
                error_discard(ffi_take(&ffi_err));
            }
        }
        if (ok && err == NULL) err = FFI_SET(topic, kafka_admin_NewTopic_with_replicas_assignments_err(name, map, &ffi_err));
        // The map's keys and lists are copied by the call: freed here.
        if (map != NULL) {
            int32_t size = 0;
            kafka_common_Error_t* e2 = FFI_SET(size, kafka_Map_size_err(map, &ffi_err));
            for (int32_t i = 0; e2 == NULL && i < size; i++) {
                void* key = NULL;
                void* ids = NULL;
                e2 = FFI_SET(key, kafka_Map_key_err(map, i, &ffi_err));
                if (e2 == NULL) e2 = FFI_SET(ids, kafka_Map_value_err(map, i, &ffi_err));
                if (e2 == NULL) {
                    free(key);
                    int_list_destroy(ids, &ffi_err);
                    error_discard(ffi_take(&ffi_err));
                }
            }
            error_warn(env, 1, e2);
            error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
        }
    }
    free(name);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (topic == NULL) return NULL;
    napi_value configs = property(env, obj, "configs");
    int is_null = 0;
    kafka_Map_t* config_map = configs == NULL ? NULL : string_map(env, configs, &is_null);
    if (configs == NULL || (config_map == NULL && !is_null)) {
        FFI_DISCARD(kafka_admin_NewTopic_destroy_err(topic, &ffi_err));
        return NULL;
    }
    if (config_map != NULL) {
        err = FFI_ERR(kafka_admin_NewTopic_set_configs_err(topic, config_map, &ffi_err));
        admin_map_free(env, config_map);
        if (err != NULL) {
            FFI_DISCARD(kafka_admin_NewTopic_destroy_err(topic, &ffi_err));
            error_throw_owned(env, err);
            return NULL;
        }
    }
    return topic;
}

// adminCreateTopics(handle, newTopics, options): each new topic as its fields
// (new_topic_from_js); the result's futures are `values` (a map), and the
// per-topic `config`, `topic_id`, `num_partitions` and `replication_factor`.
napi_value js_admin_create_topics(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_List_t* topics = list_new(env);
    if (topics == NULL) return NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value t = element(env, argv[1], i);
        kafka_admin_NewTopic_t* topic = t == NULL ? NULL : new_topic_from_js(env, t);
        if (topic == NULL || list_add(env, topics, topic, new_topic_item_destroy) < 0) {
            admin_list_free(env, topics, new_topic_item_destroy);
            return NULL;
        }
    }
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, topics, new_topic_item_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_create_topics_with_options_err(a->view, topics, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, topics, new_topic_item_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison CreateTopicsOptions_desc CreateTopicsResult_desc

// ---- deleteTopics -------------------------------------------------------------------

#define ADMIN_RPC DeleteTopics

ADMIN_OPT_TIMEOUT(kafka_admin_DeleteTopicsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(retryOnQuotaViolation, kafka_admin_DeleteTopicsOptions_set_retry_on_quota_violation_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DeleteTopicsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DeleteTopicsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(retryOnQuotaViolation), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DeleteTopicsResult_destroy_err),
    {ADMIN_ACC_MAP(topic_name_values, kafka_admin_DeleteTopicsResult_topic_name_values_err, key_string, NULL),
     ADMIN_ACC_MAP(topic_id_values, kafka_admin_DeleteTopicsResult_topic_id_values_err, key_uuid, NULL), ADMIN_ACC_END},
};

// adminDeleteTopics(handle, form, items, options): Java's
// deleteTopics(Collection<String>) for form 0 (`_with_string_slice`), a
// TopicNameCollection for 1 and a TopicIdCollection for 2
// (`_with_topic_collection`), the ids as [mostSigBits, leastSigBits].
napi_value js_admin_delete_topics(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int32_t form = 0;
    NAPI_CHECK(env, napi_get_value_int32(env, argv[1], &form), NULL);
    kafka_List_t* names = NULL;
    kafka_common_TopicCollection_t* topics = NULL;
    if (form == 0) names = string_list(env, argv[2]);
    else topics = topic_collection(env, form, argv[2]);
    if (names == NULL && topics == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[3]);
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = NULL;
    if (o != NULL) {
        ADMIN_OPTIONS_T* opts = (ADMIN_OPTIONS_T*)admin_opts(o);
        err = form == 0 ? FFI_SET(r, kafka_admin_Admin_delete_topics_with_string_slice_options_err(a->view, names, opts, &ffi_err))
                        : FFI_SET(r, kafka_admin_Admin_delete_topics_with_topic_collection_options_err(a->view, topics, opts, &ffi_err));
    }
    admin_list_free(env, names, free_item);
    if (topics != NULL) error_warn(env, 1, FFI_ERR(kafka_common_TopicCollection_destroy_err(topics, &ffi_err)));
    if (o == NULL) return NULL;
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DeleteTopicsOptions_desc DeleteTopicsResult_desc

// ---- describeTopics -----------------------------------------------------------------

#define ADMIN_RPC DescribeTopics

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeTopicsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(includeAuthorizedOperations, kafka_admin_DescribeTopicsOptions_set_include_authorized_operations_err)
ADMIN_OPT_INT32(partitionSizeLimitPerResponse, kafka_admin_DescribeTopicsOptions_set_partition_size_limit_per_response_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeTopicsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeTopicsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(includeAuthorizedOperations),
     ADMIN_SETTER(partitionSizeLimitPerResponse), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeTopicsResult_destroy_err),
    {ADMIN_ACC_MAP(topic_name_values, kafka_admin_DescribeTopicsResult_topic_name_values_err, key_string, conv_topic_description),
     ADMIN_ACC_MAP(topic_id_values, kafka_admin_DescribeTopicsResult_topic_id_values_err, key_uuid, conv_topic_description),
     ADMIN_ACC_END},
};

// adminDescribeTopics(handle, form, items, options): as adminDeleteTopics's
// forms, over `_with_topic_names` (0) and `_with_topics` (1, 2).
napi_value js_admin_describe_topics(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int32_t form = 0;
    NAPI_CHECK(env, napi_get_value_int32(env, argv[1], &form), NULL);
    kafka_List_t* names = NULL;
    kafka_common_TopicCollection_t* topics = NULL;
    if (form == 0) names = string_list(env, argv[2]);
    else topics = topic_collection(env, form, argv[2]);
    if (names == NULL && topics == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[3]);
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = NULL;
    if (o != NULL) {
        ADMIN_OPTIONS_T* opts = (ADMIN_OPTIONS_T*)admin_opts(o);
        err = form == 0 ? FFI_SET(r, kafka_admin_Admin_describe_topics_with_topic_names_options_err(a->view, names, opts, &ffi_err))
                        : FFI_SET(r, kafka_admin_Admin_describe_topics_with_topics_options_err(a->view, topics, opts, &ffi_err));
    }
    admin_list_free(env, names, free_item);
    if (topics != NULL) error_warn(env, 1, FFI_ERR(kafka_common_TopicCollection_destroy_err(topics, &ffi_err)));
    if (o == NULL) return NULL;
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeTopicsOptions_desc DescribeTopicsResult_desc

// ---- createPartitions ---------------------------------------------------------------

#define ADMIN_RPC CreatePartitions

ADMIN_OPT_TIMEOUT(kafka_admin_CreatePartitionsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(validateOnly, kafka_admin_CreatePartitionsOptions_set_validate_only_err)
ADMIN_OPT_BOOL(retryOnQuotaViolation, kafka_admin_CreatePartitionsOptions_set_retry_on_quota_violation_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_CreatePartitionsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_CreatePartitionsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(validateOnly),
     ADMIN_SETTER(retryOnQuotaViolation), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_CreatePartitionsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_CreatePartitionsResult_values_err, key_string, NULL), ADMIN_ACC_END},
};

static void new_partitions_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_NewPartitions_destroy_err((kafka_admin_NewPartitions_t*)item, out_err);
}

// A JS NewPartitions's fields ({ totalCount, assignments }: an array of
// arrays of broker ids, or null) as an owned NewPartitions, or NULL with a JS
// exception pending.
static kafka_admin_NewPartitions_t* new_partitions_from_js(napi_env env, napi_value obj) {
    napi_value c = property(env, obj, "totalCount");
    int32_t total = 0;
    if (c == NULL || napi_get_value_int32(env, c, &total) != napi_ok) {
        napi_throw_type_error(env, NULL, "a NewPartitions' count");
        return NULL;
    }
    napi_value assignments = property(env, obj, "assignments");
    napi_valuetype type;
    if (assignments == NULL || napi_typeof(env, assignments, &type) != napi_ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_NewPartitions_t* p = NULL;
    kafka_common_Error_t* err = NULL;
    if (type == napi_null) {
        err = FFI_SET(p, kafka_admin_NewPartitions_increase_to_err(total, &ffi_err));
    } else {
        int ok = 0;
        uint32_t n = length_of(env, assignments, &ok);
        if (!ok) return NULL;
        kafka_List_t* outer = list_new(env);
        if (outer == NULL) return NULL;
        for (uint32_t i = 0; i < n; i++) {
            napi_value inner = element(env, assignments, i);
            kafka_List_t* ids = inner == NULL ? NULL : int_list(env, inner);
            if (ids == NULL || list_add(env, outer, ids, int_list_destroy) < 0) {
                admin_list_free(env, outer, int_list_destroy);
                return NULL;
            }
        }
        err = FFI_SET(p, kafka_admin_NewPartitions_increase_to_with_new_assignments_err(total, outer, &ffi_err));
        admin_list_free(env, outer, int_list_destroy);
    }
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return p;
}

// adminCreatePartitions(handle, newPartitions, options): newPartitions an
// array of [topic, { totalCount, assignments }]; the result's futures are
// `values` (a map).
napi_value js_admin_create_partitions(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value pair = element(env, argv[1], i);
        napi_value k = pair == NULL ? NULL : element(env, pair, 0);
        napi_value v = pair == NULL ? NULL : element(env, pair, 1);
        char* topic = k == NULL ? NULL : string_or_null(env, k, "a topic name must be a string", &ok);
        if (k == NULL || !ok) {
            ok = 0;
            break;
        }
        kafka_admin_NewPartitions_t* p = v == NULL ? NULL : new_partitions_from_js(env, v);
        if (p == NULL) {
            free(topic);
            ok = 0;
            break;
        }
        err = FFI_ERR(kafka_Map_put_err(map, topic, p, &ffi_err));
        if (err != NULL) {
            free(topic);
            FFI_DISCARD(kafka_admin_NewPartitions_destroy_err(p, &ffi_err));
        }
    }
    void* o = err == NULL && ok ? admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]) : NULL;
    ADMIN_RESULT_T* r = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_create_partitions_with_options_err(
                             a->view, map, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    // The map's keys and NewPartitions are copied by the call: freed here.
    if (map != NULL) {
        int32_t size = 0;
        kafka_common_Error_t* e2 = FFI_SET(size, kafka_Map_size_err(map, &ffi_err));
        for (int32_t i = 0; e2 == NULL && i < size; i++) {
            void* key = NULL;
            void* p = NULL;
            e2 = FFI_SET(key, kafka_Map_key_err(map, i, &ffi_err));
            if (e2 == NULL) e2 = FFI_SET(p, kafka_Map_value_err(map, i, &ffi_err));
            if (e2 == NULL) {
                free(key);
                e2 = FFI_ERR(new_partitions_destroy(p, &ffi_err));
            }
        }
        error_warn(env, 1, e2);
        error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
    }
    if (o == NULL) {
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison CreatePartitionsOptions_desc CreatePartitionsResult_desc

// ---- deleteRecords --------------------------------------------------------------------

#define ADMIN_RPC DeleteRecords

ADMIN_OPT_TIMEOUT(kafka_admin_DeleteRecordsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DeleteRecordsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DeleteRecordsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DeleteRecordsResult_destroy_err),
    {ADMIN_ACC_MAP(low_watermarks, kafka_admin_DeleteRecordsResult_low_watermarks_err, key_topic_partition, conv_deleted_records),
     ADMIN_ACC_END},
};

// adminDeleteRecords(handle, recordsToDelete, options): an array of
// [[topic, partition], beforeOffset]; the result's futures are
// `low_watermarks` (a map keyed by [topic, partition]).
napi_value js_admin_delete_records(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value pair = element(env, argv[1], i);
        napi_value key = pair == NULL ? NULL : element(env, pair, 0);
        napi_value offset_value = pair == NULL ? NULL : element(env, pair, 1);
        napi_value t = key == NULL ? NULL : element(env, key, 0);
        napi_value p = key == NULL ? NULL : element(env, key, 1);
        int32_t partition = 0;
        int64_t offset = 0;
        bool lossless = false;
        char* topic = t == NULL ? NULL : string_or_null(env, t, "a topic must be a string", &ok);
        if (t == NULL || !ok || p == NULL || napi_get_value_int32(env, p, &partition) != napi_ok || offset_value == NULL
            || napi_get_value_bigint_int64(env, offset_value, &offset, &lossless) != napi_ok) {
            free(topic);
            if (ok) napi_throw_type_error(env, NULL, "a record deletion's [[topic, partition], offset]");
            ok = 0;
            break;
        }
        kafka_common_TopicPartition_t* tp = NULL;
        kafka_admin_RecordsToDelete_t* rd = NULL;
        err = FFI_SET(tp, kafka_common_TopicPartition_new_err(topic, partition, &ffi_err));
        free(topic);
        if (err == NULL) err = FFI_SET(rd, kafka_admin_RecordsToDelete_with_offset_err(offset, &ffi_err));
        if (err == NULL) err = FFI_ERR(kafka_Map_put_err(map, tp, rd, &ffi_err));
        if (err != NULL) {
            if (tp != NULL) FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            if (rd != NULL) FFI_DISCARD(kafka_admin_RecordsToDelete_destroy_err(rd, &ffi_err));
        }
    }
    void* o = err == NULL && ok ? admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]) : NULL;
    ADMIN_RESULT_T* r = NULL;
    if (o != NULL)
        err = FFI_SET(
            r, kafka_admin_Admin_delete_records_with_options_err(a->view, map, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    if (map != NULL) {
        int32_t size = 0;
        kafka_common_Error_t* e2 = FFI_SET(size, kafka_Map_size_err(map, &ffi_err));
        for (int32_t i = 0; e2 == NULL && i < size; i++) {
            void* k = NULL;
            void* v = NULL;
            e2 = FFI_SET(k, kafka_Map_key_err(map, i, &ffi_err));
            if (e2 == NULL) e2 = FFI_SET(v, kafka_Map_value_err(map, i, &ffi_err));
            if (e2 == NULL) e2 = FFI_ERR(kafka_common_TopicPartition_destroy_err((kafka_common_TopicPartition_t*)k, &ffi_err));
            if (e2 == NULL) e2 = FFI_ERR(kafka_admin_RecordsToDelete_destroy_err((kafka_admin_RecordsToDelete_t*)v, &ffi_err));
        }
        error_warn(env, 1, e2);
        error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
    }
    if (o == NULL) {
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DeleteRecordsOptions_desc DeleteRecordsResult_desc

// ---- the mock's topics ----------------------------------------------------------------

// mockAdminAddTopic(handle, internal, name, partitions, configs): Java's
// MockAdminClient.addTopic, each partition as its fields
// (topic_partition_info_from_js), configs an array of [name, value] or null.
napi_value js_mock_admin_add_topic(napi_env env, napi_callback_info info) {
    size_t argc = 5;
    napi_value argv[5];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 5 ? admin_mock_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    bool internal = false;
    int ok = 0;
    if (napi_get_value_bool(env, argv[1], &internal) != napi_ok) {
        napi_throw_type_error(env, NULL, "internal must be a boolean");
        return NULL;
    }
    char* name = string_or_null(env, argv[2], "name must be a string", &ok);
    if (!ok) return NULL;
    uint32_t n = length_of(env, argv[3], &ok);
    kafka_List_t* partitions = ok ? list_new(env) : NULL;
    for (uint32_t i = 0; partitions != NULL && i < n; i++) {
        napi_value p = element(env, argv[3], i);
        kafka_common_TopicPartitionInfo_t* tpi = p == NULL ? NULL : topic_partition_info_from_js(env, p);
        if (tpi == NULL || list_add(env, partitions, tpi, tpi_item_destroy) < 0) {
            admin_list_free(env, partitions, tpi_item_destroy);
            partitions = NULL;
        }
    }
    int is_null = 0;
    kafka_Map_t* configs = partitions == NULL ? NULL : string_map(env, argv[4], &is_null);
    if (partitions == NULL || (configs == NULL && !is_null)) {
        free(name);
        admin_list_free(env, partitions, tpi_item_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err =
        FFI_ERR(kafka_admin_MockAdminClient_add_topic(a->mock, internal ? 1 : 0, name, partitions, configs, &ffi_err));
    free(name);
    admin_list_free(env, partitions, tpi_item_destroy);
    admin_map_free(env, configs);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockAdminMarkTopicForDeletion(handle, name): Java's markTopicForDeletion.
napi_value js_mock_admin_mark_topic_for_deletion(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 2 ? admin_mock_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    char* name = string_or_null(env, argv[1], "name must be a string", &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_admin_MockAdminClient_mark_topic_for_deletion(a->mock, name, &ffi_err));
    free(name);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockAdminSetFetchesRemainingUntilVisible(handle, topicName, fetchesRemainingUntilVisible).
napi_value js_mock_admin_set_fetches_remaining_until_visible(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_mock_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    int32_t count = 0;
    char* name = string_or_null(env, argv[1], "topicName must be a string", &ok);
    if (!ok) return NULL;
    if (napi_get_value_int32(env, argv[2], &count) != napi_ok) {
        free(name);
        napi_throw_type_error(env, NULL, "fetchesRemainingUntilVisible must be a number");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err =
        FFI_ERR(kafka_admin_MockAdminClient_set_fetches_remaining_until_visible(a->mock, name, count, &ffi_err));
    free(name);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// ---- configs and log dirs: shared values ---------------------------------------------

static void config_resource_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_config_ConfigResource_destroy_err((kafka_common_config_ConfigResource_t*)item, out_err);
}

static void tpr_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_TopicPartitionReplica_destroy_err((kafka_common_TopicPartitionReplica_t*)item, out_err);
}

static void alter_config_op_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_AlterConfigOp_destroy_err((kafka_admin_AlterConfigOp_t*)item, out_err);
}

static void alter_config_op_list_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_List_t* list = (kafka_List_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* op = NULL;
        err = FFI_SET(op, kafka_List_get_err(list, i, &ffi_err));
        if (err == NULL && op != NULL) err = FFI_ERR(alter_config_op_item_destroy(op, &ffi_err));
    }
    error_discard(err);
    kafka_List_destroy_err(list, out_err);
}

// Frees a map whose keys and values were made for a call: each key through
// `key_destroy` and each value through `value_destroy` (`free` when NULL),
// then the map.
static void map_free_with(napi_env env, kafka_Map_t* map, void (*key_destroy)(void*, kafka_common_Error_t**),
                          void (*value_destroy)(void*, kafka_common_Error_t**)) {
    if (map == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_Map_size_err(map, &ffi_err));
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* k = NULL;
        void* v = NULL;
        err = FFI_SET(k, kafka_Map_key_err(map, i, &ffi_err));
        if (err == NULL) err = FFI_SET(v, kafka_Map_value_err(map, i, &ffi_err));
        if (err != NULL) break;
        if (k != NULL) err = FFI_ERR(key_destroy(k, &ffi_err));
        if (v != NULL) {
            if (value_destroy == NULL) free(v);
            else error_warn(env, 1, FFI_ERR(value_destroy(v, &ffi_err)));
        }
    }
    error_warn(env, 1, err);
    error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
}

static int32_t int32_of(napi_env env, napi_value v, const char* what, int* ok) {
    int32_t n = 0;
    if (v == NULL || napi_get_value_int32(env, v, &n) != napi_ok) {
        *ok = 0;
        napi_throw_type_error(env, NULL, what);
    }
    return n;
}

// A JS [typeId, name] as an owned ConfigResource (the type from Java's byte
// id), or NULL with a JS exception pending.
static kafka_common_config_ConfigResource_t* config_resource_from_js(napi_env env, napi_value pair) {
    int ok = 1;
    napi_value t = element(env, pair, 0);
    napi_value n = t == NULL ? NULL : element(env, pair, 1);
    int32_t id = int32_of(env, t, "a resource's type id", &ok);
    if (!ok || n == NULL) return NULL;
    char* name = js_string_dup(env, n, "a resource's name must be a string");
    if (name == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_config_ConfigResource_Type_t* type = NULL;
    kafka_common_config_ConfigResource_t* resource = NULL;
    kafka_common_Error_t* err = FFI_SET(type, kafka_common_config_ConfigResource_Type_for_id_err((int8_t)id, &ffi_err));
    if (err == NULL) err = FFI_SET(resource, kafka_common_config_ConfigResource_new_err(type, name, &ffi_err));
    free(name);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return resource;
}

// A JS [topic, partition, brokerId] as an owned TopicPartitionReplica, or NULL
// with a JS exception pending.
static kafka_common_TopicPartitionReplica_t* tpr_from_js(napi_env env, napi_value triple) {
    int ok = 1;
    napi_value t = element(env, triple, 0);
    napi_value p = t == NULL ? NULL : element(env, triple, 1);
    napi_value b = p == NULL ? NULL : element(env, triple, 2);
    if (b == NULL) return NULL;
    int32_t partition = int32_of(env, p, "a replica's partition", &ok);
    int32_t broker = ok ? int32_of(env, b, "a replica's broker id", &ok) : 0;
    if (!ok) return NULL;
    char* topic = js_string_dup(env, t, "a replica's topic must be a string");
    if (topic == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_TopicPartitionReplica_t* replica = NULL;
    kafka_common_Error_t* err = FFI_SET(replica, kafka_common_TopicPartitionReplica_new_err(topic, partition, broker, &ffi_err));
    free(topic);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return replica;
}

// An array of `from_js` items as an owned list of owned handles, each freed
// through `destroy` on failure, or NULL with a JS exception pending.
static kafka_List_t* handle_list(napi_env env, napi_value array, void* (*from_js)(napi_env, napi_value),
                                 void (*destroy)(void*, kafka_common_Error_t**)) {
    int ok = 0;
    uint32_t n = length_of(env, array, &ok);
    if (!ok) return NULL;
    kafka_List_t* list = list_new(env);
    if (list == NULL) return NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value v = element(env, array, i);
        void* item = v == NULL ? NULL : from_js(env, v);
        if (item == NULL || list_add(env, list, item, destroy) < 0) {
            admin_list_free(env, list, destroy);
            return NULL;
        }
    }
    return list;
}

static void* config_resource_from_js_item(napi_env env, napi_value v) {
    return config_resource_from_js(env, v);
}

static void* tpr_from_js_item(napi_env env, napi_value v) {
    return tpr_from_js(env, v);
}

// A borrowed ConfigResource as [typeOrdinal, name].
static napi_value config_resource_to_js(napi_env env, const kafka_common_config_ConfigResource_t* r) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_config_ConfigResource_Type_t* type = NULL;
    const char* name = NULL;
    int ordinal = 0;
    kafka_common_Error_t* err = FFI_SET(type, kafka_common_config_ConfigResource_type_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(ordinal, (int)kafka_common_config_ConfigResource_Type__enum_err(type, &ffi_err));
    if (err == NULL) err = FFI_SET(name, kafka_common_config_ConfigResource_name_err(r, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value pair;
    NAPI_CHECK(env, napi_create_array_with_length(env, 2, &pair), NULL);
    napi_value o = js_int32(env, ordinal), s = js_string_or_null(env, name);
    if (o == NULL || s == NULL) return NULL;
    NAPI_CHECK(env, napi_set_element(env, pair, 0, o), NULL);
    NAPI_CHECK(env, napi_set_element(env, pair, 1, s), NULL);
    return pair;
}

static napi_value key_config_resource(napi_env env, const void* k) {
    return config_resource_to_js(env, (const kafka_common_config_ConfigResource_t*)k);
}

static napi_value item_config_resource(napi_env env, const void* item) {
    return config_resource_to_js(env, (const kafka_common_config_ConfigResource_t*)item);
}

// A list of ConfigResource as an array of [typeOrdinal, name].
static napi_value conv_config_resources(napi_env env, void* v) {
    return admin_list_to_js(env, (const kafka_List_t*)v, item_config_resource, 0);
}

// A borrowed int32_t broker id.
static napi_value key_broker_id(napi_env env, const void* k) {
    return js_int32(env, *(const int32_t*)k);
}

// A borrowed TopicPartitionReplica as [topic, partition, brokerId].
static napi_value key_tpr(napi_env env, const void* k) {
    const kafka_common_TopicPartitionReplica_t* r = (const kafka_common_TopicPartitionReplica_t*)k;
    kafka_common_Error_t* ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = 0, broker = 0;
    kafka_common_Error_t* err = FFI_SET(topic, kafka_common_TopicPartitionReplica_topic_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(partition, kafka_common_TopicPartitionReplica_partition_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(broker, kafka_common_TopicPartitionReplica_broker_id_err(r, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value triple;
    NAPI_CHECK(env, napi_create_array_with_length(env, 3, &triple), NULL);
    napi_value t = js_string_or_null(env, topic), p = js_int32(env, partition), b = js_int32(env, broker);
    if (t == NULL || p == NULL || b == NULL) return NULL;
    NAPI_CHECK(env, napi_set_element(env, triple, 0, t), NULL);
    NAPI_CHECK(env, napi_set_element(env, triple, 1, p), NULL);
    NAPI_CHECK(env, napi_set_element(env, triple, 2, b), NULL);
    return triple;
}

static napi_value js_int64(napi_env env, int64_t n) {
    napi_value v = NULL;
    if (napi_create_bigint_int64(env, n, &v) != napi_ok) js_throw_last_napi_error(env, "napi_create_bigint_int64");
    return v;
}

// A ReplicaInfo as { size, offsetLag, isFuture }.
static napi_value item_replica_info(napi_env env, const void* item) {
    const kafka_admin_ReplicaInfo_t* r = (const kafka_admin_ReplicaInfo_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    int64_t size = 0, lag = 0;
    int8_t future = 0;
    kafka_common_Error_t* err = FFI_SET(size, kafka_admin_ReplicaInfo_size_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(lag, kafka_admin_ReplicaInfo_offset_lag_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(future, kafka_admin_ReplicaInfo_is_future_err(r, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "size", js_int64(env, size)) < 0 || set(env, obj, "offsetLag", js_int64(env, lag)) < 0
        || set(env, obj, "isFuture", js_bool(env, future)) < 0)
        return NULL;
    return obj;
}

// A LogDirDescription as { error, replicaInfos, totalBytes, usableBytes,
// isCordoned }: the error its JS error (null for none; it is borrowed from the
// description), the replicas an array of [[topic, partition], replica], each
// size a bigint, -1 for Java's empty OptionalLong.
static napi_value item_log_dir_description(napi_env env, const void* item) {
    const kafka_admin_LogDirDescription_t* d = (const kafka_admin_LogDirDescription_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_Error_t* error = NULL;
    kafka_Map_t* infos = NULL;
    int64_t total = 0, usable = 0;
    int8_t cordoned = 0;
    kafka_common_Error_t* err = FFI_SET(error, kafka_admin_LogDirDescription_error_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(infos, kafka_admin_LogDirDescription_replica_infos_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(total, kafka_admin_LogDirDescription_total_bytes_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(usable, kafka_admin_LogDirDescription_usable_bytes_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(cordoned, kafka_admin_LogDirDescription_is_cordoned_err(d, &ffi_err));
    // The map is owned, its entries with it: converted, then destroyed.
    napi_value replicas = err == NULL ? admin_map_to_js(env, infos, key_topic_partition, item_replica_info) : NULL;
    if (infos != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(infos, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (replicas == NULL) return NULL;
    napi_value js_error = error == NULL ? js_null(env) : error_to_js(env, error);
    if (js_error == NULL) return NULL;
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "error", js_error) < 0 || set(env, obj, "replicaInfos", replicas) < 0
        || set(env, obj, "totalBytes", js_int64(env, total)) < 0 || set(env, obj, "usableBytes", js_int64(env, usable)) < 0
        || set(env, obj, "isCordoned", js_bool(env, cordoned)) < 0)
        return NULL;
    return obj;
}

// A broker's log directories: [[path, description]], sorted by path.
static napi_value conv_log_dir_descriptions(napi_env env, void* v) {
    return admin_map_to_js(env, (const kafka_Map_t*)v, admin_item_string, item_log_dir_description);
}

// A ReplicaLogDirInfo as { currentReplicaLogDir, currentReplicaOffsetLag,
// futureReplicaLogDir, futureReplicaOffsetLag }, the dirs null for Java's
// null.
static napi_value conv_replica_log_dir_info(napi_env env, void* v) {
    if (v == NULL) return js_null(env);
    const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t* i =
        (const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t*)v;
    kafka_common_Error_t* ffi_err = NULL;
    const char *current = NULL, *future = NULL;
    int64_t current_lag = 0, future_lag = 0;
    kafka_common_Error_t* err =
        FFI_SET(current, kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir_err(i, &ffi_err));
    if (err == NULL)
        err = FFI_SET(current_lag, kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_offset_lag_err(i, &ffi_err));
    if (err == NULL) err = FFI_SET(future, kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir_err(i, &ffi_err));
    if (err == NULL)
        err = FFI_SET(future_lag, kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_offset_lag_err(i, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "currentReplicaLogDir", js_string_or_null(env, current)) < 0
        || set(env, obj, "currentReplicaOffsetLag", js_int64(env, current_lag)) < 0
        || set(env, obj, "futureReplicaLogDir", js_string_or_null(env, future)) < 0
        || set(env, obj, "futureReplicaOffsetLag", js_int64(env, future_lag)) < 0)
        return NULL;
    return obj;
}

// ---- describeConfigs --------------------------------------------------------------------

#define ADMIN_RPC DescribeConfigs

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeConfigsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(includeSynonyms, kafka_admin_DescribeConfigsOptions_set_include_synonyms_err)
ADMIN_OPT_BOOL(includeDocumentation, kafka_admin_DescribeConfigsOptions_set_include_documentation_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeConfigsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeConfigsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(includeSynonyms), ADMIN_SETTER(includeDocumentation), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeConfigsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_DescribeConfigsResult_values_err, key_config_resource, conv_config), ADMIN_ACC_END},
};

// adminDescribeConfigs(handle, resources, options): each resource as
// [typeId, name]; the result's futures are `values` (a map keyed by
// [typeOrdinal, name]).
napi_value js_admin_describe_configs(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* resources = handle_list(env, argv[1], config_resource_from_js_item, config_resource_item_destroy);
    if (resources == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, resources, config_resource_item_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_describe_configs_with_options_err(a->view, resources, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, resources, config_resource_item_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeConfigsOptions_desc DescribeConfigsResult_desc

// ---- incrementalAlterConfigs ----------------------------------------------------------

#define ADMIN_RPC AlterConfigs

ADMIN_OPT_TIMEOUT(kafka_admin_AlterConfigsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(validateOnly, kafka_admin_AlterConfigsOptions_set_validate_only_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_AlterConfigsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_AlterConfigsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(validateOnly), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_AlterConfigsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_AlterConfigsResult_values_err, key_config_resource, NULL), ADMIN_ACC_END},
};

// A JS [opId, name, value] as an owned AlterConfigOp over a new
// ConfigEntry(name, value) (value null for Java's null), or NULL with a JS
// exception pending.
static kafka_admin_AlterConfigOp_t* alter_config_op_from_js(napi_env env, napi_value triple) {
    int ok = 1;
    napi_value t = element(env, triple, 0);
    napi_value n = t == NULL ? NULL : element(env, triple, 1);
    napi_value v = n == NULL ? NULL : element(env, triple, 2);
    if (v == NULL) return NULL;
    int32_t id = int32_of(env, t, "an operation's type id", &ok);
    if (!ok) return NULL;
    char* name = js_string_dup(env, n, "a config name must be a string");
    if (name == NULL) return NULL;
    char* value = string_or_null(env, v, "a config value must be a string or null", &ok);
    if (!ok) {
        free(name);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_admin_AlterConfigOp_OpType_t* type = NULL;
    kafka_admin_ConfigEntry_t* entry = NULL;
    kafka_admin_AlterConfigOp_t* op = NULL;
    kafka_common_Error_t* err = FFI_SET(type, kafka_admin_AlterConfigOp_OpType_for_id_err((int8_t)id, &ffi_err));
    if (err == NULL && type == NULL) {
        free(name);
        free(value);
        napi_throw_type_error(env, NULL, "an operation's type id has no OpType");
        return NULL;
    }
    if (err == NULL) err = FFI_SET(entry, kafka_admin_ConfigEntry_new_err(name, value, &ffi_err));
    free(name);
    free(value);
    if (err == NULL) err = FFI_SET(op, kafka_admin_AlterConfigOp_new_err(entry, type, &ffi_err));
    // The entry is copied into the operation.
    if (entry != NULL) error_warn(env, 1, FFI_ERR(kafka_admin_ConfigEntry_destroy_err(entry, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return op;
}

static void* alter_config_op_from_js_item(napi_env env, napi_value v) {
    return alter_config_op_from_js(env, v);
}

// adminIncrementalAlterConfigs(handle, configs, options): configs an array of
// [[typeId, name], [[opId, name, value]]]; the result's futures are `values`
// (a map keyed by [typeOrdinal, name]).
napi_value js_admin_incremental_alter_configs(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value pair = element(env, argv[1], i);
        napi_value k = pair == NULL ? NULL : element(env, pair, 0);
        napi_value v = k == NULL ? NULL : element(env, pair, 1);
        kafka_common_config_ConfigResource_t* resource = v == NULL ? NULL : config_resource_from_js(env, k);
        kafka_List_t* ops = resource == NULL ? NULL : handle_list(env, v, alter_config_op_from_js_item, alter_config_op_item_destroy);
        if (ops == NULL) {
            if (resource != NULL) FFI_DISCARD(kafka_common_config_ConfigResource_destroy_err(resource, &ffi_err));
            ok = 0;
            break;
        }
        err = FFI_ERR(kafka_Map_put_err(map, resource, ops, &ffi_err));
        if (err != NULL) {
            FFI_DISCARD(kafka_common_config_ConfigResource_destroy_err(resource, &ffi_err));
            FFI_DISCARD(alter_config_op_list_destroy(ops, &ffi_err));
        }
    }
    void* o = err == NULL && ok ? admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]) : NULL;
    ADMIN_RESULT_T* r = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_incremental_alter_configs_with_options_err(a->view, map, (ADMIN_OPTIONS_T*)admin_opts(o),
                                                                                    &ffi_err));
    // The map's resources and operations are copied by the call: freed here.
    map_free_with(env, map, config_resource_item_destroy, alter_config_op_list_destroy);
    if (o == NULL) {
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison AlterConfigsOptions_desc AlterConfigsResult_desc

// ---- listConfigResources -----------------------------------------------------------------

#define ADMIN_RPC ListConfigResources

ADMIN_OPT_TIMEOUT(kafka_admin_ListConfigResourcesOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ListConfigResourcesOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListConfigResourcesOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ListConfigResourcesResult_destroy_err),
    {ADMIN_ACC_ONE(all, kafka_admin_ListConfigResourcesResult_all_err, conv_config_resources), ADMIN_ACC_END},
};

// adminListConfigResources(handle, typeIds, options): the types as Java's
// byte ids (an empty array lists every type); the result's one future is
// `all`, the resources as [typeOrdinal, name].
napi_value js_admin_list_config_resources(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_List_t* types = list_new(env);
    if (types == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    // The types are borrowed singletons: the list is freed without them.
    for (uint32_t i = 0; i < n; i++) {
        napi_value v = element(env, argv[1], i);
        int32_t id = int32_of(env, v, "a resource type's id", &ok);
        const kafka_common_config_ConfigResource_Type_t* type = NULL;
        kafka_common_Error_t* err = ok ? FFI_SET(type, kafka_common_config_ConfigResource_Type_for_id_err((int8_t)id, &ffi_err)) : NULL;
        if (err != NULL) error_throw_owned(env, err);
        if (!ok || err != NULL || list_add(env, types, (void*)type, NULL) < 0) {
            admin_list_free(env, types, NULL);
            return NULL;
        }
    }
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, types, NULL);
        return NULL;
    }
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_list_config_resources_with_options_err(a->view, types, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, types, NULL);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ListConfigResourcesOptions_desc ListConfigResourcesResult_desc

// ---- describeLogDirs -------------------------------------------------------------------

#define ADMIN_RPC DescribeLogDirs

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeLogDirsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeLogDirsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeLogDirsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeLogDirsResult_destroy_err),
    {ADMIN_ACC_MAP(descriptions, kafka_admin_DescribeLogDirsResult_descriptions_err, key_broker_id, conv_log_dir_descriptions),
     ADMIN_ACC_END},
};

// adminDescribeLogDirs(handle, brokers, options): the broker ids; the
// result's futures are `descriptions` (a map keyed by broker id).
napi_value js_admin_describe_log_dirs(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* brokers = int_list(env, argv[1]);
    if (brokers == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, brokers, free_item);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_describe_log_dirs_with_options_err(a->view, brokers, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, brokers, free_item);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeLogDirsOptions_desc DescribeLogDirsResult_desc

// ---- alterReplicaLogDirs -----------------------------------------------------------------

#define ADMIN_RPC AlterReplicaLogDirs

ADMIN_OPT_TIMEOUT(kafka_admin_AlterReplicaLogDirsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_AlterReplicaLogDirsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_AlterReplicaLogDirsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_AlterReplicaLogDirsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_AlterReplicaLogDirsResult_values_err, key_tpr, NULL), ADMIN_ACC_END},
};

// adminAlterReplicaLogDirs(handle, replicaAssignment, options): an array of
// [[topic, partition, brokerId], logDir], the log dir null for Java's null;
// the result's futures are `values` (a map keyed by [topic, partition,
// brokerId]).
napi_value js_admin_alter_replica_log_dirs(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value pair = element(env, argv[1], i);
        napi_value k = pair == NULL ? NULL : element(env, pair, 0);
        napi_value v = k == NULL ? NULL : element(env, pair, 1);
        kafka_common_TopicPartitionReplica_t* replica = v == NULL ? NULL : tpr_from_js(env, k);
        if (replica == NULL) {
            ok = 0;
            break;
        }
        char* dir = string_or_null(env, v, "a log directory must be a string or null", &ok);
        if (!ok) {
            FFI_DISCARD(kafka_common_TopicPartitionReplica_destroy_err(replica, &ffi_err));
            break;
        }
        err = FFI_ERR(kafka_Map_put_err(map, replica, dir, &ffi_err));
        if (err != NULL) {
            FFI_DISCARD(kafka_common_TopicPartitionReplica_destroy_err(replica, &ffi_err));
            free(dir);
        }
    }
    void* o = err == NULL && ok ? admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]) : NULL;
    ADMIN_RESULT_T* r = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_alter_replica_log_dirs_with_options_err(a->view, map, (ADMIN_OPTIONS_T*)admin_opts(o),
                                                                                 &ffi_err));
    // The map's replicas and directories are copied by the call: freed here.
    map_free_with(env, map, tpr_item_destroy, NULL);
    if (o == NULL) {
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison AlterReplicaLogDirsOptions_desc AlterReplicaLogDirsResult_desc

// ---- describeReplicaLogDirs --------------------------------------------------------------

#define ADMIN_RPC DescribeReplicaLogDirs

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeReplicaLogDirsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeReplicaLogDirsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeReplicaLogDirsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeReplicaLogDirsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_DescribeReplicaLogDirsResult_values_err, key_tpr, conv_replica_log_dir_info), ADMIN_ACC_END},
};

// adminDescribeReplicaLogDirs(handle, replicas, options): each replica as
// [topic, partition, brokerId]; the result's futures are `values` (a map
// keyed by [topic, partition, brokerId]).
napi_value js_admin_describe_replica_log_dirs(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* replicas = handle_list(env, argv[1], tpr_from_js_item, tpr_item_destroy);
    if (replicas == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, replicas, tpr_item_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_describe_replica_log_dirs_with_options_err(a->view, replicas, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, replicas, tpr_item_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeReplicaLogDirsOptions_desc DescribeReplicaLogDirsResult_desc

// ---- leaders, reassignments and offsets: shared values ----------------------------------

static void tp_item_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_TopicPartition_destroy_err((kafka_common_TopicPartition_t*)item, out_err);
}

// A JS [topic, partition] as an owned TopicPartition, or NULL with a JS
// exception pending.
static kafka_common_TopicPartition_t* tp_from_js(napi_env env, napi_value pair) {
    int ok = 1;
    napi_value t = element(env, pair, 0);
    napi_value p = t == NULL ? NULL : element(env, pair, 1);
    if (p == NULL) return NULL;
    int32_t partition = int32_of(env, p, "a partition's index", &ok);
    if (!ok) return NULL;
    char* topic = js_string_dup(env, t, "a partition's topic must be a string");
    if (topic == NULL) return NULL;
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

static void* tp_from_js_item(napi_env env, napi_value v) {
    return tp_from_js(env, v);
}

// An array of [topic, partition] as an owned list of owned TopicPartitions;
// `null` is NULL with none (*is_null), Java's null set; NULL with a JS
// exception pending on failure.
static kafka_List_t* tp_list_or_null(napi_env env, napi_value array, int* is_null) {
    napi_valuetype type;
    *is_null = 0;
    if (napi_typeof(env, array, &type) != napi_ok) {
        js_throw_last_napi_error(env, "napi_typeof");
        return NULL;
    }
    if (type == napi_null) {
        *is_null = 1;
        return NULL;
    }
    return handle_list(env, array, tp_from_js_item, tp_item_destroy);
}

// A borrowed int32_t.
static napi_value item_int32(napi_env env, const void* item) {
    return js_int32(env, *(const int32_t*)item);
}

// A borrowed error, or NULL (Java's empty Optional) as `null`.
static napi_value item_error_or_null(napi_env env, const void* item) {
    return item == NULL ? js_null(env) : error_to_js(env, (const kafka_common_Error_t*)item);
}

// A PartitionReassignment as { replicas, addingReplicas, removingReplicas },
// each owned list converted, then destroyed with its elements.
static napi_value item_partition_reassignment(napi_env env, const void* item) {
    const kafka_admin_PartitionReassignment_t* r = (const kafka_admin_PartitionReassignment_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t *replicas = NULL, *adding = NULL, *removing = NULL;
    kafka_common_Error_t* err = FFI_SET(replicas, kafka_admin_PartitionReassignment_replicas_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(adding, kafka_admin_PartitionReassignment_adding_replicas_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(removing, kafka_admin_PartitionReassignment_removing_replicas_err(r, &ffi_err));
    napi_value lists[3];
    lists[0] = owned_list_to_js(env, replicas, item_int32, 0);
    lists[1] = owned_list_to_js(env, adding, item_int32, 0);
    lists[2] = owned_list_to_js(env, removing, item_int32, 0);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "replicas", lists[0]) < 0 || set(env, obj, "addingReplicas", lists[1]) < 0
        || set(env, obj, "removingReplicas", lists[2]) < 0)
        return NULL;
    return obj;
}

// The elections' outcomes: [[[topic, partition], error or null]].
static napi_value conv_elections(napi_env env, void* v) {
    return admin_map_to_js(env, (const kafka_Map_t*)v, key_topic_partition, item_error_or_null);
}

// The reassignments: [[[topic, partition], reassignment]].
static napi_value conv_reassignments(napi_env env, void* v) {
    return admin_map_to_js(env, (const kafka_Map_t*)v, key_topic_partition, item_partition_reassignment);
}

// A ListOffsetsResultInfo as { offset, timestamp, leaderEpoch }, the epoch
// null for Java's empty Optional (-1).
static napi_value conv_list_offsets_info(napi_env env, void* v) {
    if (v == NULL) return js_null(env);
    const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t* i = (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t*)v;
    kafka_common_Error_t* ffi_err = NULL;
    int64_t offset = 0, timestamp = 0;
    int32_t epoch = -1;
    kafka_common_Error_t* err = FFI_SET(offset, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset_err(i, &ffi_err));
    if (err == NULL) err = FFI_SET(timestamp, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp_err(i, &ffi_err));
    if (err == NULL) err = FFI_SET(epoch, kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch_err(i, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "offset", js_int64(env, offset)) < 0 || set(env, obj, "timestamp", js_int64(env, timestamp)) < 0
        || set(env, obj, "leaderEpoch", epoch < 0 ? js_null(env) : js_int32(env, epoch)) < 0)
        return NULL;
    return obj;
}

// ---- electLeaders -------------------------------------------------------------------------

#define ADMIN_RPC ElectLeaders

ADMIN_OPT_TIMEOUT(kafka_admin_ElectLeadersOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ElectLeadersOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ElectLeadersOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ElectLeadersResult_destroy_err),
    {ADMIN_ACC_ONE(partitions, kafka_admin_ElectLeadersResult_partitions_err, conv_elections), ADMIN_ACC_END},
};

// adminElectLeaders(handle, electionType, partitions, options): the election
// type as its ordinal (0 PREFERRED, 1 UNCLEAN), the partitions as [topic,
// partition] pairs or null for every partition; the result's one future is
// `partitions`.
napi_value js_admin_elect_leaders(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 1;
    int32_t ordinal = int32_of(env, argv[1], "an election type's ordinal", &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_ElectionType_t* type = NULL;
    kafka_common_Error_t* err = ordinal == 1 ? FFI_SET(type, kafka_common_ElectionType_unclean_err(&ffi_err))
                                             : FFI_SET(type, kafka_common_ElectionType_preferred_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    int is_null = 0;
    kafka_List_t* partitions = tp_list_or_null(env, argv[2], &is_null);
    if (partitions == NULL && !is_null) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[3]);
    if (o == NULL) {
        admin_list_free(env, partitions, tp_item_destroy);
        return NULL;
    }
    ADMIN_RESULT_T* r = NULL;
    err = FFI_SET(r, kafka_admin_Admin_elect_leaders_with_options_err(a->view, type, partitions, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, partitions, tp_item_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ElectLeadersOptions_desc ElectLeadersResult_desc

// ---- alterPartitionReassignments ----------------------------------------------------------

#define ADMIN_RPC AlterPartitionReassignments

ADMIN_OPT_TIMEOUT(kafka_admin_AlterPartitionReassignmentsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(allowReplicationFactorChange, kafka_admin_AlterPartitionReassignmentsOptions_set_allow_replication_factor_change_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_AlterPartitionReassignmentsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_AlterPartitionReassignmentsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(allowReplicationFactorChange), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_AlterPartitionReassignmentsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_AlterPartitionReassignmentsResult_values_err, key_topic_partition, NULL), ADMIN_ACC_END},
};

static void new_reassignment_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_NewPartitionReassignment_destroy_err((kafka_admin_NewPartitionReassignment_t*)item, out_err);
}

// adminAlterPartitionReassignments(handle, reassignments, options): an array
// of [[topic, partition], targetReplicas or null], null cancelling (Java's
// empty Optional); the result's futures are `values` (a map keyed by [topic,
// partition]).
napi_value js_admin_alter_partition_reassignments(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value pair = element(env, argv[1], i);
        napi_value k = pair == NULL ? NULL : element(env, pair, 0);
        napi_value v = k == NULL ? NULL : element(env, pair, 1);
        kafka_common_TopicPartition_t* tp = v == NULL ? NULL : tp_from_js(env, k);
        if (tp == NULL) {
            ok = 0;
            break;
        }
        napi_valuetype type;
        kafka_admin_NewPartitionReassignment_t* target = NULL;
        if (napi_typeof(env, v, &type) != napi_ok) {
            FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            js_throw_last_napi_error(env, "napi_typeof");
            ok = 0;
            break;
        }
        if (type != napi_null) {
            kafka_List_t* replicas = int_list(env, v);
            if (replicas == NULL) {
                FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
                ok = 0;
                break;
            }
            err = FFI_SET(target, kafka_admin_NewPartitionReassignment_new(replicas, &ffi_err));
            admin_list_free(env, replicas, free_item);
            if (err != NULL) {
                FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
                break;
            }
        }
        err = FFI_ERR(kafka_Map_put_err(map, tp, target, &ffi_err));
        if (err != NULL) {
            FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            if (target != NULL) FFI_DISCARD(kafka_admin_NewPartitionReassignment_destroy_err(target, &ffi_err));
        }
    }
    void* o = err == NULL && ok ? admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]) : NULL;
    ADMIN_RESULT_T* r = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_alter_partition_reassignments_with_options_err(a->view, map, (ADMIN_OPTIONS_T*)admin_opts(o),
                                                                                         &ffi_err));
    // The map's keys and reassignments are copied by the call: freed here.
    map_free_with(env, map, tp_item_destroy, new_reassignment_destroy);
    if (o == NULL) {
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison AlterPartitionReassignmentsOptions_desc AlterPartitionReassignmentsResult_desc

// ---- listPartitionReassignments -----------------------------------------------------------

#define ADMIN_RPC ListPartitionReassignments

ADMIN_OPT_TIMEOUT(kafka_admin_ListPartitionReassignmentsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ListPartitionReassignmentsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListPartitionReassignmentsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ListPartitionReassignmentsResult_destroy_err),
    {ADMIN_ACC_ONE(reassignments, kafka_admin_ListPartitionReassignmentsResult_reassignments_err, conv_reassignments), ADMIN_ACC_END},
};

// adminListPartitionReassignments(handle, partitions, options): the
// partitions as [topic, partition] pairs (`_with_partitions_options`), or
// null for every partition (`_with_options`, Java's empty Optional); the
// result's one future is `reassignments`.
napi_value js_admin_list_partition_reassignments(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int is_null = 0;
    kafka_List_t* partitions = tp_list_or_null(env, argv[1], &is_null);
    if (partitions == NULL && !is_null) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, partitions, tp_item_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    ADMIN_OPTIONS_T* opts = (ADMIN_OPTIONS_T*)admin_opts(o);
    kafka_common_Error_t* err =
        partitions == NULL
            ? FFI_SET(r, kafka_admin_Admin_list_partition_reassignments_with_options_err(a->view, opts, &ffi_err))
            : FFI_SET(r, kafka_admin_Admin_list_partition_reassignments_with_partitions_options_err(a->view, partitions, opts, &ffi_err));
    admin_list_free(env, partitions, tp_item_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ListPartitionReassignmentsOptions_desc ListPartitionReassignmentsResult_desc

// ---- listOffsets --------------------------------------------------------------------------

#define ADMIN_RPC ListOffsets

// Java's `new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)`: the FFI has
// no setter of the isolation level, a final field of Java's, only its
// constructor `with_isolation_level`.
static ADMIN_OPTIONS_T* list_offsets_options_read_committed(kafka_common_Error_t** out_err) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_IsolationLevel_t* level = NULL;
    kafka_common_Error_t* err = FFI_SET(level, kafka_common_IsolationLevel_read_committed_err(&ffi_err));
    if (err != NULL) {
        *out_err = err;
        return NULL;
    }
    return kafka_admin_ListOffsetsOptions_with_isolation_level_err(level, out_err);
}

ADMIN_OPT_TIMEOUT(kafka_admin_ListOffsetsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ListOffsetsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListOffsetsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
// The same options, from Java's READ_COMMITTED constructor.
static const AdminOptionsDesc ListOffsetsReadCommittedOptions_desc = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(list_offsets_options_read_committed),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListOffsetsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};

// `partition_result(partition)`, the key a [topic, partition] pair.
static kafka_common_KafkaFuture_t* list_offsets_partition_result(napi_env env, const void* result, napi_value key,
                                                                  kafka_common_Error_t** out_err) {
    kafka_common_TopicPartition_t* tp = tp_from_js(env, key);
    if (tp == NULL) return NULL;
    kafka_common_KafkaFuture_t* future = kafka_admin_ListOffsetsResult_partition_result((const ADMIN_RESULT_T*)result, tp, out_err);
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 1, FFI_ERR(kafka_common_TopicPartition_destroy_err(tp, &ffi_err)));
    return future;
}

static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ListOffsetsResult_destroy_err),
    {ADMIN_ACC_JS(partition_result, list_offsets_partition_result, conv_list_offsets_info), ADMIN_ACC_END},
};

// An OffsetSpec of the C enum's order (0 EARLIEST, 1 LATEST, 2 MAX_TIMESTAMP,
// 3 EARLIEST_LOCAL, 4 LATEST_TIERED, 5 EARLIEST_PENDING_UPLOAD, 6 TIMESTAMP
// with `timestamp`): a singleton, or an owned TimestampSpec, both freed with
// kafka_admin_OffsetSpec_destroy, which takes a singleton too (a no-op).
static const kafka_admin_OffsetSpec_t* offset_spec_of(int32_t spec, int64_t timestamp, kafka_common_Error_t** out_err) {
    switch (spec) {
        case 0: return kafka_admin_OffsetSpec_earliest_err(out_err);
        case 2: return kafka_admin_OffsetSpec_max_timestamp_err(out_err);
        case 3: return kafka_admin_OffsetSpec_earliest_local_err(out_err);
        case 4: return kafka_admin_OffsetSpec_latest_tiered_err(out_err);
        case 5: return kafka_admin_OffsetSpec_earliest_pending_upload_err(out_err);
        case 6: return kafka_admin_OffsetSpec_for_timestamp_err(timestamp, out_err);
        default: return kafka_admin_OffsetSpec_latest_err(out_err);
    }
}

static void offset_spec_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_OffsetSpec_destroy_err((kafka_admin_OffsetSpec_t*)item, out_err);
}

// adminListOffsets(handle, topicPartitionOffsets, readCommitted, options): an
// array of [[topic, partition], spec, timestamp] (offset_spec_of), the
// options built from Java's READ_COMMITTED constructor when readCommitted;
// the result's futures are each partition's `partition_result`.
napi_value js_admin_list_offsets(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    bool read_committed = false;
    if (napi_get_value_bool(env, argv[2], &read_committed) != napi_ok) {
        napi_throw_type_error(env, NULL, "readCommitted must be a boolean");
        return NULL;
    }
    int ok = 0;
    uint32_t n = length_of(env, argv[1], &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value row = element(env, argv[1], i);
        napi_value k = row == NULL ? NULL : element(env, row, 0);
        napi_value s = k == NULL ? NULL : element(env, row, 1);
        napi_value t = s == NULL ? NULL : element(env, row, 2);
        int32_t spec = t == NULL ? 0 : int32_of(env, s, "an offset spec", &ok);
        int64_t timestamp = 0;
        bool lossless = false;
        if (t == NULL || !ok || napi_get_value_bigint_int64(env, t, &timestamp, &lossless) != napi_ok) {
            if (t != NULL && ok) napi_throw_type_error(env, NULL, "an offset spec's timestamp must be a bigint");
            ok = 0;
            break;
        }
        kafka_common_TopicPartition_t* tp = tp_from_js(env, k);
        if (tp == NULL) {
            ok = 0;
            break;
        }
        const kafka_admin_OffsetSpec_t* value = NULL;
        err = FFI_SET(value, offset_spec_of(spec, timestamp, &ffi_err));
        if (err == NULL) err = FFI_ERR(kafka_Map_put_err(map, tp, (void*)value, &ffi_err));
        if (err != NULL) {
            FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            if (value != NULL) FFI_DISCARD(kafka_admin_OffsetSpec_destroy_err((kafka_admin_OffsetSpec_t*)value, &ffi_err));
        }
    }
    const AdminOptionsDesc* odesc = read_committed ? &ListOffsetsReadCommittedOptions_desc : &ADMIN_OPTIONS_DESC;
    void* o = err == NULL && ok ? admin_options_build(env, odesc, argv[3]) : NULL;
    ADMIN_RESULT_T* r = NULL;
    if (o != NULL) err = FFI_SET(r, kafka_admin_Admin_list_offsets_with_options_err(a->view, map, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    // The map's keys and specs are copied by the call: freed here.
    map_free_with(env, map, tp_item_destroy, offset_spec_destroy);
    if (o == NULL) {
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return admin_rpc_return(env, odesc, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ListOffsetsOptions_desc ListOffsetsReadCommittedOptions_desc ListOffsetsResult_desc

// ---- the mock's offsets -------------------------------------------------------------------

// An array of [[topic, partition], offset] as an owned map of owned
// TopicPartitions to malloc'ed int64_t, or NULL with a JS exception pending.
static kafka_Map_t* offsets_map_from_js(napi_env env, napi_value rows) {
    int ok = 0;
    uint32_t n = length_of(env, rows, &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value pair = element(env, rows, i);
        napi_value k = pair == NULL ? NULL : element(env, pair, 0);
        napi_value v = k == NULL ? NULL : element(env, pair, 1);
        int64_t* offset = (int64_t*)malloc(sizeof(int64_t));
        bool lossless = false;
        if (v == NULL || offset == NULL || napi_get_value_bigint_int64(env, v, offset, &lossless) != napi_ok) {
            free(offset);
            if (v != NULL) napi_throw_type_error(env, NULL, "an offset must be a bigint");
            ok = 0;
            break;
        }
        kafka_common_TopicPartition_t* tp = tp_from_js(env, k);
        if (tp == NULL) {
            free(offset);
            ok = 0;
            break;
        }
        err = FFI_ERR(kafka_Map_put_err(map, tp, offset, &ffi_err));
        if (err != NULL) {
            FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            free(offset);
        }
    }
    if (err != NULL || !ok) {
        map_free_with(env, map, tp_item_destroy, NULL);
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return map;
}

// mockAdminUpdateOffsets(handle, end, newOffsets): Java's
// MockAdminClient.updateEndOffsets (end) or updateBeginningOffsets, the
// offsets an array of [[topic, partition], offset].
napi_value js_mock_admin_update_offsets(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_mock_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    bool end = false;
    if (napi_get_value_bool(env, argv[1], &end) != napi_ok) {
        napi_throw_type_error(env, NULL, "end must be a boolean");
        return NULL;
    }
    kafka_Map_t* map = offsets_map_from_js(env, argv[2]);
    if (map == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = end ? FFI_ERR(kafka_admin_MockAdminClient_update_end_offsets_err(a->mock, map, &ffi_err))
                                    : FFI_ERR(kafka_admin_MockAdminClient_update_beginning_offsets_err(a->mock, map, &ffi_err));
    // The map is copied by the call: its keys and offsets are freed here.
    map_free_with(env, map, tp_item_destroy, NULL);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// ---- groups: shared values ------------------------------------------------------------------

// A GroupState of Java's ordinal, a singleton.
static const kafka_common_GroupState_t* group_state_of(int32_t ordinal, kafka_common_Error_t** out_err) {
    switch (ordinal) {
        case 1: return kafka_common_GroupState_preparing_rebalance_err(out_err);
        case 2: return kafka_common_GroupState_completing_rebalance_err(out_err);
        case 3: return kafka_common_GroupState_stable_err(out_err);
        case 4: return kafka_common_GroupState_dead_err(out_err);
        case 5: return kafka_common_GroupState_empty_err(out_err);
        case 6: return kafka_common_GroupState_assigning_err(out_err);
        case 7: return kafka_common_GroupState_reconciling_err(out_err);
        case 8: return kafka_common_GroupState_not_ready_err(out_err);
        default: return kafka_common_GroupState_unknown_err(out_err);
    }
}

// A GroupType of Java's ordinal, a singleton.
static const kafka_common_GroupType_t* group_type_of(int32_t ordinal, kafka_common_Error_t** out_err) {
    switch (ordinal) {
        case 1: return kafka_common_GroupType_consumer_err(out_err);
        case 2: return kafka_common_GroupType_classic_err(out_err);
        case 3: return kafka_common_GroupType_share_err(out_err);
        case 4: return kafka_common_GroupType_streams_err(out_err);
        default: return kafka_common_GroupType_unknown_err(out_err);
    }
}

// An array of ordinals as an owned list of borrowed singletons (`of` each),
// or NULL with a JS exception pending.
static kafka_List_t* singleton_list(napi_env env, napi_value array, const void* (*of)(int32_t, kafka_common_Error_t**)) {
    int ok = 0;
    uint32_t n = length_of(env, array, &ok);
    if (!ok) return NULL;
    kafka_List_t* list = list_new(env);
    if (list == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value v = element(env, array, i);
        int32_t ordinal = int32_of(env, v, "an enum's ordinal", &ok);
        const void* item = NULL;
        kafka_common_Error_t* err = ok ? FFI_SET(item, of(ordinal, &ffi_err)) : NULL;
        if (!ok || err != NULL) {
            admin_list_free(env, list, NULL);
            if (err != NULL) error_throw_owned(env, err);
            return NULL;
        }
        if (list_add(env, list, (void*)item, NULL) < 0) {
            admin_list_free(env, list, NULL);
            return NULL;
        }
    }
    return list;
}

static const void* group_state_item(int32_t ordinal, kafka_common_Error_t** out_err) {
    return group_state_of(ordinal, out_err);
}

static const void* group_type_item(int32_t ordinal, kafka_common_Error_t** out_err) {
    return group_type_of(ordinal, out_err);
}

// An ordinal, or -1 for NULL (Java's null or empty Optional), as a number or `null`.
static napi_value js_ordinal_or_null(napi_env env, int ordinal) {
    return ordinal < 0 ? js_null(env) : js_int32(env, ordinal);
}

// A GroupListing as { groupId, type, protocol, groupState }, the type and the
// state their ordinals, null for Java's empty Optional.
static napi_value item_group_listing(napi_env env, const void* item) {
    const kafka_admin_GroupListing_t* g = (const kafka_admin_GroupListing_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const char *group_id = NULL, *protocol = NULL;
    const kafka_common_GroupType_t* type = NULL;
    const kafka_common_GroupState_t* state = NULL;
    int type_ordinal = -1, state_ordinal = -1;
    kafka_common_Error_t* err = FFI_SET(group_id, kafka_admin_GroupListing_group_id_err(g, &ffi_err));
    if (err == NULL) err = FFI_SET(type, kafka_admin_GroupListing_type_err(g, &ffi_err));
    if (err == NULL && type != NULL) err = FFI_SET(type_ordinal, (int)kafka_common_GroupType__enum_err(type, &ffi_err));
    if (err == NULL) err = FFI_SET(protocol, kafka_admin_GroupListing_protocol_err(g, &ffi_err));
    if (err == NULL) err = FFI_SET(state, kafka_admin_GroupListing_group_state_err(g, &ffi_err));
    if (err == NULL && state != NULL) err = FFI_SET(state_ordinal, (int)kafka_common_GroupState__enum_err(state, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "groupId", js_string_or_null(env, group_id)) < 0 || set(env, obj, "type", js_ordinal_or_null(env, type_ordinal)) < 0
        || set(env, obj, "protocol", js_string_or_null(env, protocol)) < 0
        || set(env, obj, "groupState", js_ordinal_or_null(env, state_ordinal)) < 0)
        return NULL;
    return obj;
}

static napi_value conv_group_listings(napi_env env, void* v) {
    return admin_list_to_js(env, (const kafka_List_t*)v, item_group_listing, 0);
}

static napi_value item_error(napi_env env, const void* item) {
    return error_to_js(env, (const kafka_common_Error_t*)item);
}

// The listing errors: the borrowed errors of the future's list.
static napi_value conv_errors(napi_env env, void* v) {
    return admin_list_to_js(env, (const kafka_List_t*)v, item_error, 0);
}

// A MemberAssignment as its [topic, partition] pairs, `null` for NULL.
static napi_value member_assignment_to_js(napi_env env, const kafka_admin_MemberAssignment_t* a) {
    if (a == NULL) return js_null(env);
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* partitions = NULL;
    kafka_common_Error_t* err = FFI_SET(partitions, kafka_admin_MemberAssignment_topic_partitions_err(a, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return owned_list_to_js(env, partitions, key_topic_partition, 0);
}

// A MemberDescription as { memberId, groupInstanceId, rackId, clientId, host,
// assignment, targetAssignment, memberEpoch, upgraded }, null for each of
// Java's empty Optionals (-1 for the epoch and the flag).
static napi_value item_member_description(napi_env env, const void* item) {
    const kafka_admin_MemberDescription_t* m = (const kafka_admin_MemberDescription_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const char *member_id = NULL, *instance = NULL, *rack = NULL, *client = NULL, *host = NULL;
    const kafka_admin_MemberAssignment_t *assignment = NULL, *target = NULL;
    int32_t epoch = -1;
    int8_t upgraded = -1;
    kafka_common_Error_t* err = FFI_SET(member_id, kafka_admin_MemberDescription_consumer_id_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(instance, kafka_admin_MemberDescription_group_instance_id_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(rack, kafka_admin_MemberDescription_rack_id_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(client, kafka_admin_MemberDescription_client_id_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(host, kafka_admin_MemberDescription_host_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(assignment, kafka_admin_MemberDescription_assignment_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(target, kafka_admin_MemberDescription_target_assignment_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(epoch, kafka_admin_MemberDescription_member_epoch_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(upgraded, kafka_admin_MemberDescription_upgraded_err(m, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "memberId", js_string_or_null(env, member_id)) < 0
        || set(env, obj, "groupInstanceId", js_string_or_null(env, instance)) < 0
        || set(env, obj, "rackId", js_string_or_null(env, rack)) < 0 || set(env, obj, "clientId", js_string_or_null(env, client)) < 0
        || set(env, obj, "host", js_string_or_null(env, host)) < 0
        || set(env, obj, "assignment", member_assignment_to_js(env, assignment)) < 0
        || set(env, obj, "targetAssignment", member_assignment_to_js(env, target)) < 0
        || set(env, obj, "memberEpoch", epoch < 0 ? js_null(env) : js_int32(env, epoch)) < 0
        || set(env, obj, "upgraded", upgraded < 0 ? js_null(env) : js_bool(env, upgraded)) < 0)
        return NULL;
    return obj;
}

// The authorized operations of an OWNED list (NULL: Java's null) as their
// codes or `null`, the list then destroyed.
static napi_value owned_acl_operations_to_js(napi_env env, kafka_List_t* operations) {
    napi_value ops = admin_acl_operations_to_js(env, operations);
    kafka_common_Error_t* ffi_err = NULL;
    if (operations != NULL) error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(operations, &ffi_err)));
    return ops;
}

// A ConsumerGroupDescription as { groupId, isSimpleConsumerGroup, members,
// partitionAssignor, type, groupState, coordinator, authorizedOperations,
// groupEpoch, targetAssignmentEpoch }, the type and state their ordinals (null
// for Java's null), an epoch null for Java's empty Optional (-1).
static napi_value conv_consumer_group_description(napi_env env, void* v) {
    if (v == NULL) return js_null(env);
    const kafka_admin_ConsumerGroupDescription_t* d = (const kafka_admin_ConsumerGroupDescription_t*)v;
    kafka_common_Error_t* ffi_err = NULL;
    const char *group_id = NULL, *assignor = NULL;
    int8_t simple = 0;
    kafka_List_t *members = NULL, *operations = NULL;
    const kafka_common_GroupType_t* type = NULL;
    const kafka_common_GroupState_t* state = NULL;
    const kafka_common_Node_t* coordinator = NULL;
    int type_ordinal = -1, state_ordinal = -1;
    int32_t group_epoch = -1, target_epoch = -1;
    kafka_common_Error_t* err = FFI_SET(group_id, kafka_admin_ConsumerGroupDescription_group_id_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(simple, kafka_admin_ConsumerGroupDescription_is_simple_consumer_group_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(members, kafka_admin_ConsumerGroupDescription_members_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(assignor, kafka_admin_ConsumerGroupDescription_partition_assignor_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(type, kafka_admin_ConsumerGroupDescription_type_err(d, &ffi_err));
    if (err == NULL && type != NULL) err = FFI_SET(type_ordinal, (int)kafka_common_GroupType__enum_err(type, &ffi_err));
    if (err == NULL) err = FFI_SET(state, kafka_admin_ConsumerGroupDescription_group_state_err(d, &ffi_err));
    if (err == NULL && state != NULL) err = FFI_SET(state_ordinal, (int)kafka_common_GroupState__enum_err(state, &ffi_err));
    if (err == NULL) err = FFI_SET(coordinator, kafka_admin_ConsumerGroupDescription_coordinator_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(operations, kafka_admin_ConsumerGroupDescription_authorized_operations_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(group_epoch, kafka_admin_ConsumerGroupDescription_group_epoch_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(target_epoch, kafka_admin_ConsumerGroupDescription_target_assignment_epoch_err(d, &ffi_err));
    // Each owned list converted, then destroyed, whatever happened before.
    napi_value js_members = owned_list_to_js(env, members, item_member_description, 0);
    napi_value ops = owned_acl_operations_to_js(env, operations);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "groupId", js_string_or_null(env, group_id)) < 0 || set(env, obj, "isSimpleConsumerGroup", js_bool(env, simple)) < 0
        || set(env, obj, "members", js_members) < 0 || set(env, obj, "partitionAssignor", js_string_or_null(env, assignor)) < 0
        || set(env, obj, "type", js_ordinal_or_null(env, type_ordinal)) < 0
        || set(env, obj, "groupState", js_ordinal_or_null(env, state_ordinal)) < 0
        || set(env, obj, "coordinator", node_to_js(env, coordinator)) < 0 || set(env, obj, "authorizedOperations", ops) < 0
        || set(env, obj, "groupEpoch", group_epoch < 0 ? js_null(env) : js_int32(env, group_epoch)) < 0
        || set(env, obj, "targetAssignmentEpoch", target_epoch < 0 ? js_null(env) : js_int32(env, target_epoch)) < 0)
        return NULL;
    return obj;
}

// A ClassicGroupDescription as { groupId, protocol, protocolData, members,
// state, coordinator, authorizedOperations }, the state its ordinal (null for
// Java's null).
static napi_value conv_classic_group_description(napi_env env, void* v) {
    if (v == NULL) return js_null(env);
    const kafka_admin_ClassicGroupDescription_t* d = (const kafka_admin_ClassicGroupDescription_t*)v;
    kafka_common_Error_t* ffi_err = NULL;
    const char *group_id = NULL, *protocol = NULL, *protocol_data = NULL;
    kafka_List_t *members = NULL, *operations = NULL;
    const kafka_common_ClassicGroupState_t* state = NULL;
    const kafka_common_Node_t* coordinator = NULL;
    int state_ordinal = -1;
    kafka_common_Error_t* err = FFI_SET(group_id, kafka_admin_ClassicGroupDescription_group_id_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(protocol, kafka_admin_ClassicGroupDescription_protocol_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(protocol_data, kafka_admin_ClassicGroupDescription_protocol_data_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(members, kafka_admin_ClassicGroupDescription_members_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(state, kafka_admin_ClassicGroupDescription_state_err(d, &ffi_err));
    if (err == NULL && state != NULL) err = FFI_SET(state_ordinal, (int)kafka_common_ClassicGroupState__enum_err(state, &ffi_err));
    if (err == NULL) err = FFI_SET(coordinator, kafka_admin_ClassicGroupDescription_coordinator_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(operations, kafka_admin_ClassicGroupDescription_authorized_operations_err(d, &ffi_err));
    napi_value js_members = owned_list_to_js(env, members, item_member_description, 0);
    napi_value ops = owned_acl_operations_to_js(env, operations);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "groupId", js_string_or_null(env, group_id)) < 0 || set(env, obj, "protocol", js_string_or_null(env, protocol)) < 0
        || set(env, obj, "protocolData", js_string_or_null(env, protocol_data)) < 0 || set(env, obj, "members", js_members) < 0
        || set(env, obj, "state", js_ordinal_or_null(env, state_ordinal)) < 0
        || set(env, obj, "coordinator", node_to_js(env, coordinator)) < 0 || set(env, obj, "authorizedOperations", ops) < 0)
        return NULL;
    return obj;
}

// A borrowed OffsetAndMetadata as { offset, metadata, leaderEpoch }, the epoch
// null for Java's empty Optional (-1), `null` for NULL (no committed offset).
static napi_value item_offset_and_metadata(napi_env env, const void* item) {
    if (item == NULL) return js_null(env);
    const kafka_consumer_OffsetAndMetadata_t* o = (const kafka_consumer_OffsetAndMetadata_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    int64_t offset = 0;
    const char* metadata = NULL;
    int32_t epoch = -1;
    kafka_common_Error_t* err = FFI_SET(offset, kafka_consumer_OffsetAndMetadata_offset_err(o, &ffi_err));
    if (err == NULL) err = FFI_SET(metadata, kafka_consumer_OffsetAndMetadata_metadata_err(o, &ffi_err));
    if (err == NULL) err = FFI_SET(epoch, kafka_consumer_OffsetAndMetadata_leader_epoch_err(o, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "offset", js_int64(env, offset)) < 0 || set(env, obj, "metadata", js_string_or_null(env, metadata)) < 0
        || set(env, obj, "leaderEpoch", epoch < 0 ? js_null(env) : js_int32(env, epoch)) < 0)
        return NULL;
    return obj;
}

// A group's offsets: [[[topic, partition], offset fields or null]].
static napi_value conv_group_offsets(napi_env env, void* v) {
    return admin_map_to_js(env, (const kafka_Map_t*)v, key_topic_partition, item_offset_and_metadata);
}

// ---- listGroups -------------------------------------------------------------------------------

#define ADMIN_RPC ListGroups

ADMIN_OPT_TIMEOUT(kafka_admin_ListGroupsOptions_set_timeout_ms_err)
// `inGroupStates` and `withTypes`: the stored set as ordinals, the core's
// singletons in a list the call copies.
static int ADMIN_SETTER_FN(inGroupStates)(napi_env env, void* o, napi_value v, kafka_common_Error_t** out_err) {
    kafka_List_t* list = singleton_list(env, v, group_state_item);
    if (list == NULL) return -1;
    kafka_admin_ListGroupsOptions_in_group_states_err((ADMIN_OPTIONS_T*)o, list, out_err);
    admin_list_free(env, list, NULL);
    return 0;
}
static int ADMIN_SETTER_FN(withTypes)(napi_env env, void* o, napi_value v, kafka_common_Error_t** out_err) {
    kafka_List_t* list = singleton_list(env, v, group_type_item);
    if (list == NULL) return -1;
    kafka_admin_ListGroupsOptions_with_types_err((ADMIN_OPTIONS_T*)o, list, out_err);
    admin_list_free(env, list, NULL);
    return 0;
}
// `withProtocolTypes`: the stored set of strings.
static int ADMIN_SETTER_FN(withProtocolTypes)(napi_env env, void* o, napi_value v, kafka_common_Error_t** out_err) {
    kafka_List_t* list = string_list(env, v);
    if (list == NULL) return -1;
    kafka_admin_ListGroupsOptions_with_protocol_types_err((ADMIN_OPTIONS_T*)o, list, out_err);
    admin_list_free(env, list, free_item);
    return 0;
}
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ListGroupsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListGroupsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(inGroupStates), ADMIN_SETTER(withTypes), ADMIN_SETTER(withProtocolTypes), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ListGroupsResult_destroy_err),
    {ADMIN_ACC_ONE(all, kafka_admin_ListGroupsResult_all_err, conv_group_listings),
     ADMIN_ACC_ONE(valid, kafka_admin_ListGroupsResult_valid_err, conv_group_listings),
     ADMIN_ACC_ONE(errors, kafka_admin_ListGroupsResult_errors_err, conv_errors), ADMIN_ACC_END},
};

// adminListGroups(handle, options): the result's futures are `all`, `valid`
// and `errors`.
napi_value js_admin_list_groups(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 2 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[1]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(r, kafka_admin_Admin_list_groups_with_options_err(a->view, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ListGroupsOptions_desc ListGroupsResult_desc

// ---- describeConsumerGroups -------------------------------------------------------------------

#define ADMIN_RPC DescribeConsumerGroups

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeConsumerGroupsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(includeAuthorizedOperations, kafka_admin_DescribeConsumerGroupsOptions_set_include_authorized_operations_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeConsumerGroupsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeConsumerGroupsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(includeAuthorizedOperations), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeConsumerGroupsResult_destroy_err),
    {ADMIN_ACC_MAP(described_groups, kafka_admin_DescribeConsumerGroupsResult_described_groups_err, key_string,
                   conv_consumer_group_description),
     ADMIN_ACC_END},
};

// adminDescribeConsumerGroups(handle, groupIds, options): the result's futures
// are `described_groups` (a map by group id).
napi_value js_admin_describe_consumer_groups(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* ids = string_list(env, argv[1]);
    if (ids == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, ids, free_item);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_describe_consumer_groups_with_options_err(a->view, ids, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, ids, free_item);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeConsumerGroupsOptions_desc DescribeConsumerGroupsResult_desc

// ---- describeClassicGroups --------------------------------------------------------------------

#define ADMIN_RPC DescribeClassicGroups

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeClassicGroupsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(includeAuthorizedOperations, kafka_admin_DescribeClassicGroupsOptions_set_include_authorized_operations_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeClassicGroupsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeClassicGroupsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(includeAuthorizedOperations), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeClassicGroupsResult_destroy_err),
    {ADMIN_ACC_MAP(described_groups, kafka_admin_DescribeClassicGroupsResult_described_groups_err, key_string,
                   conv_classic_group_description),
     ADMIN_ACC_END},
};

// adminDescribeClassicGroups(handle, groupIds, options): the result's futures
// are `described_groups` (a map by group id).
napi_value js_admin_describe_classic_groups(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* ids = string_list(env, argv[1]);
    if (ids == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, ids, free_item);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_describe_classic_groups_with_options_err(a->view, ids, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, ids, free_item);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeClassicGroupsOptions_desc DescribeClassicGroupsResult_desc

// ---- listConsumerGroupOffsets -----------------------------------------------------------------

#define ADMIN_RPC ListConsumerGroupOffsets

ADMIN_OPT_TIMEOUT(kafka_admin_ListConsumerGroupOffsetsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(requireStable, kafka_admin_ListConsumerGroupOffsetsOptions_set_require_stable_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ListConsumerGroupOffsetsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListConsumerGroupOffsetsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(requireStable), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ListConsumerGroupOffsetsResult_destroy_err),
    {ADMIN_ACC_STR(partitions_to_offset_and_metadata_with_group_id,
                   kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id, conv_group_offsets),
     ADMIN_ACC_END},
};

static void spec_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_ListConsumerGroupOffsetsSpec_destroy_err((kafka_admin_ListConsumerGroupOffsetsSpec_t*)item, out_err);
}

// An array of [groupId, [topic, partition] pairs or null] as an owned map of
// malloc'ed group ids to owned specs (null partitions: every partition of the
// group), or NULL with a JS exception pending.
static kafka_Map_t* group_specs_from_js(napi_env env, napi_value rows) {
    int ok = 0;
    uint32_t n = length_of(env, rows, &ok);
    if (!ok) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value row = element(env, rows, i);
        napi_value g = row == NULL ? NULL : element(env, row, 0);
        napi_value p = g == NULL ? NULL : element(env, row, 1);
        char* group = p == NULL ? NULL : js_string_dup(env, g, "a group id must be a string");
        if (group == NULL) {
            ok = 0;
            break;
        }
        int is_null = 0;
        kafka_List_t* partitions = tp_list_or_null(env, p, &is_null);
        if (partitions == NULL && !is_null) {
            free(group);
            ok = 0;
            break;
        }
        kafka_admin_ListConsumerGroupOffsetsSpec_t* spec = NULL;
        err = FFI_SET(spec, kafka_admin_ListConsumerGroupOffsetsSpec_new_err(&ffi_err));
        if (err == NULL && partitions != NULL)
            err = FFI_ERR(kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions_err(spec, partitions, &ffi_err));
        admin_list_free(env, partitions, tp_item_destroy);
        if (err == NULL) err = FFI_ERR(kafka_Map_put_err(map, group, spec, &ffi_err));
        if (err != NULL) {
            free(group);
            if (spec != NULL) FFI_DISCARD(spec_destroy(spec, &ffi_err));
        }
    }
    if (err != NULL || !ok) {
        map_free_with(env, map, free_item, spec_destroy);
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return map;
}

// adminListConsumerGroupOffsets(handle, groupId, groupSpecs, options): Java's
// (groupId, options) form when groupId is a string (`_with_group_id_options`),
// else its (groupSpecs, options) form, the specs an array of [groupId,
// partitions or null] (`_with_group_specs_options`); each requested group's
// future is `partitions_to_offset_and_metadata_with_group_id`.
napi_value js_admin_list_consumer_group_offsets(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    char* group = NULL;
    kafka_Map_t* specs = NULL;
    if (type == napi_string) {
        group = js_string_dup(env, argv[1], "groupId must be a string");
        if (group == NULL) return NULL;
    } else if ((specs = group_specs_from_js(env, argv[2])) == NULL) {
        return NULL;
    }
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[3]);
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = NULL;
    if (o != NULL) {
        ADMIN_OPTIONS_T* opts = (ADMIN_OPTIONS_T*)admin_opts(o);
        err = group != NULL
                  ? FFI_SET(r, kafka_admin_Admin_list_consumer_group_offsets_with_group_id_options_err(a->view, group, opts, &ffi_err))
                  : FFI_SET(r, kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options_err(a->view, specs, opts, &ffi_err));
    }
    free(group);
    map_free_with(env, specs, free_item, spec_destroy);
    if (o == NULL) return NULL;
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ListConsumerGroupOffsetsOptions_desc ListConsumerGroupOffsetsResult_desc

// ---- alterConsumerGroupOffsets ----------------------------------------------------------------

#define ADMIN_RPC AlterConsumerGroupOffsets

ADMIN_OPT_TIMEOUT(kafka_admin_AlterConsumerGroupOffsetsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_AlterConsumerGroupOffsetsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_AlterConsumerGroupOffsetsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};

// `partition_result(partition)`, the key a [topic, partition] pair.
static kafka_common_KafkaFuture_t* alter_offsets_partition_result(napi_env env, const void* result, napi_value key,
                                                                  kafka_common_Error_t** out_err) {
    kafka_common_TopicPartition_t* tp = tp_from_js(env, key);
    if (tp == NULL) return NULL;
    kafka_common_KafkaFuture_t* future = kafka_admin_AlterConsumerGroupOffsetsResult_partition_result_err((const ADMIN_RESULT_T*)result, tp, out_err);
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 1, FFI_ERR(kafka_common_TopicPartition_destroy_err(tp, &ffi_err)));
    return future;
}

static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_AlterConsumerGroupOffsetsResult_destroy_err),
    {ADMIN_ACC_JS(partition_result, alter_offsets_partition_result, NULL),
     ADMIN_ACC_ONE(all, kafka_admin_AlterConsumerGroupOffsetsResult_all_err, NULL), ADMIN_ACC_END},
};

static void offset_and_metadata_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_consumer_OffsetAndMetadata_destroy_err((kafka_consumer_OffsetAndMetadata_t*)item, out_err);
}

// adminAlterConsumerGroupOffsets(handle, groupId, offsets, options): the
// offsets an array of [[topic, partition], offset, leaderEpoch (-1: none),
// metadata]; the result's futures are `partition_result(partition)` and `all`.
napi_value js_admin_alter_consumer_group_offsets(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    int ok = 0;
    uint32_t n = length_of(env, argv[2], &ok);
    if (!ok) return NULL;
    char* group = js_string_dup(env, argv[1], "groupId must be a string");
    if (group == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && ok && i < n; i++) {
        napi_value row = element(env, argv[2], i);
        napi_value k = row == NULL ? NULL : element(env, row, 0);
        napi_value off = k == NULL ? NULL : element(env, row, 1);
        napi_value ep = off == NULL ? NULL : element(env, row, 2);
        napi_value md = ep == NULL ? NULL : element(env, row, 3);
        int64_t offset = 0;
        bool lossless = false;
        if (md == NULL || napi_get_value_bigint_int64(env, off, &offset, &lossless) != napi_ok) {
            if (md != NULL) napi_throw_type_error(env, NULL, "an offset must be a bigint");
            ok = 0;
            break;
        }
        int32_t epoch = int32_of(env, ep, "a leader epoch", &ok);
        char* metadata = ok ? js_string_dup(env, md, "metadata must be a string") : NULL;
        if (metadata == NULL) {
            ok = 0;
            break;
        }
        kafka_common_TopicPartition_t* tp = tp_from_js(env, k);
        if (tp == NULL) {
            free(metadata);
            ok = 0;
            break;
        }
        kafka_consumer_OffsetAndMetadata_t* value = NULL;
        err = FFI_SET(value, kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(offset, epoch, metadata, &ffi_err));
        free(metadata);
        if (err == NULL) err = FFI_ERR(kafka_Map_put_err(map, tp, value, &ffi_err));
        if (err != NULL) {
            FFI_DISCARD(kafka_common_TopicPartition_destroy_err(tp, &ffi_err));
            if (value != NULL) FFI_DISCARD(kafka_consumer_OffsetAndMetadata_destroy_err(value, &ffi_err));
        }
    }
    void* o = err == NULL && ok ? admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[3]) : NULL;
    ADMIN_RESULT_T* r = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_alter_consumer_group_offsets_with_options_err(a->view, group, map, (ADMIN_OPTIONS_T*)admin_opts(o),
                                                                                        &ffi_err));
    free(group);
    // The map's keys and offsets are copied by the call: freed here.
    map_free_with(env, map, tp_item_destroy, offset_and_metadata_destroy);
    if (o == NULL) {
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison AlterConsumerGroupOffsetsOptions_desc AlterConsumerGroupOffsetsResult_desc

// ---- deleteConsumerGroupOffsets ---------------------------------------------------------------

#define ADMIN_RPC DeleteConsumerGroupOffsets

ADMIN_OPT_TIMEOUT(kafka_admin_DeleteConsumerGroupOffsetsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DeleteConsumerGroupOffsetsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DeleteConsumerGroupOffsetsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};

// `partition_result(partition)`, the key a [topic, partition] pair.
static kafka_common_KafkaFuture_t* delete_offsets_partition_result(napi_env env, const void* result, napi_value key,
                                                                   kafka_common_Error_t** out_err) {
    kafka_common_TopicPartition_t* tp = tp_from_js(env, key);
    if (tp == NULL) return NULL;
    kafka_common_KafkaFuture_t* future = kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result((const ADMIN_RESULT_T*)result, tp, out_err);
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 1, FFI_ERR(kafka_common_TopicPartition_destroy_err(tp, &ffi_err)));
    return future;
}

static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DeleteConsumerGroupOffsetsResult_destroy_err),
    {ADMIN_ACC_JS(partition_result, delete_offsets_partition_result, NULL),
     ADMIN_ACC_ONE(all, kafka_admin_DeleteConsumerGroupOffsetsResult_all_err, NULL), ADMIN_ACC_END},
};

// adminDeleteConsumerGroupOffsets(handle, groupId, partitions, options): the
// partitions as [topic, partition] pairs; the result's futures are
// `partition_result(partition)` and `all`.
napi_value js_admin_delete_consumer_group_offsets(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    char* group = js_string_dup(env, argv[1], "groupId must be a string");
    if (group == NULL) return NULL;
    kafka_List_t* partitions = handle_list(env, argv[2], tp_from_js_item, tp_item_destroy);
    if (partitions == NULL) {
        free(group);
        return NULL;
    }
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[3]);
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_delete_consumer_group_offsets_with_options_err(a->view, group, partitions,
                                                                                         (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    free(group);
    admin_list_free(env, partitions, tp_item_destroy);
    if (o == NULL) return NULL;
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DeleteConsumerGroupOffsetsOptions_desc DeleteConsumerGroupOffsetsResult_desc

// ---- deleteConsumerGroups ---------------------------------------------------------------------

#define ADMIN_RPC DeleteConsumerGroups

ADMIN_OPT_TIMEOUT(kafka_admin_DeleteConsumerGroupsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DeleteConsumerGroupsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DeleteConsumerGroupsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DeleteConsumerGroupsResult_destroy_err),
    {ADMIN_ACC_MAP(deleted_groups, kafka_admin_DeleteConsumerGroupsResult_deleted_groups_err, key_string, NULL), ADMIN_ACC_END},
};

// adminDeleteConsumerGroups(handle, groupIds, options): the result's futures
// are `deleted_groups` (a map by group id).
napi_value js_admin_delete_consumer_groups(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* ids = string_list(env, argv[1]);
    if (ids == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, ids, free_item);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_delete_consumer_groups_with_options_err(a->view, ids, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, ids, free_item);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DeleteConsumerGroupsOptions_desc DeleteConsumerGroupsResult_desc

// ---- removeMembersFromConsumerGroup -----------------------------------------------------------

#define ADMIN_RPC RemoveMembersFromConsumerGroup

// The members Java's `(members)` constructor takes, set by the entry point
// for the descriptor's create, which takes no argument, during the one call.
// Each worker thread is a JS thread of its own and the addon is loaded once
// per process, so the pointer is per thread: a call on one thread never reads
// another's members.
static _Thread_local const kafka_List_t* remove_members_pending = NULL;

// Java's `new RemoveMembersFromConsumerGroupOptions(members)`: the FFI has no
// setter of the members, a final field of Java's, only that constructor.
static ADMIN_OPTIONS_T* remove_members_options_with_members(kafka_common_Error_t** out_err) {
    return kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(remove_members_pending, out_err);
}

ADMIN_OPT_TIMEOUT(kafka_admin_RemoveMembersFromConsumerGroupOptions_set_timeout_ms_err)
// `reason(String)`: a string, or null to clear it.
static int ADMIN_SETTER_FN(reason)(napi_env env, void* o, napi_value v, kafka_common_Error_t** out_err) {
    int ok = 0;
    char* reason = string_or_null(env, v, "reason must be a string or null", &ok);
    if (!ok) return -1;
    kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason_err((ADMIN_OPTIONS_T*)o, reason, out_err);
    free(reason);
    return 0;
}
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_RemoveMembersFromConsumerGroupOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(reason), {NULL, NULL}},
};
// The same options, from Java's `(members)` constructor.
static const AdminOptionsDesc RemoveMembersFromConsumerGroupWithMembersOptions_desc = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(remove_members_options_with_members),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(reason), {NULL, NULL}},
};

static void member_to_remove_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_MemberToRemove_destroy_err((kafka_admin_MemberToRemove_t*)item, out_err);
}

// A JS group instance id as an owned MemberToRemove, or NULL with a JS
// exception pending.
static kafka_admin_MemberToRemove_t* member_from_js(napi_env env, napi_value id) {
    char* s = js_string_dup(env, id, "a member's group instance id must be a string");
    if (s == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_MemberToRemove_t* m = NULL;
    kafka_common_Error_t* err = FFI_SET(m, kafka_admin_MemberToRemove_new_err(s, &ffi_err));
    free(s);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return m;
}

static void* member_from_js_item(napi_env env, napi_value v) {
    return member_from_js(env, v);
}

// `member_result(member)`, the key the member's group instance id.
static kafka_common_KafkaFuture_t* remove_members_member_result(napi_env env, const void* result, napi_value key,
                                                                kafka_common_Error_t** out_err) {
    kafka_admin_MemberToRemove_t* m = member_from_js(env, key);
    if (m == NULL) return NULL;
    kafka_common_KafkaFuture_t* future = kafka_admin_RemoveMembersFromConsumerGroupResult_member_result((const ADMIN_RESULT_T*)result, m, out_err);
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 1, FFI_ERR(kafka_admin_MemberToRemove_destroy_err(m, &ffi_err)));
    return future;
}

static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_RemoveMembersFromConsumerGroupResult_destroy_err),
    {ADMIN_ACC_JS(member_result, remove_members_member_result, NULL),
     ADMIN_ACC_ONE(all, kafka_admin_RemoveMembersFromConsumerGroupResult_all_err, NULL), ADMIN_ACC_END},
};

// adminRemoveMembersFromConsumerGroup(handle, groupId, members, options): the
// members their group instance ids, or null for Java's `()` constructor
// (removeAll mode), the options built with Java's `(members)` constructor
// otherwise; the result's futures are `member_result(member)` and `all`.
napi_value js_admin_remove_members_from_consumer_group(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 4 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
    kafka_List_t* members = NULL;
    if (type != napi_null && (members = handle_list(env, argv[2], member_from_js_item, member_to_remove_destroy)) == NULL) return NULL;
    char* group = js_string_dup(env, argv[1], "groupId must be a string");
    if (group == NULL) {
        admin_list_free(env, members, member_to_remove_destroy);
        return NULL;
    }
    const AdminOptionsDesc* odesc = members != NULL ? &RemoveMembersFromConsumerGroupWithMembersOptions_desc : &ADMIN_OPTIONS_DESC;
    remove_members_pending = members;
    void* o = admin_options_build(env, odesc, argv[3]);
    remove_members_pending = NULL;
    admin_list_free(env, members, member_to_remove_destroy);
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_remove_members_from_consumer_group_with_options_err(a->view, group, (ADMIN_OPTIONS_T*)admin_opts(o),
                                                                                             &ffi_err));
    free(group);
    if (o == NULL) return NULL;
    return admin_rpc_return(env, odesc, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison RemoveMembersFromConsumerGroupOptions_desc RemoveMembersFromConsumerGroupWithMembersOptions_desc \
    RemoveMembersFromConsumerGroupResult_desc

// ---- the mock's group offsets -----------------------------------------------------------------

// mockAdminUpdateConsumerGroupOffsets(handle, newOffsets): Java's
// MockAdminClient.updateConsumerGroupOffsets, the offsets an array of
// [[topic, partition], offset].
napi_value js_mock_admin_update_consumer_group_offsets(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 2 ? admin_mock_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_Map_t* map = offsets_map_from_js(env, argv[1]);
    if (map == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_admin_MockAdminClient_update_consumer_group_offsets_err(a->mock, map, &ffi_err));
    // The map is copied by the call: its keys and offsets are freed here.
    map_free_with(env, map, tp_item_destroy, NULL);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// ---- security: ACLs: shared values ----------------------------------------------------------

// An ACL value crosses as Python's seven-tuple
// [resourceTypeCode, name, patternTypeCode, principal, host, operationCode,
// permissionTypeCode], each enum as Java's byte code; a filter's name,
// principal and host may be null.

static void acl_binding_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_acl_AclBinding_destroy_err((kafka_common_acl_AclBinding_t*)item, out_err);
}

static void acl_binding_filter_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_acl_AclBindingFilter_destroy_err((kafka_common_acl_AclBindingFilter_t*)item, out_err);
}

// The seven fields of a JS tuple: the codes in codes[0..3] (resource type,
// pattern type, operation, permission type), the strings malloc'ed (NULL for
// null) in strs[0..2] (name, principal, host); 0, or -1 with a JS exception
// pending (nothing left to free).
static int acl_fields_from_js(napi_env env, napi_value tuple, int32_t codes[4], char* strs[3]) {
    static const uint32_t code_at[4] = {0, 2, 5, 6};
    static const uint32_t str_at[3] = {1, 3, 4};
    static const char* const what[3] = {"an ACL's resource name must be a string or null", "an ACL's principal must be a string or null",
                                         "an ACL's host must be a string or null"};
    int ok = 1;
    for (int i = 0; i < 4 && ok; i++) {
        napi_value v = element(env, tuple, code_at[i]);
        if (v == NULL) return -1;
        codes[i] = int32_of(env, v, "an ACL's code", &ok);
    }
    if (!ok) return -1;
    for (int i = 0; i < 3; i++) strs[i] = NULL;
    for (int i = 0; i < 3; i++) {
        napi_value v = element(env, tuple, str_at[i]);
        if (v != NULL) strs[i] = string_or_null(env, v, what[i], &ok);
        if (v == NULL || !ok) {
            for (int j = 0; j < i; j++) free(strs[j]);
            return -1;
        }
    }
    return 0;
}

// The enum singletons of four codes, through each enum's fromCode (UNKNOWN for
// a code Java does not know).
typedef struct {
    const kafka_common_resource_ResourceType_t* resource_type;
    const kafka_common_resource_PatternType_t* pattern_type;
    const kafka_common_acl_AclOperation_t* operation;
    const kafka_common_acl_AclPermissionType_t* permission_type;
} AclEnums;

static kafka_common_Error_t* acl_enums_of(const int32_t codes[4], AclEnums* out) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_SET(out->resource_type, kafka_common_resource_ResourceType_from_code_err((int8_t)codes[0], &ffi_err));
    if (err == NULL) err = FFI_SET(out->pattern_type, kafka_common_resource_PatternType_from_code_err((int8_t)codes[1], &ffi_err));
    if (err == NULL) err = FFI_SET(out->operation, kafka_common_acl_AclOperation_from_code_err((int8_t)codes[2], &ffi_err));
    if (err == NULL)
        err = FFI_SET(out->permission_type, kafka_common_acl_AclPermissionType_from_code_err((int8_t)codes[3], &ffi_err));
    return err;
}

// A JS tuple as an owned AclBinding, or NULL with a JS exception pending.
static void* acl_binding_from_js(napi_env env, napi_value tuple) {
    int32_t codes[4];
    char* strs[3];
    if (acl_fields_from_js(env, tuple, codes, strs) < 0) return NULL;
    kafka_common_acl_AclBinding_t* binding = NULL;
    kafka_common_Error_t* err = NULL;
    if (strs[0] == NULL || strs[1] == NULL || strs[2] == NULL) {
        napi_throw_type_error(env, NULL, "an AclBinding's name, principal and host must be strings");
    } else {
        AclEnums e;
        kafka_common_Error_t* ffi_err = NULL;
        kafka_common_resource_ResourcePattern_t* pattern = NULL;
        kafka_common_acl_AccessControlEntry_t* entry = NULL;
        err = acl_enums_of(codes, &e);
        if (err == NULL) err = FFI_SET(pattern, kafka_common_resource_ResourcePattern_new(e.resource_type, strs[0], e.pattern_type, &ffi_err));
        if (err == NULL)
            err = FFI_SET(entry, kafka_common_acl_AccessControlEntry_new(strs[1], strs[2], e.operation, e.permission_type, &ffi_err));
        if (err == NULL) err = FFI_SET(binding, kafka_common_acl_AclBinding_new_err(pattern, entry, &ffi_err));
        // The binding holds copies.
        if (pattern != NULL) error_warn(env, 1, FFI_ERR(kafka_common_resource_ResourcePattern_destroy_err(pattern, &ffi_err)));
        if (entry != NULL) error_warn(env, 1, FFI_ERR(kafka_common_acl_AccessControlEntry_destroy_err(entry, &ffi_err)));
    }
    for (int i = 0; i < 3; i++) free(strs[i]);
    if (err != NULL) error_throw_owned(env, err);
    return binding;
}

// A JS tuple as an owned AclBindingFilter, or NULL with a JS exception pending.
static kafka_common_acl_AclBindingFilter_t* acl_binding_filter_from_js(napi_env env, napi_value tuple) {
    int32_t codes[4];
    char* strs[3];
    if (acl_fields_from_js(env, tuple, codes, strs) < 0) return NULL;
    AclEnums e;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_resource_ResourcePatternFilter_t* pattern = NULL;
    kafka_common_acl_AccessControlEntryFilter_t* entry = NULL;
    kafka_common_acl_AclBindingFilter_t* filter = NULL;
    kafka_common_Error_t* err = acl_enums_of(codes, &e);
    if (err == NULL)
        err = FFI_SET(pattern, kafka_common_resource_ResourcePatternFilter_new_err(e.resource_type, strs[0], e.pattern_type, &ffi_err));
    if (err == NULL)
        err = FFI_SET(entry, kafka_common_acl_AccessControlEntryFilter_new_err(strs[1], strs[2], e.operation, e.permission_type, &ffi_err));
    if (err == NULL) err = FFI_SET(filter, kafka_common_acl_AclBindingFilter_new_err(pattern, entry, &ffi_err));
    // The filter holds copies.
    if (pattern != NULL) error_warn(env, 1, FFI_ERR(kafka_common_resource_ResourcePatternFilter_destroy_err(pattern, &ffi_err)));
    if (entry != NULL) error_warn(env, 1, FFI_ERR(kafka_common_acl_AccessControlEntryFilter_destroy_err(entry, &ffi_err)));
    for (int i = 0; i < 3; i++) free(strs[i]);
    if (err != NULL) error_throw_owned(env, err);
    return filter;
}

static void* acl_binding_filter_from_js_item(napi_env env, napi_value v) {
    return acl_binding_filter_from_js(env, v);
}

// Four borrowed enum singletons and three borrowed strings (NULL as null) as a
// JS seven-tuple.
static napi_value acl_tuple_to_js(napi_env env, const AclEnums* e, const char* name, const char* principal, const char* host) {
    kafka_common_Error_t* ffi_err = NULL;
    int8_t codes[4] = {0, 0, 0, 0};
    kafka_common_Error_t* err = FFI_SET(codes[0], kafka_common_resource_ResourceType_code_err(e->resource_type, &ffi_err));
    if (err == NULL) err = FFI_SET(codes[1], kafka_common_resource_PatternType_code_err(e->pattern_type, &ffi_err));
    if (err == NULL) err = FFI_SET(codes[2], kafka_common_acl_AclOperation_code_err(e->operation, &ffi_err));
    if (err == NULL) err = FFI_SET(codes[3], kafka_common_acl_AclPermissionType_code_err(e->permission_type, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value vals[7] = {js_int32(env, codes[0]),         js_string_or_null(env, name), js_int32(env, codes[1]),
                          js_string_or_null(env, principal), js_string_or_null(env, host), js_int32(env, codes[2]),
                          js_int32(env, codes[3])};
    napi_value tuple;
    NAPI_CHECK(env, napi_create_array_with_length(env, 7, &tuple), NULL);
    for (uint32_t i = 0; i < 7; i++) {
        if (vals[i] == NULL) return NULL;
        NAPI_CHECK(env, napi_set_element(env, tuple, i, vals[i]), NULL);
    }
    return tuple;
}

// A borrowed AclBinding as its seven-tuple.
static napi_value acl_binding_to_js(napi_env env, const kafka_common_acl_AclBinding_t* b) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_resource_ResourcePattern_t* pattern = NULL;
    const kafka_common_acl_AccessControlEntry_t* entry = NULL;
    const char *name = NULL, *principal = NULL, *host = NULL;
    AclEnums e = {NULL, NULL, NULL, NULL};
    kafka_common_Error_t* err = FFI_SET(pattern, kafka_common_acl_AclBinding_pattern_err(b, &ffi_err));
    if (err == NULL) err = FFI_SET(entry, kafka_common_acl_AclBinding_entry_err(b, &ffi_err));
    if (err == NULL) err = FFI_SET(e.resource_type, kafka_common_resource_ResourcePattern_resource_type_err(pattern, &ffi_err));
    if (err == NULL) err = FFI_SET(name, kafka_common_resource_ResourcePattern_name_err(pattern, &ffi_err));
    if (err == NULL) err = FFI_SET(e.pattern_type, kafka_common_resource_ResourcePattern_pattern_type_err(pattern, &ffi_err));
    if (err == NULL) err = FFI_SET(principal, kafka_common_acl_AccessControlEntry_principal_err(entry, &ffi_err));
    if (err == NULL) err = FFI_SET(host, kafka_common_acl_AccessControlEntry_host_err(entry, &ffi_err));
    if (err == NULL) err = FFI_SET(e.operation, kafka_common_acl_AccessControlEntry_operation_err(entry, &ffi_err));
    if (err == NULL) err = FFI_SET(e.permission_type, kafka_common_acl_AccessControlEntry_permission_type_err(entry, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return acl_tuple_to_js(env, &e, name, principal, host);
}

// A borrowed AclBindingFilter as its seven-tuple, its null fields null.
static napi_value acl_binding_filter_to_js(napi_env env, const kafka_common_acl_AclBindingFilter_t* f) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_resource_ResourcePatternFilter_t* pattern = NULL;
    const kafka_common_acl_AccessControlEntryFilter_t* entry = NULL;
    const char *name = NULL, *principal = NULL, *host = NULL;
    AclEnums e = {NULL, NULL, NULL, NULL};
    kafka_common_Error_t* err = FFI_SET(pattern, kafka_common_acl_AclBindingFilter_pattern_filter_err(f, &ffi_err));
    if (err == NULL) err = FFI_SET(entry, kafka_common_acl_AclBindingFilter_entry_filter_err(f, &ffi_err));
    if (err == NULL) err = FFI_SET(e.resource_type, kafka_common_resource_ResourcePatternFilter_resource_type_err(pattern, &ffi_err));
    if (err == NULL) err = FFI_SET(name, kafka_common_resource_ResourcePatternFilter_name_err(pattern, &ffi_err));
    if (err == NULL) err = FFI_SET(e.pattern_type, kafka_common_resource_ResourcePatternFilter_pattern_type_err(pattern, &ffi_err));
    if (err == NULL) err = FFI_SET(principal, kafka_common_acl_AccessControlEntryFilter_principal_err(entry, &ffi_err));
    if (err == NULL) err = FFI_SET(host, kafka_common_acl_AccessControlEntryFilter_host_err(entry, &ffi_err));
    if (err == NULL) err = FFI_SET(e.operation, kafka_common_acl_AccessControlEntryFilter_operation_err(entry, &ffi_err));
    if (err == NULL) err = FFI_SET(e.permission_type, kafka_common_acl_AccessControlEntryFilter_permission_type_err(entry, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return acl_tuple_to_js(env, &e, name, principal, host);
}

static napi_value key_acl_binding(napi_env env, const void* k) {
    return acl_binding_to_js(env, (const kafka_common_acl_AclBinding_t*)k);
}

static napi_value item_acl_binding(napi_env env, const void* item) {
    return acl_binding_to_js(env, (const kafka_common_acl_AclBinding_t*)item);
}

static napi_value key_acl_binding_filter(napi_env env, const void* k) {
    return acl_binding_filter_to_js(env, (const kafka_common_acl_AclBindingFilter_t*)k);
}

// describeAcls' value: the future's list of bindings, borrowed.
static napi_value conv_acl_bindings(napi_env env, void* v) {
    return admin_list_to_js(env, (const kafka_List_t*)v, item_acl_binding, 0);
}

// A FilterResult as { binding, error }: the binding's tuple, or null when the
// deletion failed; the error, or null when it succeeded (both borrowed).
static napi_value item_filter_result(napi_env env, const void* item) {
    const kafka_admin_DeleteAclsResult_FilterResult_t* r = (const kafka_admin_DeleteAclsResult_FilterResult_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_acl_AclBinding_t* binding = NULL;
    const kafka_common_Error_t* error = NULL;
    kafka_common_Error_t* err = FFI_SET(binding, kafka_admin_DeleteAclsResult_FilterResult_binding_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(error, kafka_admin_DeleteAclsResult_FilterResult_error_err(r, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value b = binding == NULL ? js_null(env) : acl_binding_to_js(env, binding);
    if (b == NULL) return NULL;
    napi_value e = error == NULL ? js_null(env) : error_to_js(env, error);
    if (e == NULL) return NULL;
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "binding", b), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "error", e), NULL);
    return obj;
}

// deleteAcls' value: a borrowed FilterResults as an array of { binding,
// error }, read from the owned list of copies its values() hands out, which
// is destroyed with its elements.
static napi_value conv_filter_results(napi_env env, void* v) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err =
        FFI_SET(list, kafka_admin_DeleteAclsResult_FilterResults_values_err((const kafka_admin_DeleteAclsResult_FilterResults_t*)v, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value out = admin_list_to_js(env, list, item_filter_result, 0);
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    return out;
}

// ---- createAcls -------------------------------------------------------------------------------

#define ADMIN_RPC CreateAcls

ADMIN_OPT_TIMEOUT(kafka_admin_CreateAclsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_CreateAclsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_CreateAclsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_CreateAclsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_CreateAclsResult_values_err, key_acl_binding, NULL), ADMIN_ACC_END},
};

// adminCreateAcls(handle, acls, options): each binding as its seven-tuple;
// the result's futures are `values`, a map keyed by the binding's tuple
// (Java's all() is composed in TypeScript).
napi_value js_admin_create_acls(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* acls = handle_list(env, argv[1], acl_binding_from_js, acl_binding_destroy);
    if (acls == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, acls, acl_binding_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_create_acls_with_options_err(a->view, acls, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, acls, acl_binding_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison CreateAclsOptions_desc CreateAclsResult_desc

// ---- describeAcls -----------------------------------------------------------------------------

#define ADMIN_RPC DescribeAcls

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeAclsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeAclsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeAclsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeAclsResult_destroy_err),
    {ADMIN_ACC_ONE_BORROWED(values, kafka_admin_DescribeAclsResult_values_err, conv_acl_bindings), ADMIN_ACC_END},
};

// adminDescribeAcls(handle, filter, options): the filter as its seven-tuple;
// the result's one future is `values`, which the result lends out.
napi_value js_admin_describe_acls(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_common_acl_AclBindingFilter_t* filter = acl_binding_filter_from_js(env, argv[1]);
    if (filter == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_describe_acls_with_options_err(a->view, filter, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    error_warn(env, 1, FFI_ERR(kafka_common_acl_AclBindingFilter_destroy_err(filter, &ffi_err)));
    if (o == NULL) return NULL;
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeAclsOptions_desc DescribeAclsResult_desc

// ---- deleteAcls -------------------------------------------------------------------------------

#define ADMIN_RPC DeleteAcls

ADMIN_OPT_TIMEOUT(kafka_admin_DeleteAclsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DeleteAclsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DeleteAclsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DeleteAclsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_DeleteAclsResult_values_err, key_acl_binding_filter, conv_filter_results), ADMIN_ACC_END},
};

// adminDeleteAcls(handle, filters, options): each filter as its seven-tuple;
// the result's futures are `values`, a map keyed by the filter's tuple, each
// an array of { binding, error } (Java's all() is composed in TypeScript).
napi_value js_admin_delete_acls(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* filters = handle_list(env, argv[1], acl_binding_filter_from_js_item, acl_binding_filter_destroy);
    if (filters == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, filters, acl_binding_filter_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_delete_acls_with_options_err(a->view, filters, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, filters, acl_binding_filter_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DeleteAclsOptions_desc DeleteAclsResult_desc

// ---- security: client quotas: shared values -------------------------------------------------

// An entity crosses as its entries, [[entityType, name | null], ...]; an
// alteration as [entityEntries, [[key, value | null], ...]], a null value a
// removal (Java's null Double, which the core reads from NaN); a filter as
// [strict, [[entityType, matchKind, name | null], ...]], matchKind 0 for an
// exact name, 1 for the default name and 2 for any name.

static void quota_alteration_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_quota_ClientQuotaAlteration_destroy_err((kafka_common_quota_ClientQuotaAlteration_t*)item, out_err);
}

static void quota_op_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_quota_ClientQuotaAlteration_Op_destroy_err((kafka_common_quota_ClientQuotaAlteration_Op_t*)item, out_err);
}

static void quota_component_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_quota_ClientQuotaFilterComponent_destroy_err((kafka_common_quota_ClientQuotaFilterComponent_t*)item, out_err);
}

// An entity's entries as an owned entity, or NULL with a JS exception pending.
static kafka_common_quota_ClientQuotaEntity_t* quota_entity_from_js(napi_env env, napi_value pairs) {
    int is_null = 0;
    kafka_Map_t* map = string_map(env, pairs, &is_null);
    if (map == NULL) {
        if (is_null) napi_throw_type_error(env, NULL, "a quota entity's entries must not be null");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_quota_ClientQuotaEntity_t* entity = NULL;
    kafka_common_Error_t* err = FFI_SET(entity, kafka_common_quota_ClientQuotaEntity_new_err(map, &ffi_err));  // copies
    admin_map_free(env, map);
    if (err != NULL) error_throw_owned(env, err);
    return entity;
}

// An alteration's ops as an owned list of owned ops, or NULL with a JS
// exception pending.
static kafka_List_t* quota_ops_from_js(napi_env env, napi_value ops) {
    int ok = 0;
    uint32_t n = length_of(env, ops, &ok);
    if (!ok) return NULL;
    kafka_List_t* list = list_new(env);
    if (list == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value row = element(env, ops, i);
        napi_value k = row == NULL ? NULL : element(env, row, 0);
        napi_value v = row == NULL ? NULL : element(env, row, 1);
        char* key = k == NULL ? NULL : js_string_dup(env, k, "a quota op's key must be a string");
        napi_valuetype type = napi_undefined;
        double value = NAN;
        if (key == NULL || v == NULL || napi_typeof(env, v, &type) != napi_ok
            || (type != napi_null && napi_get_value_double(env, v, &value) != napi_ok)) {
            bool pending = false;
            if (key != NULL && napi_is_exception_pending(env, &pending) == napi_ok && !pending) {
                napi_throw_type_error(env, NULL, "a quota op's value must be a number or null");
            }
            free(key);
            admin_list_free(env, list, quota_op_destroy);
            return NULL;
        }
        kafka_common_quota_ClientQuotaAlteration_Op_t* op = NULL;
        kafka_common_Error_t* err = FFI_SET(op, kafka_common_quota_ClientQuotaAlteration_Op_new_err(key, value, &ffi_err));
        free(key);
        if (err != NULL) {
            admin_list_free(env, list, quota_op_destroy);
            error_throw_owned(env, err);
            return NULL;
        }
        if (list_add(env, list, op, quota_op_destroy) < 0) {
            admin_list_free(env, list, quota_op_destroy);
            return NULL;
        }
    }
    return list;
}

// An alteration as an owned ClientQuotaAlteration, or NULL with a JS
// exception pending.
static void* quota_alteration_from_js(napi_env env, napi_value row) {
    napi_value pairs = element(env, row, 0);
    napi_value ops_value = pairs == NULL ? NULL : element(env, row, 1);
    if (ops_value == NULL) return NULL;
    kafka_common_quota_ClientQuotaEntity_t* entity = quota_entity_from_js(env, pairs);
    if (entity == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* ops = quota_ops_from_js(env, ops_value);
    if (ops == NULL) {
        error_warn(env, 1, FFI_ERR(kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err)));
        return NULL;
    }
    kafka_common_quota_ClientQuotaAlteration_t* alteration = NULL;
    kafka_common_Error_t* err = FFI_SET(alteration, kafka_common_quota_ClientQuotaAlteration_new_err(entity, ops, &ffi_err));  // copies
    admin_list_free(env, ops, quota_op_destroy);
    error_warn(env, 1, FFI_ERR(kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err)));
    if (err != NULL) error_throw_owned(env, err);
    return alteration;
}

// A filter as an owned ClientQuotaFilter, or NULL with a JS exception pending.
static kafka_common_quota_ClientQuotaFilter_t* quota_filter_from_js(napi_env env, napi_value spec) {
    napi_value strict_value = element(env, spec, 0);
    napi_value rows = strict_value == NULL ? NULL : element(env, spec, 1);
    if (rows == NULL) return NULL;
    bool strict = false;
    if (napi_get_value_bool(env, strict_value, &strict) != napi_ok) {
        napi_throw_type_error(env, NULL, "a quota filter's strictness must be a boolean");
        return NULL;
    }
    int ok = 0;
    uint32_t n = length_of(env, rows, &ok);
    if (!ok) return NULL;
    kafka_List_t* list = list_new(env);
    if (list == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    for (uint32_t i = 0; i < n; i++) {
        napi_value row = element(env, rows, i);
        napi_value t = row == NULL ? NULL : element(env, row, 0);
        napi_value m = t == NULL ? NULL : element(env, row, 1);
        napi_value nm = m == NULL ? NULL : element(env, row, 2);
        char* type = nm == NULL ? NULL : js_string_dup(env, t, "a quota filter component's entity type must be a string");
        int32_t kind = type == NULL ? 0 : int32_of(env, m, "a quota filter component's match kind", &ok);
        char* name = (type == NULL || !ok) ? NULL : string_or_null(env, nm, "a quota filter component's name must be a string", &ok);
        if (type == NULL || !ok || (kind == 0 && name == NULL) || kind < 0 || kind > 2) {
            bool pending = false;
            if (napi_is_exception_pending(env, &pending) == napi_ok && !pending) {
                napi_throw_type_error(env, NULL, "a quota filter component must be [entityType, matchKind, name]");
            }
            free(type);
            free(name);
            admin_list_free(env, list, quota_component_destroy);
            return NULL;
        }
        kafka_common_quota_ClientQuotaFilterComponent_t* c = NULL;
        kafka_common_Error_t* err =
            kind == 0   ? FFI_SET(c, kafka_common_quota_ClientQuotaFilterComponent_of_entity_err(type, name, &ffi_err))
            : kind == 1 ? FFI_SET(c, kafka_common_quota_ClientQuotaFilterComponent_of_default_entity_err(type, &ffi_err))
                        : FFI_SET(c, kafka_common_quota_ClientQuotaFilterComponent_of_entity_type_err(type, &ffi_err));
        free(type);
        free(name);
        if (err != NULL) {
            admin_list_free(env, list, quota_component_destroy);
            error_throw_owned(env, err);
            return NULL;
        }
        if (list_add(env, list, c, quota_component_destroy) < 0) {
            admin_list_free(env, list, quota_component_destroy);
            return NULL;
        }
    }
    kafka_common_quota_ClientQuotaFilter_t* filter = NULL;
    kafka_common_Error_t* err = strict ? FFI_SET(filter, kafka_common_quota_ClientQuotaFilter_contains_only_err(list, &ffi_err))
                                       : FFI_SET(filter, kafka_common_quota_ClientQuotaFilter_contains_err(list, &ffi_err));  // copies
    admin_list_free(env, list, quota_component_destroy);
    if (err != NULL) error_throw_owned(env, err);
    return filter;
}

// A borrowed entity as its entries, [[entityType, name | null], ...], read
// from the owned map entries() hands out, destroyed with its strings.
static napi_value quota_entity_to_js(napi_env env, const kafka_common_quota_ClientQuotaEntity_t* e) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* entries = NULL;
    kafka_common_Error_t* err = FFI_SET(entries, kafka_common_quota_ClientQuotaEntity_entries_err(e, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value out = admin_map_to_js(env, entries, key_string, admin_item_string);
    error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(entries, &ffi_err)));
    return out;
}

static napi_value key_quota_entity(napi_env env, const void* k) {
    return quota_entity_to_js(env, (const kafka_common_quota_ClientQuotaEntity_t*)k);
}

static napi_value item_double(napi_env env, const void* item) {
    napi_value v = NULL;
    if (napi_create_double(env, item == NULL ? NAN : *(const double*)item, &v) != napi_ok) js_throw_last_napi_error(env, "napi_create_double");
    return v;
}

// One entity's quotas, [[name, value], ...].
static napi_value item_quota_values(napi_env env, const void* item) {
    return admin_map_to_js(env, (const kafka_Map_t*)item, key_string, item_double);
}

// describeClientQuotas' value: [[entityEntries, [[name, value], ...]], ...].
static napi_value conv_quota_entities(napi_env env, void* v) {
    return admin_map_to_js(env, (const kafka_Map_t*)v, key_quota_entity, item_quota_values);
}

// ---- describeClientQuotas ---------------------------------------------------------------------

#define ADMIN_RPC DescribeClientQuotas

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeClientQuotasOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeClientQuotasOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeClientQuotasOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeClientQuotasResult_destroy_err),
    {ADMIN_ACC_ONE_BORROWED(entities, kafka_admin_DescribeClientQuotasResult_entities_err, conv_quota_entities), ADMIN_ACC_END},
};

// adminDescribeClientQuotas(handle, filter, options): the filter as [strict,
// components]; the result's one future is `entities`, which the result lends
// out.
napi_value js_admin_describe_client_quotas(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_common_quota_ClientQuotaFilter_t* filter = quota_filter_from_js(env, argv[1]);
    if (filter == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = NULL;
    if (o != NULL)
        err = FFI_SET(r, kafka_admin_Admin_describe_client_quotas_with_options_err(a->view, filter, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    error_warn(env, 1, FFI_ERR(kafka_common_quota_ClientQuotaFilter_destroy_err(filter, &ffi_err)));
    if (o == NULL) return NULL;
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeClientQuotasOptions_desc DescribeClientQuotasResult_desc

// ---- alterClientQuotas ------------------------------------------------------------------------

#define ADMIN_RPC AlterClientQuotas

ADMIN_OPT_TIMEOUT(kafka_admin_AlterClientQuotasOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(validateOnly, kafka_admin_AlterClientQuotasOptions_set_validate_only_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_AlterClientQuotasOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_AlterClientQuotasOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(validateOnly), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_AlterClientQuotasResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_AlterClientQuotasResult_values_err, key_quota_entity, NULL), ADMIN_ACC_END},
};

// adminAlterClientQuotas(handle, entries, options): each alteration as
// [entityEntries, ops]; the result's futures are `values`, a map keyed by the
// entity's entries (Java's all() is composed in TypeScript).
napi_value js_admin_alter_client_quotas(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* entries = handle_list(env, argv[1], quota_alteration_from_js, quota_alteration_destroy);
    if (entries == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, entries, quota_alteration_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_alter_client_quotas_with_options_err(a->view, entries, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, entries, quota_alteration_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison AlterClientQuotasOptions_desc AlterClientQuotasResult_desc

// ---- security: SCRAM: shared values ---------------------------------------------------------

// A description crosses as { name, credentialInfos: [[mechanismType,
// iterations], ...] }; an alteration as [0, user, mechanismType, iterations,
// password, salt] for an upsertion (TypeScript always has its salt) or [1,
// user, mechanismType] for a deletion.

static void scram_alteration_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_admin_UserScramCredentialAlteration_destroy_err((kafka_admin_UserScramCredentialAlteration_t*)item, out_err);
}

// A borrowed ScramCredentialInfo as [mechanismType, iterations].
static napi_value item_scram_credential_info(napi_env env, const void* item) {
    const kafka_admin_ScramCredentialInfo_t* i = (const kafka_admin_ScramCredentialInfo_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_admin_ScramMechanism_t* mechanism = NULL;
    int8_t type = 0;
    int32_t iterations = 0;
    kafka_common_Error_t* err = FFI_SET(mechanism, kafka_admin_ScramCredentialInfo_mechanism_err(i, &ffi_err));
    if (err == NULL) err = FFI_SET(type, kafka_admin_ScramMechanism_type_err(mechanism, &ffi_err));
    if (err == NULL) err = FFI_SET(iterations, kafka_admin_ScramCredentialInfo_iterations_err(i, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value pair, t = js_int32(env, type), n = js_int32(env, iterations);
    if (t == NULL || n == NULL) return NULL;
    NAPI_CHECK(env, napi_create_array_with_length(env, 2, &pair), NULL);
    NAPI_CHECK(env, napi_set_element(env, pair, 0, t), NULL);
    NAPI_CHECK(env, napi_set_element(env, pair, 1, n), NULL);
    return pair;
}

// A borrowed UserScramCredentialsDescription as { name, credentialInfos },
// its infos read from the owned list of copies the accessor hands out.
static napi_value item_scram_description(napi_env env, const void* item) {
    const kafka_admin_UserScramCredentialsDescription_t* d = (const kafka_admin_UserScramCredentialsDescription_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const char* name = NULL;
    kafka_List_t* infos = NULL;
    kafka_common_Error_t* err = FFI_SET(name, kafka_admin_UserScramCredentialsDescription_name_err(d, &ffi_err));
    if (err == NULL) err = FFI_SET(infos, kafka_admin_UserScramCredentialsDescription_credential_infos_err(d, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set(env, obj, "name", js_string_or_null(env, name)) < 0) {
        error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(infos, &ffi_err)));
        return NULL;
    }
    if (set(env, obj, "credentialInfos", owned_list_to_js(env, infos, item_scram_credential_info, 0)) < 0) return NULL;
    return obj;
}

static napi_value conv_scram_description(napi_env env, void* v) {
    return item_scram_description(env, v);
}

// describeUserScramCredentials' all(): [[user, description], ...].
static napi_value conv_scram_descriptions(napi_env env, void* v) {
    return admin_map_to_js(env, (const kafka_Map_t*)v, key_string, item_scram_description);
}

// users(): [user, ...].
static napi_value conv_strings(napi_env env, void* v) {
    return admin_list_to_js(env, (const kafka_List_t*)v, admin_item_string, 0);
}

// The mechanism of a type code (UNKNOWN for a code Java does not know).
static const kafka_admin_ScramMechanism_t* scram_mechanism_of(int32_t type, kafka_common_Error_t** out) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_admin_ScramMechanism_t* m = NULL;
    *out = FFI_SET(m, kafka_admin_ScramMechanism_from_type_err((int8_t)type, &ffi_err));
    return m;
}

// An upsertion [0, user, type, iterations, password, salt] as an owned
// alteration, with the user, the mechanism and the codes read; NULL with a JS
// exception pending.
static kafka_admin_UserScramCredentialAlteration_t* scram_upsertion_from_js(napi_env env, napi_value row, const char* user,
                                                                            const kafka_admin_ScramMechanism_t* mechanism) {
    int ok = 1;
    napi_value it = element(env, row, 3);
    napi_value pw = it == NULL ? NULL : element(env, row, 4);
    napi_value sa = pw == NULL ? NULL : element(env, row, 5);
    if (sa == NULL) return NULL;
    int32_t iterations = int32_of(env, it, "a credential's iterations", &ok);
    if (!ok) return NULL;
    JsBytes password, salt;
    if (js_bytes(env, pw, "a credential's password must be a Uint8Array", &password) < 0) return NULL;
    if (js_bytes(env, sa, "a credential's salt must be a Uint8Array", &salt) < 0) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_ScramCredentialInfo_t* info = NULL;
    kafka_admin_UserScramCredentialUpsertion_t* up = NULL;
    kafka_admin_UserScramCredentialAlteration_t* alteration = NULL;
    kafka_common_Error_t* err = FFI_SET(info, kafka_admin_ScramCredentialInfo_new_err(mechanism, iterations, &ffi_err));
    if (err == NULL)
        err = FFI_SET(up, kafka_admin_UserScramCredentialUpsertion_with_salt_err(user, info, password.bytes, salt.bytes, &ffi_err));  // copies
    if (err == NULL) err = FFI_SET(alteration, kafka_admin_UserScramCredentialAlteration_upsertion_err(up, &ffi_err));  // copies
    if (info != NULL) error_warn(env, 1, FFI_ERR(kafka_admin_ScramCredentialInfo_destroy_err(info, &ffi_err)));
    if (up != NULL) error_warn(env, 1, FFI_ERR(kafka_admin_UserScramCredentialUpsertion_destroy_err(up, &ffi_err)));
    if (err != NULL) error_throw_owned(env, err);
    return alteration;
}

// An alteration row as an owned UserScramCredentialAlteration, or NULL with a
// JS exception pending.
static void* scram_alteration_from_js(napi_env env, napi_value row) {
    int ok = 1;
    napi_value k = element(env, row, 0);
    napi_value u = k == NULL ? NULL : element(env, row, 1);
    napi_value t = u == NULL ? NULL : element(env, row, 2);
    if (t == NULL) return NULL;
    int32_t kind = int32_of(env, k, "an alteration's kind", &ok);
    int32_t type = ok ? int32_of(env, t, "an alteration's mechanism type", &ok) : 0;
    if (!ok) return NULL;
    char* user = js_string_dup(env, u, "an alteration's user must be a string");
    if (user == NULL) return NULL;
    kafka_common_Error_t* err = NULL;
    const kafka_admin_ScramMechanism_t* mechanism = scram_mechanism_of(type, &err);
    kafka_admin_UserScramCredentialAlteration_t* alteration = NULL;
    if (err != NULL) {
        error_throw_owned(env, err);
    } else if (kind == 0) {
        alteration = scram_upsertion_from_js(env, row, user, mechanism);
    } else {
        kafka_common_Error_t* ffi_err = NULL;
        kafka_admin_UserScramCredentialDeletion_t* del = NULL;
        err = FFI_SET(del, kafka_admin_UserScramCredentialDeletion_new_err(user, mechanism, &ffi_err));
        if (err == NULL) err = FFI_SET(alteration, kafka_admin_UserScramCredentialAlteration_deletion_err(del, &ffi_err));  // copies
        if (del != NULL) error_warn(env, 1, FFI_ERR(kafka_admin_UserScramCredentialDeletion_destroy_err(del, &ffi_err)));
        if (err != NULL) error_throw_owned(env, err);
    }
    free(user);
    return alteration;
}

// `description(userName)`: the name a string, or null, which names no user.
static kafka_common_KafkaFuture_t* scram_description_of(napi_env env, const void* result, napi_value key, kafka_common_Error_t** out_err) {
    int ok = 0;
    char* name = string_or_null(env, key, "the user name must be a string or null", &ok);
    if (!ok) return NULL;
    kafka_common_KafkaFuture_t* future =
        kafka_admin_DescribeUserScramCredentialsResult_description_err((const kafka_admin_DescribeUserScramCredentialsResult_t*)result, name, out_err);
    free(name);
    return future;
}

// ---- describeUserScramCredentials -------------------------------------------------------------

#define ADMIN_RPC DescribeUserScramCredentials

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeUserScramCredentialsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeUserScramCredentialsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeUserScramCredentialsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeUserScramCredentialsResult_destroy_err),
    {ADMIN_ACC_ONE(all, kafka_admin_DescribeUserScramCredentialsResult_all_err, conv_scram_descriptions),
     ADMIN_ACC_ONE(users, kafka_admin_DescribeUserScramCredentialsResult_users_err, conv_strings),
     ADMIN_ACC_JS(description, scram_description_of, conv_scram_description), ADMIN_ACC_END},
};

// adminDescribeUserScramCredentials(handle, users, options): `users` an array
// of names, or null (every user, Java's null); the result's futures are the
// core's own `all`, `users` and `description(user)`, each read when asked for
// (Python's composition: the Result takes the C futures).
napi_value js_admin_describe_user_scram_credentials(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    kafka_List_t* users = NULL;
    if (type != napi_null) {
        users = string_list(env, argv[1]);
        if (users == NULL) return NULL;
    }
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        if (users != NULL) admin_list_free(env, users, free_item);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_describe_user_scram_credentials_with_users_options_err(a->view, users, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    if (users != NULL) admin_list_free(env, users, free_item);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeUserScramCredentialsOptions_desc DescribeUserScramCredentialsResult_desc

// ---- alterUserScramCredentials ----------------------------------------------------------------

#define ADMIN_RPC AlterUserScramCredentials

ADMIN_OPT_TIMEOUT(kafka_admin_AlterUserScramCredentialsOptions_set_timeout_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_AlterUserScramCredentialsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_AlterUserScramCredentialsOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_AlterUserScramCredentialsResult_destroy_err),
    {ADMIN_ACC_MAP(values, kafka_admin_AlterUserScramCredentialsResult_values_err, key_string, NULL), ADMIN_ACC_END},
};

// adminAlterUserScramCredentials(handle, alterations, options): each
// alteration as its row; the result's futures are `values`, keyed by user
// (Java's all() is composed in TypeScript).
napi_value js_admin_alter_user_scram_credentials(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_List_t* alterations = handle_list(env, argv[1], scram_alteration_from_js, scram_alteration_destroy);
    if (alterations == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) {
        admin_list_free(env, alterations, scram_alteration_destroy);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_alter_user_scram_credentials_with_options_err(a->view, alterations, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    admin_list_free(env, alterations, scram_alteration_destroy);
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison AlterUserScramCredentialsOptions_desc AlterUserScramCredentialsResult_desc

// ---- security: delegation tokens: shared values ---------------------------------------------

// A principal crosses as [principalType, name] into the core and as
// [principalType, name, tokenAuthenticated] out of it; a token as [tokenId,
// owner, tokenRequester, renewers, issueTimestamp, maxTimestamp,
// expiryTimestamp, hmac], the timestamps bigints and the HMAC a copied
// Buffer.

static void principal_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_security_auth_KafkaPrincipal_destroy_err((kafka_common_security_auth_KafkaPrincipal_t*)item, out_err);
}

// [principalType, name] as an owned principal, or NULL with a JS exception
// pending.
static kafka_common_security_auth_KafkaPrincipal_t* principal_from_js(napi_env env, napi_value pair) {
    napi_value t = element(env, pair, 0);
    napi_value n = t == NULL ? NULL : element(env, pair, 1);
    if (n == NULL) return NULL;
    char* type = js_string_dup(env, t, "a principal's type must be a string");
    char* name = type == NULL ? NULL : js_string_dup(env, n, "a principal's name must be a string");
    if (name == NULL) {
        free(type);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_security_auth_KafkaPrincipal_t* p = NULL;
    kafka_common_Error_t* err = FFI_SET(p, kafka_common_security_auth_KafkaPrincipal_new_err(type, name, &ffi_err));
    free(type);
    free(name);
    if (err != NULL) error_throw_owned(env, err);
    return p;
}

static void* principal_from_js_item(napi_env env, napi_value v) {
    return principal_from_js(env, v);
}

// A borrowed principal as [principalType, name, tokenAuthenticated].
static napi_value principal_to_js(napi_env env, const kafka_common_security_auth_KafkaPrincipal_t* p) {
    if (p == NULL) return js_null(env);
    kafka_common_Error_t* ffi_err = NULL;
    const char *type = NULL, *name = NULL;
    int8_t token = 0;
    kafka_common_Error_t* err = FFI_SET(type, kafka_common_security_auth_KafkaPrincipal_principal_type_err(p, &ffi_err));
    if (err == NULL) err = FFI_SET(name, kafka_common_security_auth_KafkaPrincipal_name_err(p, &ffi_err));
    if (err == NULL) err = FFI_SET(token, kafka_common_security_auth_KafkaPrincipal_token_authenticated_err(p, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value vals[3] = {js_string_or_null(env, type), js_string_or_null(env, name), js_bool(env, token)};
    napi_value out;
    NAPI_CHECK(env, napi_create_array_with_length(env, 3, &out), NULL);
    for (uint32_t i = 0; i < 3; i++) {
        if (vals[i] == NULL) return NULL;
        NAPI_CHECK(env, napi_set_element(env, out, i, vals[i]), NULL);
    }
    return out;
}

static napi_value item_principal(napi_env env, const void* item) {
    return principal_to_js(env, (const kafka_common_security_auth_KafkaPrincipal_t*)item);
}

// A borrowed DelegationToken as its eight-tuple.
static napi_value item_delegation_token(napi_env env, const void* item) {
    const kafka_common_security_token_delegation_DelegationToken_t* t = (const kafka_common_security_token_delegation_DelegationToken_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_security_token_delegation_TokenInformation_t* info = NULL;
    const char* id = NULL;
    const kafka_common_security_auth_KafkaPrincipal_t *owner = NULL, *requester = NULL;
    kafka_List_t* renewers = NULL;
    int64_t issue = 0, max = 0, expiry = 0;
    kafka_Bytes_t hmac = {NULL, 0};
    kafka_common_Error_t* err = FFI_SET(info, kafka_common_security_token_delegation_DelegationToken_token_info_err(t, &ffi_err));
    if (err == NULL) err = FFI_SET(id, kafka_common_security_token_delegation_TokenInformation_token_id_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(owner, kafka_common_security_token_delegation_TokenInformation_owner_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(requester, kafka_common_security_token_delegation_TokenInformation_token_requester_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(issue, kafka_common_security_token_delegation_TokenInformation_issue_timestamp_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(max, kafka_common_security_token_delegation_TokenInformation_max_timestamp_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(expiry, kafka_common_security_token_delegation_TokenInformation_expiry_timestamp_err(info, &ffi_err));
    if (err == NULL) err = FFI_SET(hmac, kafka_common_security_token_delegation_DelegationToken_hmac_err(t, &ffi_err));
    if (err == NULL) err = FFI_SET(renewers, kafka_common_security_token_delegation_TokenInformation_renewers_err(info, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value buffer = NULL;
    void* ignored = NULL;
    napi_value vals[8] = {js_string_or_null(env, id), principal_to_js(env, owner), principal_to_js(env, requester),
                          owned_list_to_js(env, renewers, item_principal, 0), js_int64(env, issue), js_int64(env, max),
                          js_int64(env, expiry), NULL};
    NAPI_CHECK(env, napi_create_buffer_copy(env, (size_t)(hmac.data == NULL || hmac.len < 0 ? 0 : hmac.len), hmac.data, &ignored, &buffer), NULL);
    vals[7] = buffer;
    napi_value out;
    NAPI_CHECK(env, napi_create_array_with_length(env, 8, &out), NULL);
    for (uint32_t i = 0; i < 8; i++) {
        if (vals[i] == NULL) return NULL;
        NAPI_CHECK(env, napi_set_element(env, out, i, vals[i]), NULL);
    }
    return out;
}

static napi_value conv_delegation_token(napi_env env, void* v) {
    return item_delegation_token(env, v);
}

static napi_value conv_delegation_tokens(napi_env env, void* v) {
    return admin_list_to_js(env, (const kafka_List_t*)v, item_delegation_token, 0);
}

// An expiry timestamp: the future's `int64_t *`, as a bigint.
static napi_value conv_int64_ptr(napi_env env, void* v) {
    return js_int64(env, v == NULL ? 0 : *(const int64_t*)v);
}

// An HMAC argument, a Uint8Array, as a borrowed byte view; -1 with a JS
// exception pending.
static int hmac_of(napi_env env, napi_value value, kafka_Bytes_t* out) {
    JsBytes b;
    if (js_bytes(env, value, "hmac must be a Uint8Array", &b) < 0) return -1;
    if (b.is_null) {
        napi_throw_type_error(env, NULL, "hmac must not be null");
        return -1;
    }
    *out = b.bytes;
    return 0;
}

// ---- createDelegationToken --------------------------------------------------------------------

#define ADMIN_RPC CreateDelegationToken

ADMIN_OPT_TIMEOUT(kafka_admin_CreateDelegationTokenOptions_set_timeout_ms_err)
ADMIN_OPT_INT64(maxLifetimeMs, kafka_admin_CreateDelegationTokenOptions_set_max_lifetime_ms_err)
// `renewers`: the stored list as [principalType, name] pairs (TypeScript
// refuses a null one at the call, as Java's createDelegationToken does).
static int ADMIN_SETTER_FN(renewers)(napi_env env, void* o, napi_value v, kafka_common_Error_t** out_err) {
    kafka_List_t* list = handle_list(env, v, principal_from_js_item, principal_destroy);
    if (list == NULL) return -1;
    kafka_admin_CreateDelegationTokenOptions_set_renewers_err((ADMIN_OPTIONS_T*)o, list, out_err);  // copies
    admin_list_free(env, list, principal_destroy);
    return 0;
}
// `owner`: [principalType, name], or null, which leaves the request
// principal the owner.
static int ADMIN_SETTER_FN(owner)(napi_env env, void* o, napi_value v, kafka_common_Error_t** out_err) {
    napi_valuetype type;
    if (napi_typeof(env, v, &type) != napi_ok) return -1;
    kafka_common_security_auth_KafkaPrincipal_t* owner = type == napi_null ? NULL : principal_from_js(env, v);
    if (type != napi_null && owner == NULL) return -1;
    kafka_admin_CreateDelegationTokenOptions_set_owner_err((ADMIN_OPTIONS_T*)o, owner, out_err);  // copies
    kafka_common_Error_t* ffi_err = NULL;
    if (owner != NULL) error_warn(env, 1, FFI_ERR(kafka_common_security_auth_KafkaPrincipal_destroy_err(owner, &ffi_err)));
    return 0;
}
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_CreateDelegationTokenOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_CreateDelegationTokenOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(renewers), ADMIN_SETTER(owner), ADMIN_SETTER(maxLifetimeMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_CreateDelegationTokenResult_destroy_err),
    {ADMIN_ACC_ONE_BORROWED(delegation_token, kafka_admin_CreateDelegationTokenResult_delegation_token_err, conv_delegation_token),
     ADMIN_ACC_END},
};

// adminCreateDelegationToken(handle, options): the result's one future is
// `delegation_token`, which the result lends out.
napi_value js_admin_create_delegation_token(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 2 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[1]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err = FFI_SET(r, kafka_admin_Admin_create_delegation_token_with_options_err(a->view, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison CreateDelegationTokenOptions_desc CreateDelegationTokenResult_desc

// ---- renewDelegationToken ---------------------------------------------------------------------

#define ADMIN_RPC RenewDelegationToken

ADMIN_OPT_TIMEOUT(kafka_admin_RenewDelegationTokenOptions_set_timeout_ms_err)
ADMIN_OPT_INT64(renewTimePeriodMs, kafka_admin_RenewDelegationTokenOptions_set_renew_time_period_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_RenewDelegationTokenOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_RenewDelegationTokenOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(renewTimePeriodMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_RenewDelegationTokenResult_destroy_err),
    {ADMIN_ACC_ONE_BORROWED(expiry_timestamp, kafka_admin_RenewDelegationTokenResult_expiry_timestamp_err, conv_int64_ptr), ADMIN_ACC_END},
};

// adminRenewDelegationToken(handle, hmac, options): the HMAC a Uint8Array,
// which the core copies during the call; the result's one future is
// `expiry_timestamp`, which the result lends out.
napi_value js_admin_renew_delegation_token(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_Bytes_t hmac;
    if (hmac_of(env, argv[1], &hmac) < 0) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_renew_delegation_token_with_options_err(a->view, hmac, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison RenewDelegationTokenOptions_desc RenewDelegationTokenResult_desc

// ---- expireDelegationToken --------------------------------------------------------------------

#define ADMIN_RPC ExpireDelegationToken

ADMIN_OPT_TIMEOUT(kafka_admin_ExpireDelegationTokenOptions_set_timeout_ms_err)
ADMIN_OPT_INT64(expiryTimePeriodMs, kafka_admin_ExpireDelegationTokenOptions_set_expiry_time_period_ms_err)
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_ExpireDelegationTokenOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ExpireDelegationTokenOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(expiryTimePeriodMs), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_ExpireDelegationTokenResult_destroy_err),
    {ADMIN_ACC_ONE_BORROWED(expiry_timestamp, kafka_admin_ExpireDelegationTokenResult_expiry_timestamp_err, conv_int64_ptr), ADMIN_ACC_END},
};

// adminExpireDelegationToken(handle, hmac, options): as renew.
napi_value js_admin_expire_delegation_token(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 3 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    kafka_Bytes_t hmac;
    if (hmac_of(env, argv[1], &hmac) < 0) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[2]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_expire_delegation_token_with_options_err(a->view, hmac, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison ExpireDelegationTokenOptions_desc ExpireDelegationTokenResult_desc

// ---- describeDelegationToken ------------------------------------------------------------------

#define ADMIN_RPC DescribeDelegationToken

ADMIN_OPT_TIMEOUT(kafka_admin_DescribeDelegationTokenOptions_set_timeout_ms_err)
// `owners`: the stored list as [principalType, name] pairs, or null, Java's
// null (every token the caller owns or may describe).
static int ADMIN_SETTER_FN(owners)(napi_env env, void* o, napi_value v, kafka_common_Error_t** out_err) {
    napi_valuetype type;
    if (napi_typeof(env, v, &type) != napi_ok) return -1;
    if (type == napi_null) {
        kafka_admin_DescribeDelegationTokenOptions_set_owners_err((ADMIN_OPTIONS_T*)o, NULL, out_err);
        return 0;
    }
    kafka_List_t* list = handle_list(env, v, principal_from_js_item, principal_destroy);
    if (list == NULL) return -1;
    kafka_admin_DescribeDelegationTokenOptions_set_owners_err((ADMIN_OPTIONS_T*)o, list, out_err);  // copies
    admin_list_free(env, list, principal_destroy);
    return 0;
}
static const AdminOptionsDesc ADMIN_OPTIONS_DESC = {
    ADMIN_OPTIONS_NAME, ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeDelegationTokenOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeDelegationTokenOptions_destroy_err),
    {ADMIN_SETTER(timeoutMs), ADMIN_SETTER(owners), {NULL, NULL}},
};
static const AdminResultDesc ADMIN_RESULT_DESC = {
    ADMIN_RESULT_NAME, ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeDelegationTokenResult_destroy_err),
    {ADMIN_ACC_ONE_BORROWED(delegation_tokens, kafka_admin_DescribeDelegationTokenResult_delegation_tokens_err, conv_delegation_tokens),
     ADMIN_ACC_END},
};

// adminDescribeDelegationToken(handle, options): the result's one future is
// `delegation_tokens`, which the result lends out.
napi_value js_admin_describe_delegation_token(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    Admin* a = argc == 2 ? admin_unwrap(env, argv[0]) : NULL;
    if (a == NULL) return NULL;
    void* o = admin_options_build(env, &ADMIN_OPTIONS_DESC, argv[1]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    ADMIN_RESULT_T* r = NULL;
    kafka_common_Error_t* err =
        FFI_SET(r, kafka_admin_Admin_describe_delegation_token_with_options_err(a->view, (ADMIN_OPTIONS_T*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ADMIN_OPTIONS_DESC, o, err, r, &ADMIN_RESULT_DESC);
}

#undef ADMIN_RPC
// Its tables are named only inside the section.
#pragma GCC poison DescribeDelegationTokenOptions_desc DescribeDelegationTokenResult_desc
