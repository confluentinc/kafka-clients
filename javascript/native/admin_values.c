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
