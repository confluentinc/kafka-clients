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

ADMIN_OPT_TIMEOUT(DescribeCluster, kafka_admin_DescribeClusterOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(DescribeCluster, includeAuthorizedOperations, kafka_admin_DescribeClusterOptions_set_include_authorized_operations_err)
ADMIN_OPT_BOOL(DescribeCluster, includeFencedBrokers, kafka_admin_DescribeClusterOptions_set_include_fenced_brokers_err)
static const AdminOptionsDesc DescribeClusterOptions_desc = {
    "DescribeClusterOptions", ADMIN_OPTIONS_NEW_FN(kafka_admin_DescribeClusterOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_DescribeClusterOptions_destroy_err),
    {ADMIN_SETTER(DescribeCluster, timeoutMs), ADMIN_SETTER(DescribeCluster, includeAuthorizedOperations),
     ADMIN_SETTER(DescribeCluster, includeFencedBrokers), {NULL, NULL}},
};
static const AdminResultDesc DescribeClusterResult_desc = {
    "DescribeClusterResult", ADMIN_RESULT_DESTROY_FN(kafka_admin_DescribeClusterResult_destroy_err),
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
    void* o = admin_options_build(env, &DescribeClusterOptions_desc, argv[1]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_DescribeClusterResult_t* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_describe_cluster_with_options_err(a->view, (kafka_admin_DescribeClusterOptions_t*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &DescribeClusterOptions_desc, o, err, r, &DescribeClusterResult_desc);
}

// ---- listTopics -------------------------------------------------------------------

ADMIN_OPT_TIMEOUT(ListTopics, kafka_admin_ListTopicsOptions_set_timeout_ms_err)
ADMIN_OPT_BOOL(ListTopics, listInternal, kafka_admin_ListTopicsOptions_set_list_internal_err)
static const AdminOptionsDesc ListTopicsOptions_desc = {
    "ListTopicsOptions", ADMIN_OPTIONS_NEW_FN(kafka_admin_ListTopicsOptions_new_err),
    ADMIN_OPTIONS_DESTROY_FN(kafka_admin_ListTopicsOptions_destroy_err),
    {ADMIN_SETTER(ListTopics, timeoutMs), ADMIN_SETTER(ListTopics, listInternal), {NULL, NULL}},
};
static const AdminResultDesc ListTopicsResult_desc = {
    "ListTopicsResult", ADMIN_RESULT_DESTROY_FN(kafka_admin_ListTopicsResult_destroy_err),
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
    void* o = admin_options_build(env, &ListTopicsOptions_desc, argv[1]);
    if (o == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_ListTopicsResult_t* r = NULL;
    kafka_common_Error_t* err = FFI_SET(
        r, kafka_admin_Admin_list_topics_with_options_err(a->view, (kafka_admin_ListTopicsOptions_t*)admin_opts(o), &ffi_err));
    return admin_rpc_return(env, &ListTopicsOptions_desc, o, err, r, &ListTopicsResult_desc);
}
