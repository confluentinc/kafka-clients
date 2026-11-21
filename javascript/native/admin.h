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

// The admin client's shared declarations (admin.c, admin_values.c): the
// native handle, its futures and result boxes, and the descriptor tables of
// each RPC's options and result. It includes util.h, the one file that
// includes the bindings header.

#ifndef CONFLUENT_KAFKA_JS_ADMIN_H
#define CONFLUENT_KAFKA_JS_ADMIN_H

#include "util.h"

typedef struct AdminFuture AdminFuture;

// The native admin client handle: KafkaAdminClient's owned Admin_t, or a
// MockAdminClient through its __as_Admin view.
typedef struct {
    napi_env env;                              // the environment that created it (JS thread)
    kafka_admin_Admin_t* owned;                // KafkaAdminClient's handle, NULL once destroyed
    kafka_admin_MockAdminClient_t* mock;       // MockAdminClient's handle, NULL once destroyed
    const kafka_admin_Admin_t* view;           // `owned`, or the mock's __as_Admin view
    CallbackQueue queue;                       // notify hook, tsfn, drain, liveness
    int destroying;                            // the destroy began
    int owners;                                // the handle object, the tsfn, each future's context
    // The futures JS holds, by the slot number JS keeps (a free slot holds
    // NULL and is chained through `free_next`).
    AdminFuture** slots;
    int32_t* free_next;
    int32_t slot_count;
    int32_t free_head;                         // -1: none free
} Admin;

// Converts the BORROWED value a future delivered into its plain JS shape, or
// returns NULL with a JS exception pending.
typedef napi_value (*admin_conv_fn)(napi_env env, void* value);
// Converts a BORROWED key of a map of futures.
typedef napi_value (*admin_key_fn)(napi_env env, const void* key);

// One future of a result: the slot's reference, each wait's.
struct AdminFuture {
    Admin* a;
    kafka_common_KafkaFuture_t* future;
    int owned;                                 // destroyed with the context
    napi_ref keep;                             // a borrowed future's owner, the box holding it
    admin_conv_fn conv;                        // NULL: a Void future, whose value is null
    int refs;
    int32_t slot;
    int waiting;                               // the untimed wait started
};

typedef kafka_Map_t* (*admin_map_acc_fn)(const void* result, kafka_common_Error_t** out_err);
typedef kafka_common_KafkaFuture_t* (*admin_one_acc_fn)(const void* result, kafka_common_Error_t** out_err);
typedef kafka_common_KafkaFuture_t* (*admin_str_acc_fn)(const void* result, const char* key, kafka_common_Error_t** out_err);
// A key of another shape, read by the function itself: the future, or NULL
// with a JS exception pending (`*out_err` NULL) or the core's error.
typedef kafka_common_KafkaFuture_t* (*admin_js_acc_fn)(napi_env env, const void* result, napi_value key,
                                                       kafka_common_Error_t** out_err);

// One accessor of a result: exactly one of `map`, `one`, `by_str`, `by_js`.
typedef struct {
    const char* name;
    admin_map_acc_fn map;
    admin_one_acc_fn one;
    admin_str_acc_fn by_str;
    admin_js_acc_fn by_js;
    admin_key_fn key;                          // `map`: the key converter
    admin_conv_fn conv;                        // the futures' value converter; NULL: Void
    int borrowed;                              // `one`: the future is borrowed from the result
} AdminAccessor;

typedef struct {
    const char* name;
    void (*destroy)(void* result, kafka_common_Error_t** out_err);
    AdminAccessor accessors[12];               // ended by a row with a NULL name
} AdminResultDesc;

// A result box's data: the owned result (or a map of futures, NULL result).
typedef struct {
    void* result;
    kafka_Map_t* map;
    const AdminResultDesc* desc;
} AdminResultBox;

// One RPC's descriptor tables are written in a section that binds the RPC
// once, `#define ADMIN_RPC <Rpc>` ... `#undef ADMIN_RPC`: the tables, their
// names and the types the rows are checked against all come from it, so a row
// copied from another RPC's section does not compile. Each row still names its
// header function in full, which the lints read. A section left without its
// `#undef` is a macro redefinition at the next one, and a row outside any
// section names no type, so neither compiles.
#define ADMIN_CAT_(a, b) a##b
#define ADMIN_CAT(a, b) ADMIN_CAT_(a, b)
#define ADMIN_STR_(a) #a
#define ADMIN_STR(a) ADMIN_STR_(a)
#define ADMIN_RESULT_T ADMIN_CAT(ADMIN_CAT(kafka_admin_, ADMIN_RPC), Result_t)
#define ADMIN_OPTIONS_T ADMIN_CAT(ADMIN_CAT(kafka_admin_, ADMIN_RPC), Options_t)
#define ADMIN_RESULT_DESC ADMIN_CAT(ADMIN_RPC, Result_desc)
#define ADMIN_OPTIONS_DESC ADMIN_CAT(ADMIN_RPC, Options_desc)
#define ADMIN_RESULT_NAME ADMIN_STR(ADMIN_RPC) "Result"
#define ADMIN_OPTIONS_NAME ADMIN_STR(ADMIN_RPC) "Options"

// The rows of an AdminResultDesc: `ADMIN_CHECKED` (a `_Generic` with no
// default) makes a row compile only when its function has the type the header
// gives the section's RPC; the casts then drop only the `<Rpc>Result_t *` of
// the first parameter.
#define ADMIN_CHECKED(T, fn) _Generic((fn), T: (fn))
#define ADMIN_RESULT_DESTROY_FN(fn) \
    ((void (*)(void*, kafka_common_Error_t**))ADMIN_CHECKED(void (*)(ADMIN_RESULT_T*, kafka_common_Error_t**), fn))
#define ADMIN_ACC_ONE(acc, fn, conv)                                                                                   \
    {#acc, NULL, (admin_one_acc_fn)ADMIN_CHECKED(kafka_common_KafkaFuture_t * (*)(const ADMIN_RESULT_T*, kafka_common_Error_t**), fn), \
     NULL, NULL, NULL, (conv), 0}
// A future the result lends out (`const`): it is not destroyed with the
// context, which keeps the result's box alive instead.
#define ADMIN_ACC_ONE_BORROWED(acc, fn, conv)                                                                               \
    {#acc, NULL,                                                                                                            \
     (admin_one_acc_fn)ADMIN_CHECKED(const kafka_common_KafkaFuture_t * (*)(const ADMIN_RESULT_T*, kafka_common_Error_t**), fn), \
     NULL, NULL, NULL, (conv), 1}
#define ADMIN_ACC_MAP(acc, fn, keyfn, conv)                                                                  \
    {#acc, (admin_map_acc_fn)ADMIN_CHECKED(kafka_Map_t * (*)(const ADMIN_RESULT_T*, kafka_common_Error_t**), fn), \
     NULL, NULL, NULL, (keyfn), (conv), 0}
#define ADMIN_ACC_STR(acc, fn, conv)                                                                                  \
    {#acc, NULL, NULL,                                                                                                \
     (admin_str_acc_fn)ADMIN_CHECKED(kafka_common_KafkaFuture_t * (*)(const ADMIN_RESULT_T*, const char*, kafka_common_Error_t**), fn), \
     NULL, NULL, (conv), 0}
#define ADMIN_ACC_JS(acc, fn, conv) {#acc, NULL, NULL, NULL, (fn), NULL, (conv), 0}
#define ADMIN_ACC_END {NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0}

// One setter of an options object: runs `set_<field>` with the JS value, 0
// having called it (its caught panic in `*out_err`), or -1 with a JS
// exception pending.
typedef int (*admin_option_fn)(napi_env env, void* options, napi_value value, kafka_common_Error_t** out_err);

typedef struct {
    const char* name;                          // the JS setter's name (`timeoutMs`)
    admin_option_fn set;
} AdminOptionSetter;

typedef struct {
    const char* name;
    void* (*create)(kafka_common_Error_t** out_err);
    void (*destroy)(void* options, kafka_common_Error_t** out_err);
    AdminOptionSetter setters[12];             // ended by a row with a NULL name
} AdminOptionsDesc;

// The options' create and destroy, checked against the section's RPC as the
// result's rows are.
#define ADMIN_OPTIONS_NEW_FN(fn) ((void* (*)(kafka_common_Error_t**))ADMIN_CHECKED(ADMIN_OPTIONS_T * (*)(kafka_common_Error_t**), fn))
#define ADMIN_OPTIONS_DESTROY_FN(fn) \
    ((void (*)(void*, kafka_common_Error_t**))ADMIN_CHECKED(void (*)(ADMIN_OPTIONS_T*, kafka_common_Error_t**), fn))

// Java's null options: a marker the entry point turns into NULL.
#define ADMIN_NULL_OPTIONS ((void*)&admin_null_options_marker)
extern char admin_null_options_marker;
static inline void* admin_opts(void* options) {
    return options == ADMIN_NULL_OPTIONS ? NULL : options;
}

// The setter functions, named `<Rpc>Options__<js setter>` after the
// section's RPC, over the header's `kafka_admin_<Rpc>Options_set_<field>_err`,
// named in full; the options are passed as the section's own type, so another
// RPC's setter does not compile.
int admin_option_timeout(napi_env env, napi_value value, int32_t* out);
int admin_option_bool(napi_env env, napi_value value, int8_t* out);
int admin_option_int32(napi_env env, napi_value value, int32_t* out);
int admin_option_int64(napi_env env, napi_value value, int64_t* out);
#define ADMIN_SETTER_FN(js) ADMIN_CAT(ADMIN_CAT(ADMIN_RPC, Options__), js)
#define ADMIN_OPT_TIMEOUT(fn)                                                                               \
    static int ADMIN_SETTER_FN(timeoutMs)(napi_env env, void* o, napi_value v, kafka_common_Error_t** e) { \
        int32_t t;                                                                                          \
        if (admin_option_timeout(env, v, &t) < 0) return -1;                                                \
        fn((ADMIN_OPTIONS_T*)o, t, e);                                                                      \
        return 0;                                                                                           \
    }
#define ADMIN_OPT_BOOL(js, fn)                                                                       \
    static int ADMIN_SETTER_FN(js)(napi_env env, void* o, napi_value v, kafka_common_Error_t** e) { \
        int8_t b;                                                                                    \
        if (admin_option_bool(env, v, &b) < 0) return -1;                                            \
        fn((ADMIN_OPTIONS_T*)o, b, e);                                                               \
        return 0;                                                                                    \
    }
#define ADMIN_OPT_INT32(js, fn)                                                                      \
    static int ADMIN_SETTER_FN(js)(napi_env env, void* o, napi_value v, kafka_common_Error_t** e) { \
        int32_t i;                                                                                   \
        if (admin_option_int32(env, v, &i) < 0) return -1;                                           \
        fn((ADMIN_OPTIONS_T*)o, i, e);                                                               \
        return 0;                                                                                    \
    }
#define ADMIN_OPT_INT64(js, fn)                                                                      \
    static int ADMIN_SETTER_FN(js)(napi_env env, void* o, napi_value v, kafka_common_Error_t** e) { \
        int64_t i;                                                                                   \
        if (admin_option_int64(env, v, &i) < 0) return -1;                                           \
        fn((ADMIN_OPTIONS_T*)o, i, e);                                                               \
        return 0;                                                                                    \
    }
#define ADMIN_SETTER(js) {#js, ADMIN_SETTER_FN(js)}

// The options of `desc` built from JS's [setter, value] pairs (from
// `<Rpc>Options_new`, the core's Java defaults), ADMIN_NULL_OPTIONS for
// `null`, or NULL with a JS exception pending.
void* admin_options_build(napi_env env, const AdminOptionsDesc* desc, napi_value pairs);
void admin_options_free(napi_env env, const AdminOptionsDesc* desc, void* options);
// Ends an RPC: frees the options, throws `err` or returns the result's box.
napi_value admin_rpc_return(napi_env env, const AdminOptionsDesc* odesc, void* options, kafka_common_Error_t* err,
                            void* result, const AdminResultDesc* rdesc);

// The handle behind a JS admin handle, or NULL with a TypeError pending; the
// same for a MockAdminClient's handle only.
Admin* admin_unwrap(napi_env env, napi_value handle);
Admin* admin_mock_unwrap(napi_env env, napi_value handle);
// A box over an owned result (or map), destroyed by its finalizer.
napi_value admin_box_new(napi_env env, void* result, kafka_Map_t* map, const AdminResultDesc* desc);
// A future's context in the slot table: its slot, or -1 with a JS exception
// pending (an owned future destroyed). `keep` (or NULL) is the object owning a
// borrowed future.
int32_t admin_future_new(napi_env env, Admin* a, kafka_common_KafkaFuture_t* future, int owned, napi_value keep,
                         admin_conv_fn conv);

// A JS node's fields as an owned node; an array of them as an owned list of
// owned nodes (`null` is NULL with *is_null set); NULL with a JS exception
// pending on failure.
kafka_common_Node_t* admin_node_from_js(napi_env env, napi_value node);
kafka_List_t* admin_nodes_from_js(napi_env env, napi_value array, int* is_null);
// Frees a list, each item through `destroy` (NULL: borrowed items).
void admin_list_free(napi_env env, kafka_List_t* list, void (*destroy)(void*, kafka_common_Error_t**));
// Frees a map whose keys and values were malloc'ed.
void admin_map_free(napi_env env, kafka_Map_t* map);

// admin_values.c: the plain JS values of borrowed C values. A borrowed list
// as an array of item(env, item) (NULL as `null` when `null_for_null`, else
// empty), a borrowed map as an array of [key, value] pairs in its order, a
// Uuid as [mostSigBits, leastSigBits] bigints (`null` for NULL), a string
// item, and a list of AclOperation singletons as their codes (`null` for
// NULL). Each returns NULL with a JS exception pending on failure.
napi_value admin_list_to_js(napi_env env, const kafka_List_t* list, napi_value (*item)(napi_env, const void*), int null_for_null);
napi_value admin_map_to_js(napi_env env, const kafka_Map_t* map, napi_value (*key)(napi_env, const void*),
                           napi_value (*value)(napi_env, const void*));
napi_value admin_uuid_to_js(napi_env env, const kafka_common_Uuid_t* id);
napi_value admin_item_string(napi_env env, const void* item);
napi_value admin_acl_operations_to_js(napi_env env, const kafka_List_t* list);

#endif  // CONFLUENT_KAFKA_JS_ADMIN_H
