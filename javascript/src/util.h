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

// Shared declarations of the Node-API addon. The addon includes only the
// bindings header (root CLAUDE.md §4, "C FFI Conventions"): every function
// without an `out_err` slot of its own is called through its `_err` twin.

#ifndef CONFLUENT_KAFKA_JS_UTIL_H
#define CONFLUENT_KAFKA_JS_UTIL_H

#include <node_api.h>
#include <stddef.h>
#include <stdint.h>

#include "confluent_kafka_bindings.h"

// Evaluates a Node-API call; on failure throws a JS error describing it (unless
// one is already pending) and returns `ret` from the enclosing function.
#define NAPI_CHECK(env, call, ret)                                       \
    do {                                                                 \
        if ((call) != napi_ok) {                                         \
            js_throw_last_napi_error((env), #call);                      \
            return ret;                                                  \
        }                                                                \
    } while (0)

// The error a call stored in `*slot` (NULL if none), resetting the slot to
// NULL: a `kafka_common_Error_t **out_err` must point at NULL on entry, so one
// slot serves every call of a function. `(v = f(args, &ffi_err),
// ffi_take(&ffi_err))` is then an expression for the call's error.
static inline kafka_common_Error_t* ffi_take(kafka_common_Error_t** slot) {
    kafka_common_Error_t* e = *slot;
    *slot = NULL;
    return e;
}

// Per-addon-instance state (napi_set_instance_data).
typedef struct {
    // lib/common/kafka_error.js's factory, turning the plain error object
    // error_to_js builds into a KafkaError; NULL until registered.
    napi_ref error_factory;
    // lib/producer/record_metadata.js's factory for a RecordMetadata.
    napi_ref metadata_factory;
} AddonData;

AddonData* addon_data(napi_env env);

// Throws a JS Error naming the failed Node-API call, unless an exception is
// already pending.
void js_throw_last_napi_error(napi_env env, const char* what);

// error.c ------------------------------------------------------------------

// A KafkaError for a BORROWED error (never destroyed here), or `null` for
// NULL. Returns NULL only when Node-API itself failed.
napi_value error_to_js(napi_env env, const kafka_common_Error_t* e);

// A KafkaError for an OWNED error, which is destroyed once converted;
// `null` for NULL.
napi_value error_take_to_js(napi_env env, kafka_common_Error_t* e);

// Throws a KafkaError for an OWNED error, which is destroyed.
void error_throw_owned(napi_env env, kafka_common_Error_t* e);

// Destroys an OWNED error, swallowing a panic reported by the destroy itself.
void error_discard(kafka_common_Error_t* e);

// The `kafka_common_Error_e` enumerator name of `code` without its
// `kafka_common_Error_` prefix, or NULL for an unknown value (generated from
// the bindings header at build time).
const char* error_name(int code);

// Registers the JS factories: `setFactories(errorFactory, metadataFactory)`.
napi_value js_set_factories(napi_env env, napi_callback_info info);

// record.c -----------------------------------------------------------------

// A borrowed byte view of a JS Uint8Array (a Buffer included) or `null`.
// `bytes.data` is NULL for `null`; an empty array points at a static byte, as
// `data == NULL` would read as Java's null.
typedef struct {
    kafka_Bytes_t bytes;
    int is_null;
} JsBytes;

// Reads `value` (a Uint8Array or null/undefined) into `out`; throws a
// TypeError naming `what` and returns -1 otherwise.
int js_bytes(napi_env env, napi_value value, const char* what, JsBytes* out);

// Reads a JS string into a malloc'ed NUL-terminated UTF-8 copy, or throws a
// TypeError naming `what` and returns NULL.
char* js_string_dup(napi_env env, napi_value value, const char* what);

// Builds the C record. `headers` is `null` or an array of `[key, value]`
// pairs, key a string and value a Uint8Array or null. Key and value point at
// `key` / `value` (or are Java's null) and stay borrowed; the header bytes are
// copied by the core during this call. Returns NULL with a JS exception
// pending on failure.
kafka_producer_ProducerRecord_t* record_build(napi_env env, const char* topic, int32_t partition,
                                              int64_t timestamp, const JsBytes* key,
                                              const JsBytes* value, napi_value headers);

// producer.c ---------------------------------------------------------------

napi_value producer_init(napi_env env, napi_value exports);

#endif  // CONFLUENT_KAFKA_JS_UTIL_H
