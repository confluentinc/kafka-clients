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

// What a mock keeps of the app's JavaScript values (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Implementation over the FFI), shared by the
// mocks: a record whose key and value cross as the FFI's `void *`
// (MockRecord), and an error JS gave a mock, which the core carries as a
// LocalCallbackError (InjectedError). Each mock keeps its own lists of them;
// the counts below are of every mock's.

#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "util.h"

// The counts producerStats() and consumerStats() read, for the memory tests.
static atomic_long stat_mock_records_made;
static atomic_long stat_mock_records_freed;
static atomic_long stat_injected_made;
static atomic_long stat_injected_freed;

// ---- a mock's records -------------------------------------------------------

MockRecord* mock_record_new(napi_env env, napi_value record) {
    MockRecord* r = (MockRecord*)calloc(1, sizeof(MockRecord));
    if (r == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    if (napi_create_reference(env, record, 1, &r->record) != napi_ok) {
        free(r);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    atomic_fetch_add(&stat_mock_records_made, 1);
    return r;
}

void mock_record_free(napi_env env, MockRecord* r) {
    delete_ref(env, &r->record);
    free(r);
    atomic_fetch_add(&stat_mock_records_freed, 1);
}

void mock_records_free(napi_env env, MockRecord** list) {
    while (*list != NULL) {
        MockRecord* r = *list;
        *list = r->next;
        mock_record_free(env, r);
    }
}

// The record whose key crossed as `key`, or NULL for a pointer no mock made
// (none: the FFI's mocks hand back the pointers they were given).
MockRecord* mock_record_of(const void* key) {
    if (key == NULL) return NULL;
    return (MockRecord*)((const char*)key - offsetof(MockRecord, key_tag));
}

// ---- a mock's injected errors -----------------------------------------------

// An error JS gave a mock (setSendException, errorNext, setPollException,
// ...): the core carries it as a LocalCallbackError whose opaque is this
// struct, and error.c hands the very same JS value back (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Errors). Two owners: the core's error,
// which releases it from any thread once its last copy is dropped
// (injected_error_release), and the mock's list, which deletes the reference
// on the JS thread once the core's copies are gone (the sweep of every mock
// call, injected_errors_sweep) or at the destroy (injected_errors_release);
// the struct is freed by the last of them, and a value read after the destroy
// is the core's plain error.
struct InjectedError {
    uint32_t magic;
    atomic_int owners;
    napi_env env;
    napi_ref value;
    InjectedError* next;
};

#define INJECTED_ERROR_MAGIC 0x4a53494eu

static void injected_error_release(void* opaque) {
    InjectedError* e = (InjectedError*)opaque;
    if (atomic_fetch_sub(&e->owners, 1) == 1) {
        free(e);
        atomic_fetch_add(&stat_injected_freed, 1);
    }
}

void injected_errors_release(napi_env env, InjectedError** list) {
    while (*list != NULL) {
        InjectedError* e = *list;
        *list = e->next;
        delete_ref(env, &e->value);
        injected_error_release(e);
    }
}

// The errors the core holds no copy of any more (the list is their last
// owner): their references are deleted, on the JS thread, so a value JS gave
// the mock is collectable once the core is done with it.
void injected_errors_sweep(napi_env env, InjectedError** list) {
    InjectedError** link = list;
    while (*link != NULL) {
        InjectedError* e = *link;
        if (atomic_load(&e->owners) == 1) {
            *link = e->next;
            delete_ref(env, &e->value);
            injected_error_release(e);
        } else {
            link = &e->next;
        }
    }
}

napi_value injected_error_value(napi_env env, void* opaque) {
    InjectedError* e = (InjectedError*)opaque;
    napi_value value = NULL;
    if (e == NULL || e->magic != INJECTED_ERROR_MAGIC || e->env != env || e->value == NULL) return NULL;
    if (napi_get_reference_value(env, e->value, &value) != napi_ok) return NULL;
    return value;
}

int injected_error_new(napi_env env, InjectedError** list, napi_value value, kafka_common_Error_t** out) {
    *out = NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, value, &type), -1);
    if (type == napi_null) return 0;
    InjectedError* e = (InjectedError*)calloc(1, sizeof(InjectedError));
    if (e == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return -1;
    }
    e->magic = INJECTED_ERROR_MAGIC;
    e->env = env;
    atomic_init(&e->owners, 2);
    if (napi_create_reference(env, value, 1, &e->value) != napi_ok) {
        free(e);
        js_throw_last_napi_error(env, "napi_create_reference");
        return -1;
    }
    // The text the core logs: the error's String(), as Java's toString().
    napi_value text;
    char* message = NULL;
    if (napi_coerce_to_string(env, value, &text) == napi_ok) message = js_string_dup(env, text, "the error's text");
    if (message == NULL) {
        napi_value ignored;
        napi_get_and_clear_last_exception(env, &ignored);
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err =
        FFI_SET(*out, kafka_common_Error_local_callback_err(message, e, injected_error_release, &ffi_err));
    free(message);
    if (err != NULL) {
        delete_ref(env, &e->value);
        free(e);
        error_throw_owned(env, err);
        return -1;
    }
    e->next = *list;
    *list = e;
    atomic_fetch_add(&stat_injected_made, 1);
    error_stat_made(*out);
    return 0;
}

// ---- test-only --------------------------------------------------------------

static int count_put(napi_env env, napi_value obj, const char* name, atomic_long* count) {
    napi_value v;
    return napi_create_int64(env, (int64_t)atomic_load(count), &v) == napi_ok
                   && napi_set_named_property(env, obj, name, v) == napi_ok
               ? 0
               : -1;
}

int mock_values_stats_put(napi_env env, napi_value obj) {
    return count_put(env, obj, "mockRecordsMade", &stat_mock_records_made)
                   || count_put(env, obj, "mockRecordsFreed", &stat_mock_records_freed)
                   || count_put(env, obj, "injectedMade", &stat_injected_made)
                   || count_put(env, obj, "injectedFreed", &stat_injected_freed)
               ? -1
               : 0;
}
