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

// Headers and Header over the FFI's RecordHeaders and RecordHeader (root
// CLAUDE.md, JavaScript Binding Conventions, Serialization, Q31; option 1 of
// design/current/javascript-producer/DECISIONS.md, entry 21). Each JS
// Headers / Header holds a handle object, a plain object napi_wrap'ed over a
// box and type-tagged, whose finalizer drops the object's reference on the
// box:
//
//   - a HeadersBox owns a kafka_common_header_internals_RecordHeaders_t and
//     its __as_Headers view; its references are the JS Headers and each
//     Header view over one of its headers;
//   - a HeaderBox is either an owned RecordHeader (one the app made, or the
//     binding's copy of a header read from a Headers that can still change)
//     or a borrowed view of a header inside a read-only RecordHeaders, which
//     holds a reference on that RecordHeaders' box; its references are the JS
//     Header and each value Buffer over its bytes.
//
// So a Header keeps its Headers' native list alive, and a value Buffer its
// header's native bytes, without a reference between JS objects (a strong
// napi_ref from a native finalizer would be a root, and never collected). A
// box is freed, and its handle destroyed, once, when its last reference
// goes. Every finalizer runs on the JS thread and runs no JS.

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

// The tags that tell a headers handle object and a header one apart from
// each other and from a producer's.
static const napi_type_tag HEADERS_TAG = {0x6b6a7368656164ULL, 0x6572735f74616731ULL};
static const napi_type_tag HEADER_TAG = {0x6b6a736865616465ULL, 0x725f746167310000ULL};

typedef struct HeadersBox {
    kafka_common_header_internals_RecordHeaders_t* handle;  // owned
    kafka_common_header_Headers_t* view;                    // its __as_Headers view, borrowed from handle
    int refs;
} HeadersBox;

typedef struct HeaderBox {
    kafka_common_header_internals_RecordHeader_t* owned;    // NULL for a view
    const kafka_common_header_internals_RecordHeader_t* record;  // owned, or borrowed from parent
    const kafka_common_header_Header_t* view;               // its __as_Header view
    HeadersBox* parent;                                     // a view's RecordHeaders, NULL for an owned header
    int refs;
} HeaderBox;

// The counts headerStats() reads, for the lifetime tests
// (test/unit/common/header/headers.test.js).
static atomic_long stat_headers_made;
static atomic_long stat_headers_freed;
static atomic_long stat_header_made;
static atomic_long stat_header_freed;
static atomic_long stat_header_copies;
static atomic_long stat_header_views;
static atomic_long stat_value_external;
static atomic_long stat_value_copied;

static void headers_unref(HeadersBox* box) {
    if (--box->refs > 0) return;
    kafka_common_Error_t* ffi_err = NULL;
    // A failure freeing the handle has nowhere to go from a finalizer.
    error_discard(FFI_ERR(kafka_common_header_internals_RecordHeaders_destroy_err(box->handle, &ffi_err)));
    free(box);
    atomic_fetch_add(&stat_headers_freed, 1);
}

static void header_unref(HeaderBox* box) {
    if (--box->refs > 0) return;
    if (box->owned != NULL) {
        kafka_common_Error_t* ffi_err = NULL;
        error_discard(FFI_ERR(kafka_common_header_internals_RecordHeader_destroy_err(box->owned, &ffi_err)));
    }
    if (box->parent != NULL) headers_unref(box->parent);
    free(box);
    atomic_fetch_add(&stat_header_freed, 1);
}

static void headers_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    headers_unref((HeadersBox*)data);
}

static void header_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    header_unref((HeaderBox*)data);
}

// A value Buffer's finalizer: drops the Buffer's reference on its header.
static void value_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)data;
    header_unref((HeaderBox*)hint);
}

// The handle object of an owned RecordHeaders, which it takes over (it is
// destroyed here when the object cannot be made). NULL with a JS exception
// pending on failure.
static napi_value headers_wrap(napi_env env, kafka_common_header_internals_RecordHeaders_t* handle) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_header_Headers_t* view = NULL;
    kafka_common_Error_t* err = FFI_SET(view, kafka_common_header_internals_RecordHeaders__as_Headers_err(handle, &ffi_err));
    HeadersBox* box = err == NULL ? (HeadersBox*)calloc(1, sizeof(HeadersBox)) : NULL;
    napi_value obj = NULL;
    if (err == NULL && box != NULL) {
        box->handle = handle;
        box->view = view;
        box->refs = 1;
        if (napi_create_object(env, &obj) != napi_ok || napi_type_tag_object(env, obj, &HEADERS_TAG) != napi_ok
            || napi_wrap(env, obj, box, headers_finalize, NULL, NULL) != napi_ok) {
            js_throw_last_napi_error(env, "wrapping a RecordHeaders");
            obj = NULL;
        }
    }
    if (obj != NULL) {
        atomic_fetch_add(&stat_headers_made, 1);
        return obj;
    }
    free(box);
    error_discard(FFI_ERR(kafka_common_header_internals_RecordHeaders_destroy_err(handle, &ffi_err)));
    if (err != NULL) error_throw_owned(env, err);
    else if (box == NULL) napi_throw_error(env, NULL, "out of memory");
    return NULL;
}

// The handle object of a header: an owned one (`owned`, taken over) or a view
// of `record` inside `parent`'s read-only RecordHeaders (`owned` NULL), which
// it holds a reference on. NULL with a JS exception pending on failure.
static napi_value header_wrap(napi_env env, kafka_common_header_internals_RecordHeader_t* owned,
                              const kafka_common_header_internals_RecordHeader_t* record, HeadersBox* parent) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_header_Header_t* view = NULL;
    kafka_common_Error_t* err = FFI_SET(view, kafka_common_header_internals_RecordHeader__as_Header_err(record, &ffi_err));
    HeaderBox* box = err == NULL ? (HeaderBox*)calloc(1, sizeof(HeaderBox)) : NULL;
    napi_value obj = NULL;
    if (err == NULL && box != NULL) {
        box->owned = owned;
        box->record = record;
        box->view = view;
        box->parent = parent;
        box->refs = 1;
        if (napi_create_object(env, &obj) != napi_ok || napi_type_tag_object(env, obj, &HEADER_TAG) != napi_ok
            || napi_wrap(env, obj, box, header_finalize, NULL, NULL) != napi_ok) {
            js_throw_last_napi_error(env, "wrapping a RecordHeader");
            obj = NULL;
        }
    }
    if (obj != NULL) {
        if (parent != NULL) parent->refs++;
        atomic_fetch_add(&stat_header_made, 1);
        return obj;
    }
    free(box);
    if (owned != NULL) error_discard(FFI_ERR(kafka_common_header_internals_RecordHeader_destroy_err(owned, &ffi_err)));
    if (err != NULL) error_throw_owned(env, err);
    else if (box == NULL) napi_throw_error(env, NULL, "out of memory");
    return NULL;
}

// The box behind a handle object of `tag`, or NULL with a TypeError pending.
static void* unwrap_tagged(napi_env env, napi_value value, const napi_type_tag* tag, const char* what) {
    bool tagged = false;
    void* data = NULL;
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok || type != napi_object
        || napi_check_object_type_tag(env, value, tag, &tagged) != napi_ok || !tagged
        || napi_unwrap(env, value, &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, what);
        return NULL;
    }
    return data;
}

static HeadersBox* unwrap_headers(napi_env env, napi_value value) {
    return (HeadersBox*)unwrap_tagged(env, value, &HEADERS_TAG, "not a headers handle");
}

static HeaderBox* unwrap_header(napi_env env, napi_value value) {
    return (HeaderBox*)unwrap_tagged(env, value, &HEADER_TAG, "not a header handle");
}

kafka_common_header_internals_RecordHeaders_t* headers_handle(napi_env env, napi_value value) {
    HeadersBox* box = unwrap_headers(env, value);
    return box == NULL ? NULL : box->handle;
}

static int is_nullish(napi_env env, napi_value value) {
    napi_valuetype type;
    return napi_typeof(env, value, &type) == napi_ok && (type == napi_null || type == napi_undefined);
}

// A key argument: encodeLikeJava's bytes of the key, or null for Java's null,
// which the core reports with Java's error. Sets `*key` (NULL for null) and
// returns 0, or returns -1 with a TypeError pending.
static int key_arg(napi_env env, napi_value value, char** key) {
    *key = NULL;
    if (is_nullish(env, value)) return 0;
    *key = js_bytes_cstring(env, value, "a header key must be a string");
    return *key == NULL ? -1 : 0;
}

static napi_value undefined_value(napi_env env) {
    napi_value v = NULL;
    napi_get_undefined(env, &v);
    return v;
}

// The Header JS gets for `record`, a header of `box`'s list: a view, holding
// a reference on `box`, while the headers are read-only; the binding's own
// copy of its key and value otherwise, which stays valid after the headers
// change, as Java's Header does (option 1). Read-only headers never change
// again, but the FFI frees the views a handle handed out on every mutating
// call on it (`add_*`, `remove`), refused ones included, so a view stays
// valid only because the binding never makes such a call on read-only
// headers: src/common/header/headers.ts sends it to the shared empty
// read-only headers instead, which hand out no view (javascript/CLAUDE.md,
// boundary rule 1). Sealing again (`set_read_only`) keeps the views.
static napi_value header_out(napi_env env, HeadersBox* box, const kafka_common_header_internals_RecordHeader_t* record,
                             int read_only) {
    if (read_only) {
        atomic_fetch_add(&stat_header_views, 1);
        return header_wrap(env, NULL, record, box);
    }
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_header_Header_t* view = NULL;
    const char* key = NULL;
    kafka_Bytes_t value = {NULL, 0};
    kafka_common_header_internals_RecordHeader_t* copy = NULL;
    kafka_common_Error_t* err = FFI_SET(view, kafka_common_header_internals_RecordHeader__as_Header_err(record, &ffi_err));
    if (err == NULL) err = FFI_SET(key, kafka_common_header_Header_key_err(view, &ffi_err));
    if (err == NULL) err = FFI_SET(value, kafka_common_header_Header_value_err(view, &ffi_err));
    if (err == NULL) err = FFI_SET(copy, kafka_common_header_internals_RecordHeader_new_err(key, value, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    atomic_fetch_add(&stat_header_copies, 1);
    return header_wrap(env, copy, copy, NULL);
}

// The headers' read-only state, or -1 with a JS exception pending.
static int read_only_of(napi_env env, HeadersBox* box) {
    kafka_common_Error_t* ffi_err = NULL;
    int8_t read_only = 0;
    kafka_common_Error_t* err =
        FFI_SET(read_only, kafka_common_header_internals_RecordHeaders_is_read_only_err(box->handle, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return -1;
    }
    return read_only ? 1 : 0;
}

// A list a Headers getter returned, of borrowed RecordHeader pointers, as an
// array of Headers handle objects (header_out); the list is destroyed here.
static napi_value list_out(napi_env env, HeadersBox* box, kafka_List_t* list) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    napi_value arr = NULL;
    kafka_common_Error_t* err = FFI_SET(size, kafka_List_size_err(list, &ffi_err));
    int read_only = err == NULL ? read_only_of(env, box) : 0;
    if (err == NULL && read_only >= 0 && napi_create_array_with_length(env, (size_t)size, &arr) != napi_ok) {
        js_throw_last_napi_error(env, "napi_create_array_with_length");
        arr = NULL;
        read_only = -1;
    }
    for (int32_t i = 0; err == NULL && read_only >= 0 && i < size; i++) {
        const kafka_common_header_internals_RecordHeader_t* record = NULL;
        err = FFI_SET(record, (const kafka_common_header_internals_RecordHeader_t*)kafka_List_get_err(list, i, &ffi_err));
        if (err != NULL) break;
        napi_value item = header_out(env, box, record, read_only);
        if (item == NULL || napi_set_element(env, arr, (uint32_t)i, item) != napi_ok) {
            if (item != NULL) js_throw_last_napi_error(env, "napi_set_element");
            arr = NULL;
            read_only = -1;
        }
    }
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return read_only < 0 ? NULL : arr;
}

// headersNew(headers, form): Java's RecordHeaders() (form 0, headers null),
// RecordHeaders(Header[]) (form 1) or RecordHeaders(Iterable<Header>) (form
// 2), `headers` an array of header handle objects, whose headers the core
// copies; a null element goes to the core as NULL, which reports Java's
// error ("Header cannot be null.").
napi_value js_headers_new(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    int32_t form = 0;
    if (argc != 2 || napi_get_value_int32(env, argv[1], &form) != napi_ok || form < 0 || form > 2) {
        napi_throw_type_error(env, NULL, "headersNew(headers, form)");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_header_internals_RecordHeaders_t* handle = NULL;
    if (form == 0) {
        kafka_common_Error_t* err = FFI_SET(handle, kafka_common_header_internals_RecordHeaders_new_err(&ffi_err));
        if (err != NULL) {
            error_throw_owned(env, err);
            return NULL;
        }
        return headers_wrap(env, handle);
    }
    uint32_t count = 0;
    NAPI_CHECK(env, napi_get_array_length(env, argv[0], &count), NULL);
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_List_new_err(&ffi_err));
    int thrown = 0;
    for (uint32_t i = 0; err == NULL && !thrown && i < count; i++) {
        napi_value item;
        HeaderBox* header = NULL;
        if (napi_get_element(env, argv[0], i, &item) != napi_ok) {
            js_throw_last_napi_error(env, "napi_get_element");
            thrown = 1;
        } else if (is_nullish(env, item)) {
            err = FFI_ERR(kafka_List_add_err(list, NULL, &ffi_err));
        } else if ((header = unwrap_header(env, item)) == NULL) {
            thrown = 1;
        } else {
            err = FFI_ERR(kafka_List_add_err(list, (void*)header->record, &ffi_err));
        }
    }
    if (err == NULL && !thrown) {
        err = form == 1 ? FFI_SET(handle, kafka_common_header_internals_RecordHeaders_with_header_slice_err(list, &ffi_err))
                        : FFI_SET(handle, kafka_common_header_internals_RecordHeaders_with_header_iter_err(list, &ffi_err));
    }
    if (list != NULL) error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return thrown ? NULL : headers_wrap(env, handle);
}

// headersCopy(headers): Java's new RecordHeaders(headers) of a RecordHeaders,
// a writable copy, whatever its read-only state.
napi_value js_headers_copy(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 1 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_header_internals_RecordHeaders_t* handle = NULL;
    kafka_common_Error_t* err =
        FFI_SET(handle, kafka_common_header_internals_RecordHeaders_with_record_headers_err(box->handle, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return headers_wrap(env, handle);
}

// headersAdd(headers, header): Java's add(Header), the core storing a copy of
// `header`; null goes to the core as NULL, which reports Java's error.
napi_value js_headers_add(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 2 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    const kafka_common_header_internals_RecordHeader_t* record = NULL;
    if (!is_nullish(env, argv[1])) {
        HeaderBox* header = unwrap_header(env, argv[1]);
        if (header == NULL) return NULL;
        record = header->record;
    }
    kafka_common_Error_t* ffi_err = NULL;
    // Headers_add_with_header takes an out_err of its own.
    kafka_common_Error_t* err = FFI_ERR(kafka_common_header_Headers_add_with_header(box->view, record, &ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return undefined_value(env);
}

// headersAddKeyValue(headers, key, value): Java's add(String, byte[]), key
// encodeLikeJava's bytes or null, value a Uint8Array or null, both copied by
// the core.
napi_value js_headers_add_key_value(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 3 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    char* key = NULL;
    if (key_arg(env, argv[1], &key)) return NULL;
    JsBytes value;
    if (js_bytes(env, argv[2], "a header value must be a Buffer, a Uint8Array or null", &value)) {
        free(key);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    // Headers_add_with_key_value takes an out_err of its own.
    kafka_common_Error_t* err =
        FFI_ERR(kafka_common_header_Headers_add_with_key_value(box->view, key, value.bytes, &ffi_err));
    free(key);
    if (err != NULL) error_throw_owned(env, err);
    return undefined_value(env);
}

// headersRemove(headers, key): Java's remove(String).
napi_value js_headers_remove(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 2 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    char* key = NULL;
    if (key_arg(env, argv[1], &key)) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    // Headers_remove takes an out_err of its own.
    kafka_common_Error_t* err = FFI_ERR(kafka_common_header_Headers_remove(box->view, key, &ffi_err));
    free(key);
    if (err != NULL) error_throw_owned(env, err);
    return undefined_value(env);
}

// headersLastHeader(headers, key): Java's lastHeader(String), a header handle
// object (header_out) or null.
napi_value js_headers_last_header(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 2 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    char* key = NULL;
    if (key_arg(env, argv[1], &key)) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_header_internals_RecordHeader_t* record = NULL;
    kafka_common_Error_t* err = FFI_SET(record, kafka_common_header_Headers_last_header_err(box->view, key, &ffi_err));
    free(key);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (record == NULL) {
        napi_value null_value;
        NAPI_CHECK(env, napi_get_null(env, &null_value), NULL);
        return null_value;
    }
    int read_only = read_only_of(env, box);
    return read_only < 0 ? NULL : header_out(env, box, record, read_only);
}

// headersHeaders(headers, key): Java's headers(String), read now: an array of
// header handle objects (header_out).
napi_value js_headers_headers(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 2 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    char* key = NULL;
    if (key_arg(env, argv[1], &key)) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_common_header_Headers_headers_err(box->view, key, &ffi_err));
    free(key);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return list_out(env, box, list);
}

// headersToArray(headers): Java's toArray(), an array of header handle
// objects (header_out).
napi_value js_headers_to_array(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 1 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_common_header_Headers_to_array_err(box->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return list_out(env, box, list);
}

// headersIterator(headers): Java's iterator(), the headers as they are now,
// an array of header handle objects (header_out).
napi_value js_headers_iterator(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 1 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_common_header_Headers_iter_err(box->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return list_out(env, box, list);
}

// headersSetReadOnly(headers): Java's setReadOnly(), which send() calls once
// the serializers have run.
napi_value js_headers_set_read_only(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 1 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_common_header_internals_RecordHeaders_set_read_only_err(box->handle, &ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return undefined_value(env);
}

// headersIsReadOnly(headers): Java's isReadOnly().
napi_value js_headers_is_read_only(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeadersBox* box = argc == 1 ? unwrap_headers(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    int read_only = read_only_of(env, box);
    if (read_only < 0) return NULL;
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, read_only == 1, &result), NULL);
    return result;
}

// headerNew(key, value): Java's new RecordHeader(String, byte[]), key
// encodeLikeJava's bytes or null (the core reports Java's error), value a
// Uint8Array or null; the core copies both.
napi_value js_header_new(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2) {
        napi_throw_type_error(env, NULL, "headerNew(key, value)");
        return NULL;
    }
    char* key = NULL;
    if (key_arg(env, argv[0], &key)) return NULL;
    JsBytes value;
    if (js_bytes(env, argv[1], "a header value must be a Buffer, a Uint8Array or null", &value)) {
        free(key);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_header_internals_RecordHeader_t* header = NULL;
    kafka_common_Error_t* err =
        FFI_SET(header, kafka_common_header_internals_RecordHeader_new_err(key, value.bytes, &ffi_err));
    free(key);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return header_wrap(env, header, header, NULL);
}

// headerKey(header): Java's key(), read through kafka_common_header_Header_key.
napi_value js_header_key(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeaderBox* box = argc == 1 ? unwrap_header(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const char* key = NULL;
    kafka_common_Error_t* err = FFI_SET(key, kafka_common_header_Header_key_err(box->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_create_string_utf8(env, key == NULL ? "" : key, NAPI_AUTO_LENGTH, &result), NULL);
    return result;
}

// headerValue(header): Java's value(), read through
// kafka_common_header_Header_value: a Buffer over the header's native bytes,
// holding a reference on the header until the Buffer is collected, or null.
// Where the runtime refuses external buffers, a copy.
napi_value js_header_value(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    HeaderBox* box = argc == 1 ? unwrap_header(env, argv[0]) : NULL;
    if (box == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Bytes_t value = {NULL, 0};
    kafka_common_Error_t* err = FFI_SET(value, kafka_common_header_Header_value_err(box->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    if (value.data == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &result), NULL);
        return result;
    }
    size_t len = (size_t)(value.len > 0 ? value.len : 0);
    if (len == 0) {
        void* ignored = NULL;
        NAPI_CHECK(env, napi_create_buffer(env, 0, &ignored, &result), NULL);
        return result;
    }
    // The core's bytes are never written through the Buffer: the binding
    // hands them out as Java's value() hands out its array.
    napi_status status =
        napi_create_external_buffer(env, len, (void*)value.data, value_finalize, box, &result);
    if (status == napi_ok) {
        box->refs++;
        atomic_fetch_add(&stat_value_external, 1);
        return result;
    }
    if (status != napi_no_external_buffers_allowed) {
        js_throw_last_napi_error(env, "napi_create_external_buffer");
        return NULL;
    }
    void* ignored = NULL;
    NAPI_CHECK(env, napi_create_buffer_copy(env, len, value.data, &ignored, &result), NULL);
    atomic_fetch_add(&stat_value_copied, 1);
    return result;
}

static int set_count(napi_env env, napi_value obj, const char* name, atomic_long* count) {
    napi_value v;
    return napi_create_int64(env, (int64_t)atomic_load(count), &v) == napi_ok
                   && napi_set_named_property(env, obj, name, v) == napi_ok
               ? 0
               : -1;
}

// headerStats(): the counts of the header handles made and freed, of the
// copies option 1 makes and the views it hands out, and of the value Buffers
// over native bytes and copied. Test-only.
napi_value js_header_stats(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (set_count(env, obj, "headersMade", &stat_headers_made) || set_count(env, obj, "headersFreed", &stat_headers_freed)
        || set_count(env, obj, "headerMade", &stat_header_made) || set_count(env, obj, "headerFreed", &stat_header_freed)
        || set_count(env, obj, "headerCopies", &stat_header_copies) || set_count(env, obj, "headerViews", &stat_header_views)
        || set_count(env, obj, "valueExternal", &stat_value_external)
        || set_count(env, obj, "valueCopied", &stat_value_copied)) {
        js_throw_last_napi_error(env, "headerStats");
        return NULL;
    }
    return obj;
}
