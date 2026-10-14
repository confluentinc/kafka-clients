#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <confluent_kafka_bindings.h>
#ifdef __APPLE__
#include "tinycthread.h"
#else
#include <threads.h>
#endif
#include <string.h>
#include <stdint.h>
#ifdef _WIN32
#include <windows.h>
#define sleep(ms) Sleep(ms)
#else
#include <unistd.h>
#endif

// Converts the length of a byte buffer to the int32_t that every byte length in
// the C API takes. Returns 0 with the length stored in *out, or -1 with
// OverflowError("<what> exceeds 2 GiB") set when it does not fit: the largest
// length that does is INT32_MAX, one byte short of 2 GiB. A plain (int32_t)
// cast would instead wrap a 2 GiB buffer to a negative length, which the C API
// reads as null or empty, and a buffer of 4 GiB + n bytes to n, truncating it.
static int checked_len_to_int32(Py_ssize_t len, const char* what, int32_t* out) {
    if (len > INT32_MAX) {
        PyErr_Format(PyExc_OverflowError, "%s exceeds 2 GiB", what);
        return -1;
    }
    *out = (int32_t)len;
    return 0;
}

// ---- calling the C API -----------------------------------------------------
//
// Only confluent_kafka_bindings.h is included. It declares every function that
// can fail (and every `_cb` function) under its own name, and every other
// function f as its twin f_err: both take a trailing
// `kafka_common_Error_t **out_err`, which must point at NULL on entry, and
// store an owned error there on failure, a Rust panic caught at the boundary
// included, returning the value otherwise. One slot per function, `ffi_err`,
// serves every call, through `ffi_take`, which hands the error out and resets
// the slot, so a call reads
//
//     if (ffi_raise((v = f(args, &ffi_err), ffi_take(&ffi_err)))) <the caller's error path>;
//
// and a panic raises IllegalStateError where the plain function would have
// returned a fallback (NULL, 0, -1) that is often a valid answer.

// The error a call stored in `*slot` (NULL if none), resetting the slot to
// NULL: a `kafka_common_Error_t **out_err` must point at NULL on entry, so one
// slot serves every call of a function. `(v = f(args, &ffi_err),
// ffi_take(&ffi_err))` is then an expression for the call's error.
static kafka_common_Error_t* ffi_take(kafka_common_Error_t** slot) {
    kafka_common_Error_t* e = *slot;
    *slot = NULL;
    return e;
}

// An error factory's twin returns the error it built, or NULL with the caught
// panic in `*slot`: either way one error, which the caller owns.
static kafka_common_Error_t* ffi_built(kafka_common_Error_t* built, kafka_common_Error_t** slot) {
    kafka_common_Error_t* panic = ffi_take(slot);
    return built != NULL ? built : panic;
}

// Takes the GIL for a callback the Rust client fires, unless the interpreter
// is shutting down. Returns 1 with the GIL held (release it with
// PyGILState_Release), or 0 without it, and the callback must then return at
// once without touching Python. A callback can fire after the client was
// destroyed (an in-flight close still completes), and so after Python began
// exiting, when PyGILState_Ensure would end the calling thread (or, from 3.14,
// hang it). This only narrows that window, since finalization can still start
// between the check and the Ensure: CPython offers no way to take the GIL that
// fails instead. Whatever the skipped callback owns is leaked, since a Python
// reference cannot be dropped without the GIL. A thread that already holds the
// GIL, such as one tearing the client down that runs a callback inline, is let
// through even mid-shutdown, because re-entering the GIL there is safe.
static int gil_ensure_unless_finalizing(PyGILState_STATE* gstate) {
#if PY_VERSION_HEX >= 0x030D0000
    int finalizing = Py_IsFinalizing();
#else
    int finalizing = _Py_IsFinalizing();  // the only spelling before 3.13
#endif
    if (!PyGILState_Check() && (!Py_IsInitialized() || finalizing)) {
        return 0;
    }
    *gstate = PyGILState_Ensure();
    return 1;
}

// The report of a user callback that could not run because the interpreter is
// shutting down: a foreign callback error owning nothing, so the operation
// waiting for the report fails as if the callback had raised rather than going
// on as if it had returned. Built without the GIL.
static kafka_common_Error_t* interpreter_finalizing_error(const char* what) {
    kafka_common_Error_t *ffi_err = NULL;
    char msg[160];
    snprintf(msg, sizeof(msg), "the %s cannot run: the Python interpreter is shutting down", what);
    return ffi_built(kafka_common_Error_local_callback_err(msg, NULL, NULL, &ffi_err), &ffi_err);
}

// Frees `e`, when it is not NULL, without the GIL: for the error a report
// returned on a path that holds no GIL (gil_ensure_unless_finalizing gave 0),
// where nothing can be raised or written. A panic while freeing it leaks the
// error reporting that panic, as in ffi_error_object.
static void ffi_discard_without_gil(kafka_common_Error_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    if (e == NULL) return;
    (void)(kafka_common_Error_destroy_err(e, &ffi_err), ffi_take(&ffi_err));
}

// The Python error for the owned error `e`: `confluent_kafka._errors.
// from_ffi_error`, which builds the class of its FFI id with its payload and
// cause chain. NULL with the conversion's own exception set when it fails.
// Frees `e` either way. GIL held.
static PyObject* ffi_error_object(kafka_common_Error_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    static PyObject* from_ffi_error = NULL;
    PyObject* exc = NULL;
    if (from_ffi_error == NULL) {
        PyObject* module = PyImport_ImportModule("confluent_kafka._errors");
        if (module != NULL) {
            from_ffi_error = PyObject_GetAttrString(module, "from_ffi_error");
            Py_DECREF(module);
        }
    }
    if (from_ffi_error != NULL) {
        PyObject* handle = PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
        PyObject* args = handle ? PyTuple_Pack(1, handle) : NULL;
        PyObject* kwargs = args ? Py_BuildValue("{s:O}", "owned", Py_False) : NULL;
        if (kwargs != NULL) exc = PyObject_Call(from_ffi_error, args, kwargs);
        Py_XDECREF(kwargs);
        Py_XDECREF(args);
        Py_XDECREF(handle);
    }
    // Freed here rather than by from_ffi_error, so a conversion that fails
    // midway still frees it. A panic while freeing an error leaks the error
    // reporting it: freeing that one could panic the same way.
    (void)(kafka_common_Error_destroy_err(e, &ffi_err), ffi_take(&ffi_err));
    return exc;
}

static void ffi_unraisable(kafka_common_Error_t* e);

// 0 when `e` is NULL; otherwise raises `e` as its Python error, frees it and
// returns 1. An exception already set is kept, and `e` is reported as
// unraisable instead. GIL held.
static int ffi_raise(kafka_common_Error_t* e) {
    if (e == NULL) return 0;
    if (PyErr_Occurred()) {
        ffi_unraisable(e);
        return 1;
    }
    PyObject* exc = ffi_error_object(e);
    if (exc != NULL) {
        PyErr_SetObject((PyObject*)Py_TYPE(exc), exc);
        Py_DECREF(exc);
    }
    return 1;
}

// For a release or a report on a path that cannot fail any more (a cleanup
// path, a destructor, an error path already raising): writes `e`, when it is
// not NULL, with PyErr_WriteUnraisable and frees it, leaving the exception
// already set, if any, in place. GIL held.
static void ffi_unraisable(kafka_common_Error_t* e) {
    if (e == NULL) return;
    PyObject *type, *value, *tb;
    PyErr_Fetch(&type, &value, &tb);
    PyObject* exc = ffi_error_object(e);
    if (exc != NULL) {
        PyErr_SetObject((PyObject*)Py_TYPE(exc), exc);
        Py_DECREF(exc);
    }
    PyErr_WriteUnraisable(NULL);
    PyErr_Restore(type, value, tb);
}

// One header of a ProducerRecordObject: the key points into the key str's
// cached UTF-8 buffer, the value into the exported value buffer (NULL with
// len -1 for a null value).
typedef struct {
    const char* key;
    const uint8_t* value;
    int32_t value_len;
} ProducerRecordHeader;

// ProducerRecord C extension type: the serialized record a send reads.
//
// The key and value cross the C API as `kafka_Bytes_t *` (NULL serializers),
// so the record holds the two `kafka_Bytes_t` the send path points the C
// record at: they point into the held `bytes` objects, which are never copied
// (CLAUDE.md §14). They must stay valid until the send's completion fires,
// which the send path guarantees by holding this object until then.
typedef struct {
    PyObject_HEAD
    PyObject* key;          // PyBytesObject or Py_None
    PyObject* value;        // PyBytesObject or Py_None (null value = tombstone)
    kafka_Bytes_t key_b;    // into `key`; data NULL for a null key
    kafka_Bytes_t value_b;  // into `value`; data NULL for a null value
    // The topic str, held so `topic` can point into its cached, NUL-terminated
    // UTF-8 buffer for the record's lifetime: no copy of the topic per record.
    PyObject* topic_obj;
    const char* topic;
    int32_t partition;      // -1 = unset (Java null)
    int64_t timestamp;      // -1 = unset (Java null)
    // Headers: `header_seq` is a tuple of (str, buffer|None) pairs (the given
    // tuple itself, or a tuple snapshot of another sequence), held so each key
    // str, and so its cached UTF-8 buffer the entries point into, lives as long
    // as the record. `header_views` holds one buffer export (PyObject_GetBuffer)
    // per non-None value, so the value bytes stay valid, and a memoryview value
    // cannot be released, while the record lives. Parallel, length
    // `header_count`.
    PyObject* header_seq;
    Py_buffer* header_views;
    ProducerRecordHeader* header_entries;
    int32_t header_count;
} ProducerRecordObject;

static int ProducerRecord_traverse(ProducerRecordObject* self, visitproc visit, void* arg) {
    Py_VISIT(self->key);
    Py_VISIT(self->value);
    Py_VISIT(self->topic_obj);
    Py_VISIT(self->header_seq);
    return 0;
}

static int ProducerRecord_clear(ProducerRecordObject* self) {
    Py_CLEAR(self->key);
    Py_CLEAR(self->value);
    self->key_b.data = NULL;
    self->key_b.len = -1;
    self->value_b.data = NULL;
    self->value_b.len = -1;
    self->topic = NULL;
    Py_CLEAR(self->topic_obj);
    if (self->header_views != NULL) {
        // A zeroed Py_buffer (a None value, or one never exported) has no
        // obj, and PyBuffer_Release ignores it.
        for (int32_t i = 0; i < self->header_count; i++) {
            PyBuffer_Release(&self->header_views[i]);
        }
        PyMem_Free(self->header_views);
        self->header_views = NULL;
    }
    PyMem_Free(self->header_entries);
    self->header_entries = NULL;
    self->header_count = 0;
    // The keys the entries pointed into go with the header tuple.
    Py_CLEAR(self->header_seq);
    return 0;
}

static void ProducerRecord_dealloc(ProducerRecordObject* self) {
    PyObject_GC_UnTrack(self);
    ProducerRecord_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject* ProducerRecord_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    ProducerRecordObject* self;
    self = PyObject_GC_New(ProducerRecordObject, type);
    if (self != NULL) {
        self->key = NULL;
        self->value = NULL;
        self->key_b.data = NULL;
        self->key_b.len = -1;
        self->value_b.data = NULL;
        self->value_b.len = -1;
        self->topic_obj = NULL;
        self->topic = NULL;
        self->partition = -1;
        self->timestamp = -1;
        self->header_seq = NULL;
        self->header_views = NULL;
        self->header_entries = NULL;
        self->header_count = 0;
    }
    return (PyObject*)self;
}

// The NUL-terminated UTF-8 buffer str `obj` caches (valid while `obj` lives),
// or NULL with ValueError for an embedded NUL, which a C string cannot carry
// (as PyArg_Parse's "s" rejects it).
static const char* str_utf8_borrowed(PyObject* obj) {
    Py_ssize_t size = 0;
    const char* utf8 = PyUnicode_AsUTF8AndSize(obj, &size);
    if (utf8 == NULL) {
        return NULL;
    }
    if ((Py_ssize_t)strlen(utf8) != size) {
        PyErr_SetString(PyExc_ValueError, "embedded null character");
        return NULL;
    }
    return utf8;
}

// Build the header entries from a Python sequence of (str, buffer|None) pairs.
// The pairs are held as a tuple (the given tuple, or a snapshot of another
// sequence), so each key str lives as long as the record and its entry points
// into the str's cached UTF-8 buffer. Each non-None value is exported with
// PyObject_GetBuffer (C-contiguous), and the export is held until clear(): a
// released memoryview is rejected (ValueError) instead of read after its
// buffer is freed. Returns 0 on success, -1 with a Python exception set on
// failure; everything allocated or exported so far is owned by the record, so
// clear() releases it.
//
// The entries are what the send path hands to
// `kafka_common_header_Headers_add_with_key_value_err`, which copies them into the
// C record's headers: the C API has no borrowed form for a header value.
static int producer_record_build_headers(ProducerRecordObject* self, PyObject* headers) {
    if (headers == NULL || headers == Py_None) {
        return 0;
    }
    PyObject* seq;
    if (PyTuple_CheckExact(headers)) {
        Py_INCREF(headers);
        seq = headers;
    } else {
        PyObject* fast = PySequence_Fast(headers, "headers must be a sequence of (str, bytes) pairs");
        if (fast == NULL) return -1;
        seq = PySequence_Tuple(fast);
        Py_DECREF(fast);
        if (seq == NULL) return -1;
    }
    Py_ssize_t n = PyTuple_GET_SIZE(seq);
    if (n == 0) { Py_DECREF(seq); return 0; }
    int32_t count;
    if (checked_len_to_int32(n, "header count", &count) < 0) { Py_DECREF(seq); return -1; }

    Py_buffer* views = PyMem_Calloc((size_t)n, sizeof(Py_buffer));
    ProducerRecordHeader* entries = PyMem_Calloc((size_t)n, sizeof(ProducerRecordHeader));
    if (views == NULL || entries == NULL) {
        PyMem_Free(views); PyMem_Free(entries); Py_DECREF(seq);
        PyErr_NoMemory();
        return -1;
    }
    // Owned by the record from here on: clear() releases the views of the first
    // header_count entries (the arrays are zeroed) and drops the tuple.
    self->header_seq = seq;
    self->header_views = views;
    self->header_entries = entries;
    self->header_count = count;

    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* pair = PyTuple_GET_ITEM(seq, i);  // borrowed, held by seq
        PyObject* k = NULL;
        PyObject* v = NULL;
        // (key: str, value: buffer|None)
        if (!PyArg_ParseTuple(pair, "UO", &k, &v)) return -1;
        // The pair tuple, held by seq, holds k.
        const char* key = str_utf8_borrowed(k);
        if (key == NULL) return -1;
        entries[i].key = key;
        if (v == Py_None) {
            entries[i].value = NULL;
            entries[i].value_len = -1;  // null header value (Java allows it)
            continue;
        }
        if (!PyObject_CheckBuffer(v)) {
            PyErr_SetString(PyExc_TypeError,
                            "header value must be bytes, a contiguous memoryview or None");
            return -1;
        }
        // Raises ValueError for a released memoryview and BufferError for a
        // non-contiguous one.
        if (PyObject_GetBuffer(v, &views[i], PyBUF_C_CONTIGUOUS) != 0) return -1;
        // views[i] is held from here: clear() releases it with the others.
        if (checked_len_to_int32(views[i].len, "header value", &entries[i].value_len) < 0) return -1;
        entries[i].value = (const uint8_t*)views[i].buf;
    }
    return 0;
}

static int ProducerRecord_init(ProducerRecordObject* self, PyObject* args, PyObject* kwds) {
    static char* kwlist[] = {"topic", "value", "key", "partition", "timestamp",
                             "headers", NULL};
    PyObject* topic_obj = NULL;
    PyObject* value = NULL;
    PyObject* key = Py_None;
    int partition = -1;
    long long timestamp = -1;
    PyObject* headers = Py_None;

    // A re-run __init__ starts from an empty record.
    ProducerRecord_clear(self);

    // "U": the topic str itself (a None topic is a TypeError), whose UTF-8
    // buffer the record borrows below.
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "UO|OiLO", kwlist,
                                      &topic_obj, &value, &key, &partition, &timestamp,
                                      &headers)) {
        return -1;
    }
    const char* topic = str_utf8_borrowed(topic_obj);
    if (topic == NULL) {
        return -1;
    }

    // Value: bytes or None. A null value is a Java tombstone (value_len == -1).
    if (value != Py_None && !PyBytes_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "Value must be bytes or None");
        return -1;
    }
    if (key != Py_None && !PyBytes_Check(key)) {
        PyErr_SetString(PyExc_TypeError, "Key must be bytes or None");
        return -1;
    }
    if (partition < -1) {
        PyErr_Format(PyExc_ValueError,
                     "Invalid partition: %d. Partition number should always be non-negative or null.",
                     partition);
        return -1;
    }
    if (timestamp < -1) {
        PyErr_Format(PyExc_ValueError,
                     "Invalid timestamp: %lld. Timestamp should always be non-negative or null.",
                     timestamp);
        return -1;
    }

    // Check the byte lengths before anything is stored, so a rejected record
    // stays empty.
    int32_t key_len = -1;
    if (key != Py_None && checked_len_to_int32(PyBytes_GET_SIZE(key), "key", &key_len) < 0) {
        return -1;
    }
    int32_t value_len = -1;
    if (value != Py_None && checked_len_to_int32(PyBytes_GET_SIZE(value), "value", &value_len) < 0) {
        return -1;
    }

    Py_INCREF(key);
    self->key = key;
    Py_INCREF(value);
    self->value = value;
    if (key != Py_None) {
        self->key_b.data = (const uint8_t*)PyBytes_AS_STRING(key);
        self->key_b.len = key_len;
    }
    if (value != Py_None) {
        self->value_b.data = (const uint8_t*)PyBytes_AS_STRING(value);
        self->value_b.len = value_len;
    }
    Py_INCREF(topic_obj);
    self->topic_obj = topic_obj;
    self->topic = topic;
    self->partition = (int32_t)partition;
    self->timestamp = (int64_t)timestamp;

    // Headers (optional). tp_clear frees whatever was allocated on failure.
    return producer_record_build_headers(self, headers);
}

// Getters
static PyObject* ProducerRecord_get_topic(ProducerRecordObject* self, void* closure) {
    if (self->topic_obj == NULL) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->topic_obj);
    return self->topic_obj;
}

static PyObject* ProducerRecord_get_partition(ProducerRecordObject* self, void* closure) {
    if (self->partition == -1) {
        Py_RETURN_NONE;
    }
    return PyLong_FromLong((long)self->partition);
}

static PyObject* ProducerRecord_get_timestamp(ProducerRecordObject* self, void* closure) {
    if (self->timestamp == -1) {
        Py_RETURN_NONE;
    }
    return PyLong_FromLongLong((long long)self->timestamp);
}

static PyObject* ProducerRecord_get_key(ProducerRecordObject* self, void* closure) {
    if (self->key == NULL) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->key);
    return self->key;
}

static PyObject* ProducerRecord_get_value(ProducerRecordObject* self, void* closure) {
    if (self->value == NULL) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->value);
    return self->value;
}

// The headers the send path reads, rebuilt from the entries (proving they are
// populated, not just the Python side): a list of (str, bytes|None) pairs, in
// insertion order.
static PyObject* ProducerRecord_get_headers(ProducerRecordObject* self, void* closure) {
    int32_t n = self->header_entries == NULL ? 0 : self->header_count;
    PyObject* out = PyList_New(n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        const ProducerRecordHeader* h = &self->header_entries[i];
        PyObject* v;
        if (h->value_len < 0 || h->value == NULL) {
            Py_INCREF(Py_None);
            v = Py_None;
        } else {
            v = PyBytes_FromStringAndSize((const char*)h->value, h->value_len);
            if (v == NULL) { Py_DECREF(out); return NULL; }
        }
        PyObject* pair = Py_BuildValue("(sN)", h->key ? h->key : "", v);  // steals v
        if (pair == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, pair);  // steals pair
    }
    return out;
}

// The value length the send path passes (-1 = null value / tombstone).
static PyObject* ProducerRecord_get_value_len(ProducerRecordObject* self, void* closure) {
    return PyLong_FromLong((long)self->value_b.len);
}

// Test-only: the addresses the send path reads -- (topic, key, value,
// [(header_key, header_value), ...]), 0 for NULL -- so a test can show they are
// the Python objects' own buffers, not copies.
static PyObject* ProducerRecord_get_ffi_addresses(ProducerRecordObject* self, void* closure) {
    int32_t n = self->header_entries == NULL ? 0 : self->header_count;
    PyObject* headers = PyList_New(n);
    if (headers == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        const ProducerRecordHeader* h = &self->header_entries[i];
        PyObject* pair = Py_BuildValue("(KK)",
            (unsigned long long)(uintptr_t)h->key,
            (unsigned long long)(uintptr_t)h->value);
        if (pair == NULL) { Py_DECREF(headers); return NULL; }
        PyList_SET_ITEM(headers, i, pair);  // steals pair
    }
    return Py_BuildValue("(KKKN)",
        (unsigned long long)(uintptr_t)self->topic,
        (unsigned long long)(uintptr_t)self->key_b.data,
        (unsigned long long)(uintptr_t)self->value_b.data,
        headers);  // "N" steals headers
}

static PyGetSetDef ProducerRecord_getsetters[] = {
    {"topic", (getter)ProducerRecord_get_topic, NULL, "Topic name", NULL},
    {"partition", (getter)ProducerRecord_get_partition, NULL, "Partition number", NULL},
    {"timestamp", (getter)ProducerRecord_get_timestamp, NULL, "Timestamp", NULL},
    {"key", (getter)ProducerRecord_get_key, NULL, "Message key", NULL},
    {"value", (getter)ProducerRecord_get_value, NULL, "Message value", NULL},
    {"headers", (getter)ProducerRecord_get_headers, NULL,
     "Headers the send path reads, as (str, bytes|None) pairs", NULL},
    {"value_len", (getter)ProducerRecord_get_value_len, NULL,
     "Value length the send path passes (-1 = null value/tombstone)", NULL},
    {"ffi_addresses", (getter)ProducerRecord_get_ffi_addresses, NULL,
     "Test-only: (topic, key, value, [(header_key, header_value)]) addresses the "
     "send path reads", NULL},
    {NULL}
};

static PyTypeObject ProducerRecordType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "kafkanative.ProducerRecord",
    .tp_doc = "Producer record for Kafka messages",
    .tp_basicsize = sizeof(ProducerRecordObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_new = ProducerRecord_new,
    .tp_init = (initproc)ProducerRecord_init,
    .tp_dealloc = (destructor)ProducerRecord_dealloc,
    .tp_traverse = (traverseproc)ProducerRecord_traverse,
    .tp_clear = (inquiry)ProducerRecord_clear,
    .tp_getset = ProducerRecord_getsetters,
};

// ---- ConsumerGroupMetadata: owns the Rust group-metadata handle -------------
//
// Java's ConsumerGroupMetadata carries exactly four fields (group id, generation
// id, member id, group instance id). The Rust consumer FFI hands out an opaque
// handle that carries those fields and must be fed back into
// kafka_producer_Producer_send_offsets_to_transaction_err, then freed exactly once.
// Modelled on ProducerRecordType (a value object that also carries C state and
// is passed into a call) and ConsumerRecordsType (a raw-handle owner): a proper
// extension type that frees its handle in tp_dealloc on GC — NOT a Python
// __del__ (see producer-transactions-python-plan.md §6.1: __del__ is less
// reliable — interpreter-shutdown ordering, non-prompt collection, swallowed
// exceptions). Each Consumer.group_metadata() call returns a fresh owned handle
// (the FFI clones internally), so two objects never alias one handle.
//
// Consumer.group_metadata() is the only way to get one: Java deprecated the
// ConsumerGroupMetadata constructors in 4.2 (the class becomes an interface in
// 5.0), and the C API has none, so the type disallows instantiation. That also
// guarantees `handle` is never NULL outside dealloc.
typedef struct {
    PyObject_HEAD
    kafka_consumer_ConsumerGroupMetadata_t* handle;  // owned; freed in dealloc
} ConsumerGroupMetadataObject;

static void ConsumerGroupMetadata_dealloc(ConsumerGroupMetadataObject* self) {
    kafka_common_Error_t *ffi_err = NULL;
    // Double-free guard mirrors ConsumerRecords_dealloc.
    if (self->handle != NULL) {
        ffi_unraisable((kafka_consumer_ConsumerGroupMetadata_destroy_err(self->handle, &ffi_err), ffi_take(&ffi_err)));
        self->handle = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* ConsumerGroupMetadata_get_group_id(ConsumerGroupMetadataObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    if (self->handle == NULL) Py_RETURN_NONE;
    const char* s = NULL;
    if (ffi_raise((s = kafka_consumer_ConsumerGroupMetadata_group_id_err(self->handle, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyUnicode_FromString(s ? s : "");
}

static PyObject* ConsumerGroupMetadata_get_generation_id(ConsumerGroupMetadataObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    if (self->handle == NULL) Py_RETURN_NONE;
    int32_t generation_id = 0;
    if (ffi_raise((generation_id = kafka_consumer_ConsumerGroupMetadata_generation_id_err(self->handle, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLong(generation_id);
}

static PyObject* ConsumerGroupMetadata_get_member_id(ConsumerGroupMetadataObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    if (self->handle == NULL) Py_RETURN_NONE;
    const char* s = NULL;
    if (ffi_raise((s = kafka_consumer_ConsumerGroupMetadata_member_id_err(self->handle, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyUnicode_FromString(s ? s : "");
}

static PyObject* ConsumerGroupMetadata_get_group_instance_id(ConsumerGroupMetadataObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    if (self->handle == NULL) Py_RETURN_NONE;
    const char* s = NULL;
    if (ffi_raise((s = kafka_consumer_ConsumerGroupMetadata_group_instance_id_err(self->handle, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (s == NULL) Py_RETURN_NONE;  // absent static instance id -> None (Java's Optional.empty)
    return PyUnicode_FromString(s);
}

static PyGetSetDef ConsumerGroupMetadata_getsetters[] = {
    {"group_id", (getter)ConsumerGroupMetadata_get_group_id, NULL, "Consumer group id", NULL},
    {"generation_id", (getter)ConsumerGroupMetadata_get_generation_id, NULL, "Generation id", NULL},
    {"member_id", (getter)ConsumerGroupMetadata_get_member_id, NULL, "Member id", NULL},
    {"group_instance_id", (getter)ConsumerGroupMetadata_get_group_instance_id, NULL,
     "Static group instance id, or None", NULL},
    {NULL}
};

// repr reproduces the former pure-Python dataclass output verbatim:
// ConsumerGroupMetadata(group_id='...', generation_id=N, member_id='...', group_instance_id=...)
// (%R = repr -> quoted strings / None; %S = str -> the plain generation number.)
static PyObject* ConsumerGroupMetadata_repr(ConsumerGroupMetadataObject* self) {
    PyObject* gid = ConsumerGroupMetadata_get_group_id(self, NULL);
    PyObject* gen = ConsumerGroupMetadata_get_generation_id(self, NULL);
    PyObject* mid = ConsumerGroupMetadata_get_member_id(self, NULL);
    PyObject* inst = ConsumerGroupMetadata_get_group_instance_id(self, NULL);
    PyObject* r = NULL;
    if (gid && gen && mid && inst) {
        r = PyUnicode_FromFormat(
            "ConsumerGroupMetadata(group_id=%R, generation_id=%S, member_id=%R, group_instance_id=%R)",
            gid, gen, mid, inst);
    }
    Py_XDECREF(gid); Py_XDECREF(gen); Py_XDECREF(mid); Py_XDECREF(inst);
    return r;
}

static PyTypeObject ConsumerGroupMetadataType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_confluentkafka.ConsumerGroupMetadata",
    .tp_doc = "Consumer group membership metadata (owns a live Rust handle)",
    .tp_basicsize = sizeof(ConsumerGroupMetadataObject),
    .tp_itemsize = 0,
    // No constructor; see the type's comment.
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_dealloc = (destructor)ConsumerGroupMetadata_dealloc,
    .tp_getset = ConsumerGroupMetadata_getsetters,
    .tp_repr = (reprfunc)ConsumerGroupMetadata_repr,
};

// ===========================================================================
// Producer -- marshaling-only bridge to the Rust producer FFI.
//
// The C layer runs NO background threads of its own and holds no business
// logic. Both Python producer families drive the `_cb` twins of the
// blocking-in-Java operations (`send_with_callback_cb`, `flush_cb`,
// `partitions_for_cb`, `close_cb` / `close_with_timeout_cb`, the
// transaction-control `_cb` ops); the blocking entry points (`Producer_send`,
// `Producer_flush`, ...) stay exported for completeness but neither family
// calls them.
// Delivery callbacks and `_cb` completions are QUEUED by Rust on the
// producer's callback vector and only run when this extension calls
// `kafka_producer_Producer__execute_callbacks_err` (Producer_poll /
// Producer__execute_callbacks / Producer_destroy), on the calling thread:
//
//   * `KafkaProducer` runs them on its own Python completion thread, a loop of
//     `Producer_poll(handle, 100)` slices (drain, wait on the condvar below
//     with the GIL released, drain again). Its waiting calls (flush, the
//     transaction-control ops, close) wait on a Python event their completion
//     sets, in 100 ms slices, so Ctrl-C is honoured between two slices.
//   * `AsyncKafkaProducer` registers a Python notify callable that does
//     nothing but `loop.call_soon_threadsafe(pump)`; the pump calls
//     `Producer__execute_callbacks` on the event loop.
//
// The hook installed with `kafka_producer_Producer__set_callbacks_notify_err` fires
// from a Rust task once per empty->non-empty transition of that vector. It
// only signals: it sets `notified` under `mtx` and broadcasts `cnd` (what the
// `Producer_poll` slices wait on) and, when one is set, invokes the Python
// notify callable. It never calls back into the C API.
//
// Ownership conventions (CLAUDE.md §4): errors cross into Python as the
// address of the `kafka_common_Error_t` (an int, 0 = success). An error a
// completion or a blocking call hands over is OWNED by Python, which reads it
// through the `KafkaError_*` accessors and frees it with `KafkaError_destroy`;
// the error a delivery callback receives is BORROWED for the duration of the
// call (converted, never destroyed), and so is the one `KafkaError_source`
// returns. Metadata crosses as a plain tuple (`metadata_to_py`), copied out of
// the borrowed `RecordMetadata_t`.
// ===========================================================================

// Defined in the consumer section below; the producer's partitions_for reuses
// it (the FFI returns the same kafka_common_PartitionInfo_t).
static PyObject* partition_info_to_py(const kafka_common_PartitionInfo_t* info);

// The int Python receives for an error handle: its address, 0 for none.
static PyObject* error_handle_to_py(const kafka_common_Error_t* e) {
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// A `kafka_producer_Callback_t` implementation: Java's `Callback`. The `void
// *self` Rust hands back is this struct. It has two owners: the delivery
// callback (released when `on_completion` fires) and the send's completion
// (released when it resolves). A send that resolved with an error may still
// fire the callback, once, later: Java's doSend appends the record, whose
// appendCallbacks carry the Callback, before maybeAddPartition throws
// (KafkaProducer.java:1027-1039), so a record that had reached the accumulator
// completes with its batch; one rejected before the append never fires. Which
// one it was cannot be told from here, so the context is then parked on the
// producer's orphan list and freed at destroy, after `_destroy` ran every
// still-pending callback: a late `on_completion` still finds it alive, and
// releases its callable when it has run.
typedef struct DeliveryCtx {
    PyObject* callable;        // Python callable(meta_tuple | None, borrowed_error_int, callback_id)
    const kafka_producer_Producer_t* producer;  // the view the callback reports through
    int refs;                  // owners left: the callback and the send completion
    int orphaned;              // on the orphan list: freed at destroy, not here
    struct DeliveryCtx* next;  // orphan-list link
} DeliveryCtx;

typedef struct {
    int is_mock;
    kafka_producer_KafkaProducer_t* kp;         // owned class handle (real producer)
    kafka_producer_MockProducer_t* mp;          // owned class handle (mock)
    const kafka_producer_Producer_t* producer;  // borrowed __as_Producer view of the above
    mtx_t mtx;
    cnd_t cnd;
    int notified;              // set by the notify hook, consumed by Producer_poll
    PyObject* notify_cb;       // asyncio: scheduler callable; NULL for the sync producer
    DeliveryCtx* orphans;      // delivery contexts whose send failed (GIL-guarded)
} Producer;

static inline Producer* producer_from_handle(unsigned long long ptr) {
    return (Producer*)(uintptr_t)ptr;
}

// ---- error / metadata -> Python tuples -------------------------------------

// The exception object of the first foreign callback error in `e`'s cause
// chain, `e` included, as a BORROWED reference in `*out`, or NULL there; returns
// 0, or -1 with the exception set when a call fails. A Python callback that
// raised reports its exception object as that error's opaque pointer, the
// error owning one reference to it (see `callback_error_from_py`), so the
// object lives as long as the handle `e` it is read from; the client wraps the
// error as Java wraps a listener's foreign Throwable, so it usually comes back
// as the cause. Every foreign callback error in this process is built by
// `callback_error_from_py`, so a non-NULL opaque is always such an object.
static int callback_exception_of(const kafka_common_Error_t* e, PyObject** out) {
    kafka_common_Error_t *ffi_err = NULL;
    *out = NULL;
    const kafka_common_Error_t* cur = e;
    while (cur != NULL) {
        const kafka_common_LocalCallbackError_t* cb = NULL;
        if (ffi_raise((cb = kafka_common_Error_local_callback_error_err(cur, &ffi_err), ffi_take(&ffi_err)))) return -1;
        if (cb != NULL) {
            void* opaque = NULL;
            if (ffi_raise((opaque = kafka_common_LocalCallbackError_opaque_err(cb, &ffi_err), ffi_take(&ffi_err)))) return -1;
            *out = (PyObject*)opaque;
            return 0;
        }
        const kafka_common_Error_t* source = NULL;
        if (ffi_raise((source = kafka_common_Error_source_err(cur, &ffi_err), ffi_take(&ffi_err)))) return -1;
        cur = source;
    }
    return 0;
}

// Mirrors KafkaError._from_c field for field, from a BORROWED error:
// (code, message | None, is_retriable, is_fatal, txn_requires_abort,
//  callback_exception | None). `callback_exception` is the exception object a
// Python callback raised (`callback_exception_of`), a new reference, which the
// consumer swaps back in.
// `error_is_fatal` (defined with the KafkaError accessors below) composes
// RequestUtils.isFatalException from the exported predicates: 1, 0, or -1 with
// the exception set. Used by the consumer and admin sections.
static int error_is_fatal(const kafka_common_Error_t* e);

static PyObject* error_to_py(const kafka_common_Error_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* msg = NULL;
    kafka_common_Error_e code = kafka_common_Error_UNKNOWN_SERVER_ERROR;
    int8_t retriable = 0;
    int8_t abortable = 0;
    PyObject* exc = NULL;
    if (ffi_raise((msg = kafka_common_Error_message_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((code = kafka_common_Error__enum_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((retriable = kafka_common_Error_is_retriable_error_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((abortable = kafka_common_Error_is_transaction_abortable_error_err(e, &ffi_err), ffi_take(&ffi_err)))
        || callback_exception_of(e, &exc) < 0) return NULL;
    int fatal = error_is_fatal(e);
    if (fatal < 0) return NULL;
    PyObject* py_msg = msg ? PyUnicode_FromString(msg) : (Py_INCREF(Py_None), Py_None);
    if (py_msg == NULL) return NULL;
    PyObject* py_exc = exc ? exc : Py_None;
    Py_INCREF(py_exc);
    return Py_BuildValue("(iNOOON)",
                         (int)code,
                         py_msg,
                         retriable ? Py_True : Py_False,
                         fatal ? Py_True : Py_False,
                         abortable ? Py_True : Py_False,
                         py_exc);
}

// Same, consuming an OWNED error.
static PyObject* owned_error_to_py(kafka_common_Error_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* out = error_to_py(e);
    ffi_unraisable((kafka_common_Error_destroy_err(e, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// Returns a new reference to a (value, error) pair where `value` is a borrowed
// reference and the error is an owned handle (NULL = None). Used by the
// KafkaFuture wrapper and the consumer section.
static PyObject* build_value_error(PyObject* value, kafka_common_Error_t* err) {
    PyObject* py_err = err ? owned_error_to_py(err) : (Py_INCREF(Py_None), Py_None);
    if (py_err == NULL) return NULL;
    return Py_BuildValue("(ON)", value, py_err);
}

// (topic, partition, offset, timestamp, serialized_key_size,
// serialized_value_size) from a BORROWED RecordMetadata; `offset` /
// `timestamp` are -1 when Java's hasOffset() / hasTimestamp() is false.
static PyObject* metadata_to_py(const kafka_producer_RecordMetadata_t* m) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* topic = NULL;
    int8_t has_offset = 0;
    int8_t has_timestamp = 0;
    int64_t offset = -1;
    int64_t timestamp = -1;
    int32_t partition = 0;
    int32_t key_size = 0;
    int32_t value_size = 0;
    if (ffi_raise((topic = kafka_producer_RecordMetadata_topic_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((has_offset = kafka_producer_RecordMetadata_has_offset_err(m, &ffi_err), ffi_take(&ffi_err)))
        || (has_offset && ffi_raise((offset = kafka_producer_RecordMetadata_offset_err(m, &ffi_err), ffi_take(&ffi_err))))
        || ffi_raise((has_timestamp = kafka_producer_RecordMetadata_has_timestamp_err(m, &ffi_err), ffi_take(&ffi_err)))
        || (has_timestamp && ffi_raise((timestamp = kafka_producer_RecordMetadata_timestamp_err(m, &ffi_err), ffi_take(&ffi_err))))
        || ffi_raise((partition = kafka_producer_RecordMetadata_partition_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((key_size = kafka_producer_RecordMetadata_serialized_key_size_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((value_size = kafka_producer_RecordMetadata_serialized_value_size_err(m, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return Py_BuildValue("(siLLii)",
                         topic ? topic : "",
                         (int)partition,
                         (long long)offset, (long long)timestamp,
                         (int)key_size,
                         (int)value_size);
}

// ---- delivery callback (kafka_producer_Callback_t) -------------------------

static DeliveryCtx* delivery_ctx_new(PyObject* callable, const kafka_producer_Producer_t* producer) {
    DeliveryCtx* ctx = (DeliveryCtx*)PyMem_Malloc(sizeof(DeliveryCtx));
    if (ctx == NULL) { PyErr_NoMemory(); return NULL; }
    Py_INCREF(callable);
    ctx->callable = callable;
    ctx->producer = producer;
    ctx->refs = 2;
    ctx->orphaned = 0;
    ctx->next = NULL;
    return ctx;
}

static void delivery_ctx_free(DeliveryCtx* ctx) {
    Py_XDECREF(ctx->callable);
    PyMem_Free(ctx);
}

// Drops one owner (GIL held); the last one frees the context unless it is on
// the orphan list, which destroy frees: an orphan only drops its callable, which
// nothing can call any more (the callback fires once).
static void delivery_ctx_release(DeliveryCtx* ctx) {
    if (--ctx->refs != 0) return;
    if (ctx->orphaned) Py_CLEAR(ctx->callable);
    else delivery_ctx_free(ctx);
}

// The send resolved with an error (GIL held): the callback fires later if the
// record had reached the accumulator, and never if it was rejected before, so
// the context goes to the orphan list unless it already fired.
static void producer_orphan_delivery(Producer* p, DeliveryCtx* ctx) {
    if (--ctx->refs == 0) {
        delivery_ctx_free(ctx);
        return;
    }
    ctx->orphaned = 1;
    ctx->next = p->orphans;
    p->orphans = ctx;
}

// Defined with the consumer's callbacks below.
static kafka_common_Error_t* callback_error_from_py(PyObject* exc);
static kafka_common_Error_t* callback_error_from_py_exception(void);

// Runs inside kafka_producer_Producer__execute_callbacks_err, i.e. on the thread
// that called Producer_poll / Producer__execute_callbacks / Producer_destroy
// or a blocking flush / close / commit / abort / future get running the queue
// while it waits, with the GIL released by that wrapper. Both arguments are
// BORROWED; exactly one is non-NULL except for a pre-accumulator rejection,
// which delivers the placeholder metadata (offset/partition -1) beside the
// error, as Java's `callback.onCompletion(nullMetadata, e)`. The Python
// callable gets the metadata tuple, the borrowed error's address (0 for
// none), which it must convert during the call and never destroy, and the
// callback id, and follows listener_fire's split:
//   * None  -> the callback returned: report now;
//   * True  -> it deferred (an `async def` callback scheduled on the loop, or
//              a hop to the record's loop) and reports through
//              Producer__set_callback_result itself;
//   * raise -> report the exception as a foreign callback error owning a
//              reference to it; the core logs it, as Java logs a throwing
//              onCompletion, and fails nothing.
// The record's future, and a flush / close / commit / abort waiting for the
// callback, completes on the report.
static void producer_on_completion(void* self_,
                                   const kafka_producer_RecordMetadata_t* metadata,
                                   const kafka_common_Error_t* error,
                                   int64_t callback_id) {
    kafka_common_Error_t *ffi_err = NULL;
    DeliveryCtx* ctx = (DeliveryCtx*)self_;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) {
        // `ctx` is leaked with the rest; the report still releases the waits.
        ffi_discard_without_gil((kafka_producer_Producer__set_callback_result_err(ctx->producer, callback_id, NULL,
            interpreter_finalizing_error("delivery callback"), &ffi_err), ffi_take(&ffi_err)));
        return;
    }
    kafka_common_Error_t* report = NULL;
    int deferred = 0;
    PyObject* meta = metadata ? metadata_to_py(metadata) : (Py_INCREF(Py_None), Py_None);
    PyObject* err = error_handle_to_py(error);
    PyObject* py_id = PyLong_FromLongLong((long long)callback_id);
    if (meta != NULL && err != NULL && py_id != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->callable, meta, err, py_id, NULL);
        if (r == NULL) {
            report = callback_error_from_py_exception();
        } else {
            deferred = (r == Py_True);
            Py_DECREF(r);
        }
    } else {
        PyErr_WriteUnraisable(ctx->callable);
    }
    Py_XDECREF(meta);
    Py_XDECREF(err);
    Py_XDECREF(py_id);
    if (!deferred) {
        ffi_unraisable((kafka_producer_Producer__set_callback_result_err(ctx->producer, callback_id, NULL, report, &ffi_err), ffi_take(&ffi_err)));
    }
    delivery_ctx_release(ctx);
    PyGILState_Release(g);
}

// ---- notify hook -----------------------------------------------------------

// Fired by Rust once each time the callback vector goes from empty to
// non-empty, from a Rust task (or from the calling thread when a call
// completes something synchronously). It only signals. `notify_cb` is read
// without the GIL: it is set once before any send can queue a completion and
// released only after the class handle is destroyed (no hook can fire past
// that point), so the pointer is stable here.
static void producer_callbacks_notify(void* opaque) {
    Producer* p = (Producer*)opaque;
    mtx_lock(&p->mtx);
    p->notified = 1;
    cnd_broadcast(&p->cnd);
    mtx_unlock(&p->mtx);
    if (p->notify_cb != NULL) {
        // asyncio: schedule the pump on the loop. PyGILState_Ensure is a no-op
        // when this thread already holds the GIL and re-acquires it when it is
        // the calling thread inside Py_BEGIN_ALLOW_THREADS.
        PyGILState_STATE g;
        if (!gil_ensure_unless_finalizing(&g)) return;
        PyObject* r = PyObject_CallNoArgs(p->notify_cb);
        if (r == NULL) PyErr_WriteUnraisable(p->notify_cb); else Py_DECREF(r);
        PyGILState_Release(g);
    }
}

// ---- construction / destruction --------------------------------------------

static Producer* producer_alloc(PyObject* notify_cb) {
    Producer* p = (Producer*)PyMem_Malloc(sizeof(Producer));
    if (p == NULL) { PyErr_NoMemory(); return NULL; }
    memset(p, 0, sizeof(*p));
    mtx_init(&p->mtx, mtx_plain);
    cnd_init(&p->cnd);
    if (notify_cb != NULL && notify_cb != Py_None) {
        Py_INCREF(notify_cb);
        p->notify_cb = notify_cb;
    }
    return p;
}

// Frees `p` once no callback can fire any more: its class handle destroyed, or
// never set. Reclaims the orphaned delivery contexts.
static void producer_free(Producer* p) {
    while (p->orphans != NULL) {
        DeliveryCtx* next = p->orphans->next;
        delivery_ctx_free(p->orphans);
        p->orphans = next;
    }
    Py_XDECREF(p->notify_cb);
    mtx_destroy(&p->mtx);
    cnd_destroy(&p->cnd);
    PyMem_Free(p);
}

// MockProducer_new(auto_complete: bool, notify_cb: callable | None) -> handle
static PyObject* py_MockProducer_new(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    int auto_complete;
    PyObject* notify_cb = Py_None;
    if (!PyArg_ParseTuple(args, "p|O", &auto_complete, &notify_cb)) return NULL;
    Producer* p = producer_alloc(notify_cb);
    if (p == NULL) return NULL;
    p->is_mock = 1;
    if (ffi_raise((p->mp = kafka_producer_MockProducer_with_auto_complete_err((int8_t)(auto_complete ? 1 : 0), &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((p->producer = kafka_producer_MockProducer__as_Producer_err(p->mp, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((kafka_producer_Producer__set_callbacks_notify_err(p->producer, producer_callbacks_notify, p, &ffi_err), ffi_take(&ffi_err)))) {
        if (p->mp != NULL) ffi_unraisable((kafka_producer_MockProducer_destroy_err(p->mp, &ffi_err), ffi_take(&ffi_err)));
        producer_free(p);
        return NULL;
    }
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)p);
}

// KafkaProducer_new(config: dict[str, str], notify_cb: callable | None)
//   -> (handle, error_int): the handle is 0 and the OWNED error non-zero when
// the config is rejected or the producer cannot be built.
static PyObject* py_KafkaProducer_new(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* config_dict;
    PyObject* notify_cb = Py_None;
    if (!PyArg_ParseTuple(args, "O!|O", &PyDict_Type, &config_dict, &notify_cb)) return NULL;

    // A C-built kafka_Map_t holds BORROWED char* -> char*: the strings stay
    // owned by the dict's str objects, which outlive this call.
    kafka_Map_t* props = NULL;
    if (ffi_raise((props = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_Check(key) ? str_utf8_borrowed(key) : NULL;
        const char* v = (k != NULL && PyUnicode_Check(value)) ? str_utf8_borrowed(value) : NULL;
        if (k == NULL || v == NULL || ffi_raise((kafka_Map_put_err(props, (void*)k, (void*)v, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_Map_destroy_err(props, &ffi_err), ffi_take(&ffi_err)));
            if (!PyErr_Occurred())
                PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
    }

    kafka_producer_ProducerConfig_t* config = NULL;
    kafka_common_Error_t* err = (config = kafka_producer_ProducerConfig_new(props, &ffi_err), ffi_take(&ffi_err));
    ffi_unraisable((kafka_Map_destroy_err(props, &ffi_err), ffi_take(&ffi_err)));
    kafka_producer_KafkaProducer_t* kp = NULL;
    if (err == NULL) {
        // NULL serializers: key and value cross as kafka_Bytes_t *.
        Py_BEGIN_ALLOW_THREADS
        err = (kp = kafka_producer_KafkaProducer_new(config, NULL, NULL, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
        ffi_unraisable((kafka_producer_ProducerConfig_destroy_err(config, &ffi_err), ffi_take(&ffi_err)));
    }
    if (err != NULL) {
        PyObject* py_err = error_handle_to_py(err);
        if (py_err == NULL) { ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        return Py_BuildValue("(iN)", 0, py_err);
    }

    Producer* p = producer_alloc(notify_cb);
    if (p == NULL) {
        ffi_unraisable((kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err), ffi_take(&ffi_err)));
        return NULL;
    }
    p->kp = kp;
    if (ffi_raise((p->producer = kafka_producer_KafkaProducer__as_Producer_err(kp, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((kafka_producer_Producer__set_callbacks_notify_err(p->producer, producer_callbacks_notify, p, &ffi_err), ffi_take(&ffi_err)))) {
        kafka_common_Error_t* destroy_err;
        Py_BEGIN_ALLOW_THREADS
        destroy_err = (kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
        ffi_unraisable(destroy_err);
        producer_free(p);
        return NULL;
    }
    return Py_BuildValue("(Ki)", (unsigned long long)(uintptr_t)p, 0);
}

// Producer_close(handle) -> error_int. Java close(): blocks until the
// in-flight records are delivered. Does not free anything.
static PyObject* py_Producer_close(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (kafka_producer_Producer_close(p->producer, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    return error_handle_to_py(err);
}

// Producer_destroy(handle): frees the class handle (which stops the submission
// task, waits for the `_cb` tasks and runs every still-pending callback exactly
// once -- on this thread) and then the C struct. Close first for a graceful
// close. No other call may use the handle meanwhile or afterwards.
static PyObject* py_Producer_destroy(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (p->is_mock) err = (kafka_producer_MockProducer_destroy_err(p->mp, &ffi_err), ffi_take(&ffi_err));
    else err = (kafka_producer_KafkaProducer_destroy_err(p->kp, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    // The handle is freed even when its destroy panicked.
    ffi_unraisable(err);
    // No callback can fire past this point.
    producer_free(p);
    Py_RETURN_NONE;
}

// ---- callback pump ---------------------------------------------------------

// Producer__execute_callbacks(handle) -> int: runs the queued callbacks on this
// thread (with the GIL released around the drain; each callback re-acquires
// it) and returns how many ran.
static PyObject* py_Producer__execute_callbacks(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    int32_t n = 0;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (n = kafka_producer_Producer__execute_callbacks_err(p->producer, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (ffi_raise(err)) return NULL;
    return PyLong_FromLong((long)n);
}

// Producer__set_callback_result(handle, callback_id, exception | None): the
// deferred report of a delivery callback (see producer_on_completion). None =
// the callback returned; an exception object = the exception it raised,
// reported as a foreign callback error owning a reference to it, which the
// core logs. Java's onCompletion is void, so the C `result` is NULL. Must not
// be called once Producer_destroy began, except from a callback it runs.
static PyObject* py_Producer__set_callback_result(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr; long long callback_id; PyObject* error;
    if (!PyArg_ParseTuple(args, "KLO", &ptr, &callback_id, &error)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err = NULL;
    if (PyExceptionInstance_Check(error)) {
        err = callback_error_from_py(error);
    } else if (error != Py_None) {
        PyErr_SetString(PyExc_TypeError, "Producer__set_callback_result: expected an exception or None");
        return NULL;
    }
    if (ffi_raise((kafka_producer_Producer__set_callback_result_err(p->producer, (int64_t)callback_id, NULL, err, &ffi_err), ffi_take(&ffi_err)))) {
        return NULL;
    }
    Py_RETURN_NONE;
}

// Producer_poll(handle, timeout_ms) -> int: the sync producer's pump. Runs the
// queued callbacks; when none were queued and timeout_ms != 0, waits on the
// notify condition (up to timeout_ms, forever when negative) and drains again.
// `notified` is cleared BEFORE the first drain so a notification that races
// the drain is not lost, and a notification already consumed by the drain
// costs at most one spurious early return.
static PyObject* py_Producer_poll(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KL", &ptr, &timeout_ms)) return NULL;
    Producer* p = producer_from_handle(ptr);
    int32_t n = 0;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    mtx_lock(&p->mtx);
    p->notified = 0;
    mtx_unlock(&p->mtx);
    err = (n = kafka_producer_Producer__execute_callbacks_err(p->producer, &ffi_err), ffi_take(&ffi_err));
    if (err == NULL && n == 0 && timeout_ms != 0) {
        mtx_lock(&p->mtx);
        if (timeout_ms < 0) {
            while (!p->notified) cnd_wait(&p->cnd, &p->mtx);
        } else {
            struct timespec ts;
            timespec_get(&ts, TIME_UTC);
            ts.tv_sec += (time_t)(timeout_ms / 1000);
            ts.tv_nsec += (long)((timeout_ms % 1000) * 1000000LL);
            if (ts.tv_nsec >= 1000000000L) { ts.tv_sec += 1; ts.tv_nsec -= 1000000000L; }
            while (!p->notified) {
                if (cnd_timedwait(&p->cnd, &p->mtx, &ts) == thrd_timedout) break;
            }
        }
        p->notified = 0;
        mtx_unlock(&p->mtx);
        err = (n = kafka_producer_Producer__execute_callbacks_err(p->producer, &ffi_err), ffi_take(&ffi_err));
    }
    Py_END_ALLOW_THREADS
    if (ffi_raise(err)) return NULL;
    return PyLong_FromLong((long)n);
}

// ---- KafkaFuture<RecordMetadata> wrapper -----------------------------------
//
// The owned kafka_common_KafkaFuture_t a blocking send returns. `get()`
// blocks with the GIL released (driving the client runtime), copies the
// BORROWED RecordMetadata's fields out and never destroys the metadata; the
// future handle is destroyed in tp_dealloc.

typedef struct {
    PyObject_HEAD
    kafka_common_KafkaFuture_t* handle;
} KafkaFutureObject;

static void KafkaFuture_dealloc(KafkaFutureObject* self) {
    kafka_common_Error_t *ffi_err = NULL;
    if (self->handle != NULL) {
        ffi_unraisable((kafka_common_KafkaFuture_destroy_err(self->handle, &ffi_err), ffi_take(&ffi_err)));
        self->handle = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* KafkaFuture_wrap(kafka_common_KafkaFuture_t* handle);

// get(timeout_ms=None) -> (meta_tuple | None, err_tuple | None), or None when
// the wait timed out (Java's java.util.concurrent.TimeoutException, i.e.
// kafka_common_Error_is_local_timeout_error_err on a future that is not done).
static PyObject* KafkaFuture_get(KafkaFutureObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* timeout_obj = Py_None;
    if (!PyArg_ParseTuple(args, "|O", &timeout_obj)) return NULL;
    long long timeout_ms = 0;
    if (timeout_obj != Py_None) {
        timeout_ms = PyLong_AsLongLong(timeout_obj);
        if (timeout_ms == -1 && PyErr_Occurred()) return NULL;
        if (timeout_ms < 0) timeout_ms = 0;
    }
    void* out = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (timeout_obj == Py_None) {
        err = (out = kafka_common_KafkaFuture_get(self->handle, &ffi_err), ffi_take(&ffi_err));
    } else {
        err = (out = kafka_common_KafkaFuture_get_with_timeout(self->handle, (int64_t)timeout_ms, &ffi_err), ffi_take(&ffi_err));
    }
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        int8_t timed_out = 0;
        int8_t done = 0;
        if (ffi_raise((timed_out = kafka_common_Error_is_local_timeout_error_err(err, &ffi_err), ffi_take(&ffi_err)))
            || (timed_out && ffi_raise((done = kafka_common_KafkaFuture_is_done_err(self->handle, &ffi_err), ffi_take(&ffi_err))))) {
            ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
            return NULL;
        }
        if (timed_out && !done) {
            ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
            Py_RETURN_NONE;
        }
        return build_value_error(Py_None, err);
    }
    PyObject* meta = metadata_to_py((const kafka_producer_RecordMetadata_t*)out);
    if (meta == NULL) return NULL;
    return Py_BuildValue("(NO)", meta, Py_None);
}

static PyObject* KafkaFuture_is_done(KafkaFutureObject* self, PyObject* Py_UNUSED(ignored)) {
    kafka_common_Error_t *ffi_err = NULL;
    int8_t done = 0;
    if (ffi_raise((done = kafka_common_KafkaFuture_is_done_err(self->handle, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(done ? 1 : 0);
}

// The queued `get`: the context keeps the Python future object -- and so the
// C handle the BORROWED RecordMetadata belongs to -- alive until the
// completion has copied the metadata out.
typedef struct {
    PyObject* cb;
    KafkaFutureObject* fut;
} FutureGetCtx;

static void kafka_future_get_cb(void* value, kafka_common_Error_t* error, void* opaque) {
    FutureGetCtx* ctx = (FutureGetCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* pair;
    if (error != NULL) {
        pair = build_value_error(Py_None, error);
    } else {
        PyObject* meta = metadata_to_py((const kafka_producer_RecordMetadata_t*)value);
        pair = meta ? Py_BuildValue("(NO)", meta, Py_None) : NULL;
    }
    if (pair == NULL) {
        PyErr_WriteUnraisable(ctx->cb);
    } else {
        PyObject* r = PyObject_Call(ctx->cb, pair, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
        Py_DECREF(pair);
    }
    Py_DECREF(ctx->cb);
    Py_DECREF((PyObject*)ctx->fut);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// get_cb(cb) -> None: the queued twin of get(). `cb(metadata_tuple | None,
// error_tuple | None)` runs from the owning producer's callback pump
// (`Producer__execute_callbacks` / `Producer_poll`) once the record completed,
// or inline before this returns when the future belongs to no client. The
// sync `SendFuture.result()` waits on it in slices, never in a native block.
static PyObject* KafkaFuture_get_cb(KafkaFutureObject* self, PyObject* args) {
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "O", &cb)) return NULL;
    FutureGetCtx* ctx = (FutureGetCtx*)PyMem_Malloc(sizeof(FutureGetCtx));
    if (ctx == NULL) return PyErr_NoMemory();
    Py_INCREF(cb);
    Py_INCREF((PyObject*)self);
    ctx->cb = cb;
    ctx->fut = self;
    Py_BEGIN_ALLOW_THREADS
    kafka_common_KafkaFuture_get_cb(self->handle, kafka_future_get_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyMethodDef KafkaFuture_methods[] = {
    {"get", (PyCFunction)KafkaFuture_get, METH_VARARGS,
     "get(timeout_ms=None) -> (metadata_tuple | None, error_tuple | None); None on timeout (blocking; the Python clients use get_cb)"},
    {"get_cb", (PyCFunction)KafkaFuture_get_cb, METH_VARARGS,
     "get_cb(cb) -> None; cb(metadata_tuple | None, error_tuple | None) queued on the owning producer's callbacks vector"},
    {"is_done", (PyCFunction)KafkaFuture_is_done, METH_NOARGS, "Whether the future completed"},
    {NULL}
};

static PyTypeObject KafkaFutureType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "kafkanative.KafkaFuture",
    .tp_doc = "Owned KafkaFuture<RecordMetadata> handle returned by a blocking send",
    .tp_basicsize = sizeof(KafkaFutureObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)KafkaFuture_dealloc,
    .tp_methods = KafkaFuture_methods,
};

static PyObject* KafkaFuture_wrap(kafka_common_KafkaFuture_t* handle) {
    kafka_common_Error_t *ffi_err = NULL;
    KafkaFutureObject* f = PyObject_New(KafkaFutureObject, &KafkaFutureType);
    if (f == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(handle, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    f->handle = handle;
    return (PyObject*)f;
}

// ---- ProducerRecord -> kafka_producer_ProducerRecord_t ---------------------

// Builds the C record from the Python record. The key and value cross as the
// record object's own `kafka_Bytes_t` (NULL serializers), which point straight
// into its bytes objects, so the record object must stay alive for as long as
// Rust may read them: the duration of a blocking send, or until the `_cb`
// completion fires for a queued one. A None key or value crosses as a NULL
// pointer (Java null; a null value is a tombstone); -1 partition / timestamp
// are Java's null. The headers are copied into a `RecordHeaders` (the C API
// has no borrowed form for a header value) and from there into the record.
// Returns the OWNED record, or NULL with the OWNED error in `*out_err`, a
// failing call's included. GIL held.
static kafka_producer_ProducerRecord_t* producer_record_to_c(ProducerRecordObject* rec,
                                                             kafka_common_Error_t** out_err) {
    kafka_common_Error_t *ffi_err = NULL;
    *out_err = NULL;
    kafka_common_header_internals_RecordHeaders_t* headers = NULL;
    kafka_common_Error_t* err = NULL;
    if (rec->header_entries != NULL && rec->header_count > 0) {
        kafka_common_header_Headers_t* view = NULL;
        err = (headers = kafka_common_header_internals_RecordHeaders_new_err(&ffi_err), ffi_take(&ffi_err));
        if (err == NULL) err = (view = kafka_common_header_internals_RecordHeaders__as_Headers_err(headers, &ffi_err), ffi_take(&ffi_err));
        for (int32_t i = 0; err == NULL && i < rec->header_count; i++) {
            const ProducerRecordHeader* h = &rec->header_entries[i];
            kafka_Bytes_t value = {h->value, h->value == NULL ? 0 : h->value_len};
            err = (kafka_common_header_Headers_add_with_key_value(view, h->key, value, &ffi_err), ffi_take(&ffi_err));
        }
        if (err != NULL) {
            if (headers != NULL) {
                ffi_unraisable((kafka_common_header_internals_RecordHeaders_destroy_err(headers, &ffi_err), ffi_take(&ffi_err)));
            }
            *out_err = err;
            return NULL;
        }
    }
    kafka_producer_ProducerRecordOptionsBuilder_t* builder = NULL;
    err = (builder = kafka_producer_ProducerRecordOptionsBuilder_new_err(&ffi_err), ffi_take(&ffi_err));
    if (err == NULL) err = (kafka_producer_ProducerRecordOptionsBuilder_set_topic_err(builder, rec->topic, &ffi_err), ffi_take(&ffi_err));
    if (err == NULL) err = (kafka_producer_ProducerRecordOptionsBuilder_set_partition_err(builder, rec->partition, &ffi_err), ffi_take(&ffi_err));
    if (err == NULL) err = (kafka_producer_ProducerRecordOptionsBuilder_set_timestamp_err(builder, rec->timestamp, &ffi_err), ffi_take(&ffi_err));
    if (err == NULL) {
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_key_err(
            builder, rec->key_b.data != NULL ? (const void*)&rec->key_b : NULL, &ffi_err), ffi_take(&ffi_err));
    }
    if (err == NULL) {
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_value_err(
            builder, rec->value_b.data != NULL ? (const void*)&rec->value_b : NULL, &ffi_err), ffi_take(&ffi_err));
    }
    if (err == NULL && headers != NULL) {
        err = (kafka_producer_ProducerRecordOptionsBuilder_set_headers_err(builder, headers, &ffi_err), ffi_take(&ffi_err));
    }
    kafka_producer_ProducerRecordOptions_t* options = NULL;
    if (err == NULL) err = (options = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &ffi_err), ffi_take(&ffi_err));
    if (builder != NULL) ffi_unraisable((kafka_producer_ProducerRecordOptionsBuilder_destroy_err(builder, &ffi_err), ffi_take(&ffi_err)));
    if (headers != NULL) ffi_unraisable((kafka_common_header_internals_RecordHeaders_destroy_err(headers, &ffi_err), ffi_take(&ffi_err)));
    if (err != NULL) {
        *out_err = err;
        return NULL;
    }
    kafka_producer_ProducerRecord_t* out = NULL;
    err = (out = kafka_producer_ProducerRecord_with_options(options, &ffi_err), ffi_take(&ffi_err));
    ffi_unraisable((kafka_producer_ProducerRecordOptions_destroy_err(options, &ffi_err), ffi_take(&ffi_err)));
    if (err != NULL) {
        *out_err = err;
        return NULL;
    }
    return out;
}

// ---- send (blocking) -------------------------------------------------------

// Producer_send(handle, record, on_completion) -> (KafkaFuture | None, error_int)
// Blocks (GIL released) until the record is registered, as Java's send(). The
// on_completion callable is registered as a kafka_producer_Callback_t and is
// run by Producer_poll / Producer__execute_callbacks / Producer_destroy, at
// most once; it receives (meta_tuple | None, borrowed_error_int, callback_id)
// and reports as producer_on_completion describes (returning None, raising, or
// returning True to report later through Producer__set_callback_result). After
// an error return it still runs if the record had reached the accumulator (see
// Producer_send_cb). The error is OWNED by the caller. Neither Python producer
// calls this.
static PyObject* py_Producer_send(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    PyObject* rec_obj;
    PyObject* callable;
    if (!PyArg_ParseTuple(args, "KO!O", &ptr, &ProducerRecordType, &rec_obj, &callable)) return NULL;
    Producer* p = producer_from_handle(ptr);

    kafka_common_Error_t* err = NULL;
    kafka_producer_ProducerRecord_t* rec =
        producer_record_to_c((ProducerRecordObject*)rec_obj, &err);
    if (rec == NULL) return Py_BuildValue("(ON)", Py_None, error_handle_to_py(err));

    DeliveryCtx* ctx = delivery_ctx_new(callable, p->producer);
    if (ctx == NULL) { ffi_unraisable((kafka_producer_ProducerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    kafka_producer_Callback_t* cb = NULL;
    err = (cb = kafka_producer_Callback_new_err(ctx, producer_on_completion, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        // Never registered: no callback can fire.
        delivery_ctx_free(ctx);
        ffi_unraisable((kafka_producer_ProducerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err)));
        return Py_BuildValue("(ON)", Py_None, error_handle_to_py(err));
    }

    kafka_common_KafkaFuture_t* fut = NULL;
    Py_BEGIN_ALLOW_THREADS
    err = (fut = kafka_producer_Producer_send_with_callback(p->producer, rec, cb, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    // Both are read during the call only (the record was cloned -- its key and
    // value bytes were read during the call -- and the callback registration
    // copied into the Rust closure).
    ffi_unraisable((kafka_producer_Callback_destroy_err(cb, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_producer_ProducerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err)));

    if (err != NULL) {
        producer_orphan_delivery(p, ctx);
        return Py_BuildValue("(ON)", Py_None, error_handle_to_py(err));
    }
    delivery_ctx_release(ctx);
    PyObject* py_fut = KafkaFuture_wrap(fut);
    if (py_fut == NULL) return NULL;
    return Py_BuildValue("(Ni)", py_fut, 0);
}

// ---- send (queued) ---------------------------------------------------------

// The completion of a `send_with_callback_cb`: the Python record (whose
// kafka_Bytes_t the C record's key/value point at) stays alive until `cb`
// fires, which is when the submission task has handed the record to the
// producer.
typedef struct {
    PyObject* resolve;         // Python callable(owned_error_int)
    PyObject* record;
    DeliveryCtx* delivery;
    Producer* producer;
} SendCtx;

static void producer_send_cb(kafka_common_KafkaFuture_t* value, kafka_common_Error_t* error, void* opaque) {
    kafka_common_Error_t *ffi_err = NULL;
    SendCtx* ctx = (SendCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    // The record's outcome reaches Python through the delivery callback; the
    // future is not needed.
    if (value != NULL) ffi_unraisable((kafka_common_KafkaFuture_destroy_err(value, &ffi_err), ffi_take(&ffi_err)));
    if (error != NULL) producer_orphan_delivery(ctx->producer, ctx->delivery);
    else delivery_ctx_release(ctx->delivery);
    PyObject* py_err = error_handle_to_py(error);
    if (py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->resolve, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->resolve); else Py_DECREF(r);
        Py_DECREF(py_err);
    } else {
        PyErr_WriteUnraisable(ctx->resolve);
        if (error != NULL) ffi_unraisable((kafka_common_Error_destroy_err(error, &ffi_err), ffi_take(&ffi_err)));
    }
    Py_DECREF(ctx->record);
    Py_DECREF(ctx->resolve);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// Producer_send_cb(handle, record, on_completion, resolve) -> error_int
// Queues the record on the producer's submission task and returns at once.
// `on_completion(meta_tuple | None, borrowed_error_int, callback_id)` is the
// record's delivery callback (Java's Callback, run at most once); it reports as
// producer_on_completion describes: returning None reports success, raising
// reports the exception, and returning True defers the report to
// Producer__set_callback_result(handle, callback_id, exception | None).
// `resolve(owned_error_int)` runs from the callback pump once the record is
// handed to the producer: 0 when it was, else the error the producer's send()
// returned (Java's doSend rethrow). After an error `on_completion` still runs,
// once, if the record had reached the accumulator before the error (Java's
// maybeAddPartition throws after the append), and never if it had not.
// A record the C API rejects before queuing returns its OWNED error and calls
// neither.
static PyObject* py_Producer_send_cb(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    PyObject* rec_obj;
    PyObject* callable;
    PyObject* resolve;
    if (!PyArg_ParseTuple(args, "KO!OO", &ptr, &ProducerRecordType, &rec_obj, &callable, &resolve)) return NULL;
    Producer* p = producer_from_handle(ptr);

    kafka_common_Error_t* err = NULL;
    kafka_producer_ProducerRecord_t* rec =
        producer_record_to_c((ProducerRecordObject*)rec_obj, &err);
    if (rec == NULL) return error_handle_to_py(err);

    SendCtx* ctx = (SendCtx*)PyMem_Malloc(sizeof(SendCtx));
    if (ctx == NULL) {
        PyErr_NoMemory();
        ffi_unraisable((kafka_producer_ProducerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err)));
        return NULL;
    }
    ctx->delivery = delivery_ctx_new(callable, p->producer);
    if (ctx->delivery == NULL) {
        ffi_unraisable((kafka_producer_ProducerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err)));
        PyMem_Free(ctx);
        return NULL;
    }
    kafka_producer_Callback_t* cb = NULL;
    err = (cb = kafka_producer_Callback_new_err(ctx->delivery, producer_on_completion, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        // Never registered: neither completion can fire.
        delivery_ctx_free(ctx->delivery);
        PyMem_Free(ctx);
        ffi_unraisable((kafka_producer_ProducerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err)));
        return error_handle_to_py(err);
    }
    Py_INCREF(resolve);
    Py_INCREF(rec_obj);
    ctx->resolve = resolve;
    ctx->record = rec_obj;
    ctx->producer = p;

    Py_BEGIN_ALLOW_THREADS
    kafka_producer_Producer_send_with_callback_cb(p->producer, rec, cb, producer_send_cb, ctx);
    Py_END_ALLOW_THREADS
    // Both are copied by the call; the record's key/value bytes stay borrowed
    // until producer_send_cb (ctx->record holds them).
    ffi_unraisable((kafka_producer_Callback_destroy_err(cb, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_producer_ProducerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err)));
    return PyLong_FromLong(0);
}

// ---- void ops: blocking + _cb ----------------------------------------------

typedef void (*producer_void_op_fn)(const kafka_producer_Producer_t*, kafka_common_Error_t**);

// Shared body of the blocking void ops: GIL released around the call, the
// OWNED error returned as an int (0 = success).
static PyObject* producer_void_op(PyObject* args, producer_void_op_fn op) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err = NULL;
    Py_BEGIN_ALLOW_THREADS
    op(p->producer, &err);
    Py_END_ALLOW_THREADS
    return error_handle_to_py(err);
}

static PyObject* py_Producer_flush(PyObject* self, PyObject* args) {
    return producer_void_op(args, kafka_producer_Producer_flush);
}
static PyObject* py_Producer_init_transactions(PyObject* self, PyObject* args) {
    return producer_void_op(args, kafka_producer_Producer_init_transactions);
}
static PyObject* py_Producer_begin_transaction(PyObject* self, PyObject* args) {
    return producer_void_op(args, kafka_producer_Producer_begin_transaction);
}
static PyObject* py_Producer_commit_transaction(PyObject* self, PyObject* args) {
    return producer_void_op(args, kafka_producer_Producer_commit_transaction);
}
static PyObject* py_Producer_abort_transaction(PyObject* self, PyObject* args) {
    return producer_void_op(args, kafka_producer_Producer_abort_transaction);
}

// A queued void op's completion: `cb(owned_error_int)` runs from the pump.
typedef struct {
    PyObject* cb;
} VoidCbCtx;

static void producer_void_cb(kafka_common_Error_t* error, void* opaque) {
    kafka_common_Error_t *ffi_err = NULL;
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* py_err = error_handle_to_py(error);
    if (py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
        Py_DECREF(py_err);
    } else {
        PyErr_WriteUnraisable(ctx->cb);
        if (error != NULL) ffi_unraisable((kafka_common_Error_destroy_err(error, &ffi_err), ffi_take(&ffi_err)));
    }
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

static VoidCbCtx* void_cb_ctx_new(PyObject* cb) {
    VoidCbCtx* ctx = (VoidCbCtx*)PyMem_Malloc(sizeof(VoidCbCtx));
    if (ctx == NULL) { PyErr_NoMemory(); return NULL; }
    Py_INCREF(cb);
    ctx->cb = cb;
    return ctx;
}

typedef void (*producer_void_cb_op_fn)(const kafka_producer_Producer_t*,
                                       void (*)(kafka_common_Error_t*, void*),
                                       void*);

static PyObject* producer_void_cb_op(PyObject* args, producer_void_cb_op_fn op) {
    unsigned long long ptr;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &ptr, &cb)) return NULL;
    Producer* p = producer_from_handle(ptr);
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) return NULL;
    Py_BEGIN_ALLOW_THREADS
    op(p->producer, producer_void_cb, ctx);
    Py_END_ALLOW_THREADS
    // The completion reports every outcome, a panic included.
    Py_RETURN_NONE;
}

static PyObject* py_Producer_flush_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_flush_cb);
}
static PyObject* py_Producer_init_transactions_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_init_transactions_cb);
}
static PyObject* py_Producer_begin_transaction_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_begin_transaction_cb);
}
static PyObject* py_Producer_commit_transaction_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_commit_transaction_cb);
}
static PyObject* py_Producer_abort_transaction_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_abort_transaction_cb);
}
static PyObject* py_Producer_close_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_close_cb);
}

// Producer_close_with_timeout_cb(handle, timeout_ms, cb): Java's
// close(Duration), queued; cb(owned_error_int).
static PyObject* py_Producer_close_with_timeout_cb(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    long long timeout_ms;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &ptr, &timeout_ms, &cb)) return NULL;
    Producer* p = producer_from_handle(ptr);
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) return NULL;
    Py_BEGIN_ALLOW_THREADS
    kafka_producer_Producer_close_with_timeout_cb(p->producer, (int64_t)timeout_ms,
                                                     producer_void_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// ---- send_offsets_to_transaction -------------------------------------------

// Shared with the consumer section: the Python-side
// (topic, partition, offset, leader_epoch, metadata) tuple list -> five
// parallel arrays. Returns count or -1 (exception set); on success the caller
// releases the arrays with offset_arrays_free.
typedef struct {
    const char** topics;
    int32_t* parts;
    int64_t* offs;
    int32_t* epochs;
    const char** metas;
} offset_arrays_t;

static void offset_arrays_free(offset_arrays_t* a) {
    PyMem_Free((void*)a->topics);
    PyMem_Free(a->parts);
    PyMem_Free(a->offs);
    PyMem_Free(a->epochs);
    PyMem_Free((void*)a->metas);
}

static Py_ssize_t offsets_to_arrays(PyObject* list, offset_arrays_t* out) {
    Py_ssize_t n = PySequence_Size(list);
    if (n < 0) return -1;
    out->topics = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    out->parts = n > 0 ? PyMem_Malloc(n * sizeof(int32_t)) : NULL;
    out->offs = n > 0 ? PyMem_Malloc(n * sizeof(int64_t)) : NULL;
    out->epochs = n > 0 ? PyMem_Malloc(n * sizeof(int32_t)) : NULL;
    out->metas = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    if (n > 0 && (!out->topics || !out->parts || !out->offs || !out->epochs || !out->metas)) {
        offset_arrays_free(out);
        PyErr_NoMemory();
        return -1;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(list, i);  // new ref
        const char* t = NULL; int p = 0; long long o = 0; int e = -1; PyObject* meta = Py_None;
        int ok = item && PyArg_ParseTuple(item, "siL|iO", &t, &p, &o, &e, &meta);
        if (ok) {
            out->topics[i] = t; out->parts[i] = p; out->offs[i] = o; out->epochs[i] = e;
            // A non-str metadata must fail the whole call: treating the failed
            // conversion as "no metadata" would commit a different map than the
            // caller passed AND return success with a live exception set, which
            // CPython later reports as an unrelated SystemError.
            if (meta == Py_None) {
                out->metas[i] = NULL;
            } else if (!PyUnicode_Check(meta)) {
                PyErr_Format(PyExc_TypeError,
                             "offset metadata must be str or None, not %s",
                             Py_TYPE(meta)->tp_name);
                ok = 0;
            } else {
                out->metas[i] = PyUnicode_AsUTF8(meta);
                if (out->metas[i] == NULL) ok = 0;  // e.g. unencodable surrogates
            }
        }
        Py_XDECREF(item);
        if (!ok) { offset_arrays_free(out); return -1; }
    }
    return n;
}

// The offsets map every send_offsets_to_transaction form takes: a C-built
// kafka_Map_t of OWNED kafka_common_TopicPartition_t* ->
// kafka_consumer_OffsetAndMetadata_t* (the container borrows, so the entries
// are destroyed one by one before the map).
typedef struct {
    kafka_Map_t* map;
    kafka_common_TopicPartition_t** tps;
    kafka_consumer_OffsetAndMetadata_t** oams;
    Py_ssize_t n;
} offsets_map_t;

static void offsets_map_free(offsets_map_t* m) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < m->n; i++) {
        if (m->oams && m->oams[i]) ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(m->oams[i], &ffi_err), ffi_take(&ffi_err)));
        if (m->tps && m->tps[i]) ffi_unraisable((kafka_common_TopicPartition_destroy_err(m->tps[i], &ffi_err), ffi_take(&ffi_err)));
    }
    PyMem_Free(m->tps);
    PyMem_Free(m->oams);
    if (m->map) ffi_unraisable((kafka_Map_destroy_err(m->map, &ffi_err), ffi_take(&ffi_err)));
}

// Returns 0 on success (exception set otherwise).
static int offsets_map_build(PyObject* spec, offsets_map_t* m) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(m, 0, sizeof(*m));
    offset_arrays_t a;
    Py_ssize_t n = offsets_to_arrays(spec, &a);
    if (n < 0) return -1;
    if (ffi_raise((m->map = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) {
        offset_arrays_free(&a);
        return -1;
    }
    m->tps = n > 0 ? PyMem_Calloc(n, sizeof(*m->tps)) : NULL;
    m->oams = n > 0 ? PyMem_Calloc(n, sizeof(*m->oams)) : NULL;
    if (n > 0 && (!m->tps || !m->oams)) {
        offset_arrays_free(&a);
        offsets_map_free(m);
        PyErr_NoMemory();
        return -1;
    }
    m->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        if (ffi_raise((m->tps[i] = kafka_common_TopicPartition_new_err(a.topics[i], a.parts[i], &ffi_err), ffi_take(&ffi_err)))) {
            offset_arrays_free(&a);
            offsets_map_free(m);
            return -1;
        }
        // A negative leader_epoch is Optional.empty(); a NULL metadata is
        // Java null, stored by the class as "".
        kafka_common_Error_t* err = (m->oams[i] = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
            a.offs[i], a.epochs[i], a.metas[i], &ffi_err), ffi_take(&ffi_err));
        if (err != NULL) {
            const char* msg = NULL;
            if (!ffi_raise((msg = kafka_common_Error_message_err(err, &ffi_err), ffi_take(&ffi_err)))) {
                PyErr_SetString(PyExc_ValueError, msg ? msg : "invalid OffsetAndMetadata");
            }
            ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
            offset_arrays_free(&a);
            offsets_map_free(m);
            return -1;
        }
        if (ffi_raise((kafka_Map_put_err(m->map, m->tps[i], m->oams[i], &ffi_err), ffi_take(&ffi_err)))) {
            offset_arrays_free(&a);
            offsets_map_free(m);
            return -1;
        }
    }
    offset_arrays_free(&a);
    return 0;
}

// Producer_send_offsets_to_transaction(handle, spec, group_metadata)
//   -> owned_error_int (0 = success; blocking, the Python clients use _cb)
static PyObject* py_Producer_send_offsets_to_transaction(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    PyObject* spec;
    PyObject* gm_obj;
    if (!PyArg_ParseTuple(args, "KOO!", &ptr, &spec, &ConsumerGroupMetadataType, &gm_obj)) return NULL;
    Producer* p = producer_from_handle(ptr);
    const kafka_consumer_ConsumerGroupMetadata_t* gm = ((ConsumerGroupMetadataObject*)gm_obj)->handle;
    offsets_map_t m;
    if (offsets_map_build(spec, &m) < 0) return NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (kafka_producer_Producer_send_offsets_to_transaction(p->producer, m.map, gm, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    offsets_map_free(&m);
    return error_handle_to_py(err);
}

// Producer_send_offsets_to_transaction_cb(handle, spec, group_metadata, cb)
// cb(owned_error_int). The offsets map and group metadata are read during the
// call (the Rust side clones them for the submission task), so they are freed
// right after it.
static PyObject* py_Producer_send_offsets_to_transaction_cb(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    PyObject* spec;
    PyObject* gm_obj;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO!O", &ptr, &spec, &ConsumerGroupMetadataType, &gm_obj, &cb)) return NULL;
    Producer* p = producer_from_handle(ptr);
    const kafka_consumer_ConsumerGroupMetadata_t* gm = ((ConsumerGroupMetadataObject*)gm_obj)->handle;
    offsets_map_t m;
    if (offsets_map_build(spec, &m) < 0) return NULL;
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) { offsets_map_free(&m); return NULL; }
    Py_BEGIN_ALLOW_THREADS
    kafka_producer_Producer_send_offsets_to_transaction_cb(p->producer, m.map, gm, producer_void_cb, ctx);
    Py_END_ALLOW_THREADS
    offsets_map_free(&m);
    Py_RETURN_NONE;
}

// ---- partitions_for --------------------------------------------------------

// Converts an OWNED kafka_List_t of kafka_common_PartitionInfo_t* into a list
// of (topic, partition, leader, replicas, isr, offline) tuples and frees it.
static PyObject* partition_info_list_to_py(kafka_List_t* infos) {
    kafka_common_Error_t *ffi_err = NULL;
    if (infos == NULL) return PyList_New(0);
    int32_t n = 0;
    PyObject* out = NULL;
    if (!ffi_raise((n = kafka_List_size_err(infos, &ffi_err), ffi_take(&ffi_err)))) out = PyList_New(n);
    for (int32_t i = 0; out != NULL && i < n; i++) {
        void* info = NULL;
        PyObject* item = NULL;
        if (!ffi_raise((info = kafka_List_get_err(infos, i, &ffi_err), ffi_take(&ffi_err)))) {
            item = partition_info_to_py((const kafka_common_PartitionInfo_t*)info);
        }
        if (item == NULL) { Py_CLEAR(out); break; }
        PyList_SET_ITEM(out, i, item);
    }
    ffi_unraisable((kafka_List_destroy_err(infos, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// Producer_partitions_for(handle, topic) -> (list | None, owned_error_int)
// (blocking; the Python clients use _cb)
static PyObject* py_Producer_partitions_for(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    const char* topic;
    if (!PyArg_ParseTuple(args, "Ks", &ptr, &topic)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_List_t* infos = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (infos = kafka_producer_Producer_partitions_for(p->producer, topic, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (infos != NULL) ffi_unraisable((kafka_List_destroy_err(infos, &ffi_err), ffi_take(&ffi_err)));
        return Py_BuildValue("(ON)", Py_None, error_handle_to_py(err));
    }
    PyObject* list = partition_info_list_to_py(infos);
    if (list == NULL) return NULL;
    return Py_BuildValue("(Ni)", list, 0);
}

static void producer_partitions_for_cb(kafka_List_t* value, kafka_common_Error_t* error, void* opaque) {
    kafka_common_Error_t *ffi_err = NULL;
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* py_list;
    if (error != NULL) {
        if (value != NULL) ffi_unraisable((kafka_List_destroy_err(value, &ffi_err), ffi_take(&ffi_err)));
        py_list = (Py_INCREF(Py_None), Py_None);
    } else {
        py_list = partition_info_list_to_py(value);
    }
    PyObject* py_err = error_handle_to_py(error);
    if (py_list != NULL && py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_list, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
    } else {
        PyErr_WriteUnraisable(ctx->cb);
        if (py_err == NULL && error != NULL) ffi_unraisable((kafka_common_Error_destroy_err(error, &ffi_err), ffi_take(&ffi_err)));
    }
    Py_XDECREF(py_list);
    Py_XDECREF(py_err);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// Producer_partitions_for_cb(handle, topic, cb); cb(list | None, owned_error_int)
static PyObject* py_Producer_partitions_for_cb(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    const char* topic;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsO", &ptr, &topic, &cb)) return NULL;
    Producer* p = producer_from_handle(ptr);
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) return NULL;
    Py_BEGIN_ALLOW_THREADS
    kafka_producer_Producer_partitions_for_cb(p->producer, topic, producer_partitions_for_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// ---- metrics ---------------------------------------------------------------

// Producer_metrics(handle) -> list[dict] (name, group, description, tags,
// value, kind). `kind`: 0 double, 1 string, 2 long, 3 int, selected with
// kafka_common_MetricValue__enum_err; `value` is read with the matching typed
// accessor (float / str / int / int). `as_string` is borrowed until the
// MetricValue handle is destroyed, so it is copied into a str first.
// Converts an OWNED metrics map (kafka_common_MetricName_t* ->
// kafka_common_metrics_KafkaMetric_t*, both owned by the map) into the
// list[dict] shape above and frees it. Shared with the consumer's metrics().
static PyObject* metrics_map_to_py(kafka_Map_t* map) {
    kafka_common_Error_t *ffi_err = NULL;
    if (map == NULL) return PyList_New(0);
    int32_t n = 0;
    if (ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) {
        ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
        return NULL;
    }
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* key = NULL;
        void* val = NULL;
        if (ffi_raise((key = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) || ffi_raise((val = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))) goto fail;
        const kafka_common_MetricName_t* name = (const kafka_common_MetricName_t*)key;
        const kafka_common_metrics_KafkaMetric_t* metric = (const kafka_common_metrics_KafkaMetric_t*)val;

        PyObject* tags = PyDict_New();
        if (tags == NULL) goto fail;
        kafka_Map_t* tag_map = NULL;  // owned, char* -> char*
        if (ffi_raise((tag_map = kafka_common_MetricName_tags_err(name, &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(tags); goto fail; }
        if (tag_map != NULL) {
            int32_t tn = 0;
            int ok = !ffi_raise((tn = kafka_Map_size_err(tag_map, &ffi_err), ffi_take(&ffi_err)));
            for (int32_t t = 0; ok && t < tn; t++) {
                void* k = NULL;
                void* v = NULL;
                ok = !ffi_raise((k = kafka_Map_key_err(tag_map, t, &ffi_err), ffi_take(&ffi_err))) && !ffi_raise((v = kafka_Map_value_err(tag_map, t, &ffi_err), ffi_take(&ffi_err)));
                if (!ok) break;
                PyObject* pv = PyUnicode_FromString(v ? (const char*)v : "");
                ok = pv != NULL && PyDict_SetItemString(tags, k ? (const char*)k : "", pv) == 0;
                Py_XDECREF(pv);
            }
            ffi_unraisable((kafka_Map_destroy_err(tag_map, &ffi_err), ffi_take(&ffi_err)));
            if (!ok) { Py_DECREF(tags); goto fail; }
        }

        PyObject* value = NULL;
        int kind = 0;
        const kafka_common_Metric_t* as_metric = NULL;
        kafka_common_MetricValue_t* mv = NULL;
        if (ffi_raise((as_metric = kafka_common_metrics_KafkaMetric__as_Metric_err(metric, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((mv = kafka_common_Metric_metric_value_err(as_metric, &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(tags);
            goto fail;
        }
        if (mv != NULL) {
            kafka_common_MetricValue_e mv_kind = kafka_common_MetricValue_DOUBLE;
            if (!ffi_raise((mv_kind = kafka_common_MetricValue__enum_err(mv, &ffi_err), ffi_take(&ffi_err)))) {
                switch (mv_kind) {
                    case kafka_common_MetricValue_STRING: {
                        kind = 1;
                        const char* s = NULL;
                        if (ffi_raise((s = kafka_common_MetricValue_as_string_err(mv, &ffi_err), ffi_take(&ffi_err)))) break;
                        value = s ? PyUnicode_FromString(s) : (Py_INCREF(Py_None), Py_None);
                        break;
                    }
                    case kafka_common_MetricValue_LONG: {
                        kind = 2;
                        int64_t v = 0;
                        if (ffi_raise((v = kafka_common_MetricValue_as_long_err(mv, &ffi_err), ffi_take(&ffi_err)))) break;
                        value = PyLong_FromLongLong((long long)v);
                        break;
                    }
                    case kafka_common_MetricValue_INT: {
                        kind = 3;
                        int32_t v = 0;
                        if (ffi_raise((v = kafka_common_MetricValue_as_int_err(mv, &ffi_err), ffi_take(&ffi_err)))) break;
                        value = PyLong_FromLong((long)v);
                        break;
                    }
                    default: {
                        kind = 0;
                        double v = 0.0;
                        if (ffi_raise((v = kafka_common_MetricValue_as_double_err(mv, &ffi_err), ffi_take(&ffi_err)))) break;
                        value = PyFloat_FromDouble(v);
                        break;
                    }
                }
            }
            ffi_unraisable((kafka_common_MetricValue_destroy_err(mv, &ffi_err), ffi_take(&ffi_err)));
        } else {
            value = (Py_INCREF(Py_None), Py_None);
        }
        if (value == NULL) { Py_DECREF(tags); goto fail; }

        const char* mname = NULL;
        const char* group = NULL;
        const char* desc = NULL;
        if (ffi_raise((mname = kafka_common_MetricName_name_err(name, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((group = kafka_common_MetricName_group_err(name, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((desc = kafka_common_MetricName_description_err(name, &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(tags);
            Py_DECREF(value);
            goto fail;
        }
        // "N" steals the references to tags/value.
        PyObject* entry = Py_BuildValue("{s:s,s:s,s:s,s:N,s:N,s:i}",
            "name", mname ? mname : "",
            "group", group ? group : "",
            "description", desc ? desc : "",
            "tags", tags,
            "value", value,
            "kind", kind);
        if (entry == NULL) goto fail;
        PyList_SET_ITEM(out, i, entry);
    }
    ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
    return out;
fail:
    Py_DECREF(out);
    ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
    return NULL;
}

static PyObject* py_Producer_metrics(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_Map_t* metrics = NULL;  // owned, owns its entries
    if (ffi_raise((metrics = kafka_producer_Producer_metrics_err(p->producer, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return metrics_map_to_py(metrics);
}

// ---- MockProducer hooks ----------------------------------------------------

// Builds the error a mock test hook injects, or one `KafkaError_new` hands
// the core. There is no factory from a numeric code -- an error is a class,
// built by the kafka_common_Error_* factory of that class -- so each code
// whose class has a factory taking a message (Java's `(String message)`
// constructor) is mapped to it here; anything else raises ValueError.
// `message` may be NULL. A factory's `_err` twin returns the error it built,
// or the panic it caught: either is an error to inject.
static kafka_common_Error_t* mock_error_from_code(int code, const char* message) {
    kafka_common_Error_t *ffi_err = NULL;
    switch (code) {
        case kafka_common_Error_LOCAL_TIMEOUT:
            return ffi_built(kafka_common_Error_local_timeout_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_LOCAL_ILLEGAL_STATE:
            return ffi_built(kafka_common_Error_local_illegal_state_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT:
            return ffi_built(kafka_common_Error_local_illegal_argument_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_LOCAL_CONCURRENT_MODIFICATION:
            return ffi_built(kafka_common_Error_local_concurrent_modification_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_REQUEST_TIMED_OUT:
            return ffi_built(kafka_common_Error_timeout_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_MESSAGE_TOO_LARGE:
            return ffi_built(kafka_common_Error_record_too_large_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_RECORD_LIST_TOO_LARGE:
            return ffi_built(kafka_common_Error_record_batch_too_large_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_INVALID_GROUP_ID:
            return ffi_built(kafka_common_Error_invalid_group_id_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_UNSUPPORTED_VERSION:
            return ffi_built(kafka_common_Error_unsupported_version_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_THROTTLING_QUOTA_EXCEEDED:
            return ffi_built(kafka_common_Error_throttling_quota_exceeded_err(0, message, &ffi_err), &ffi_err);
        // The (String message) constructors of the classes whose factory
        // takes a payload too: Java passes an empty set / null for it.
        case kafka_common_Error_TOPIC_AUTHORIZATION_FAILED:
            return ffi_built(kafka_common_Error_topic_authorization_message_err(NULL, message, &ffi_err), &ffi_err);
        case kafka_common_Error_INVALID_TOPIC_ERROR:
            return ffi_built(kafka_common_Error_invalid_topics_message_err(NULL, message, &ffi_err), &ffi_err);
        case kafka_common_Error_GROUP_AUTHORIZATION_FAILED:
            return ffi_built(kafka_common_Error_group_authorization_with_message_err(NULL, message, &ffi_err), &ffi_err);
        case kafka_common_Error_PRODUCER_BUFFER_EXHAUSTED:
            return ffi_built(kafka_common_Error_buffer_exhausted_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_CONFIG:
            return ffi_built(kafka_common_Error_config_message_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_INVALID_RECEIVE:
            return ffi_built(kafka_common_Error_invalid_receive_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_SCHEMA:
            return ffi_built(kafka_common_Error_schema_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_SERIALIZATION:
            return ffi_built(kafka_common_Error_serialization_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_WAKEUP:
            return ffi_built(kafka_common_Error_wakeup_err(message, &ffi_err), &ffi_err);
        case kafka_common_Error_TRANSACTION_ABORTED:
            return ffi_built(kafka_common_Error_transaction_aborted_message_err(message, &ffi_err), &ffi_err);
        default:
            PyErr_Format(PyExc_ValueError,
                         "no kafka_common_Error_* factory is mapped for error code %d; "
                         "see mock_error_from_code in _confluentkafka.c", code);
            return NULL;
    }
}

static PyObject* py_MockProducer_complete_next(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    int8_t r = 0;
    kafka_common_Error_t* call_err;
    Py_BEGIN_ALLOW_THREADS
    call_err = (r = kafka_producer_MockProducer_complete_next_err(p->mp, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (ffi_raise(call_err)) return NULL;
    return PyBool_FromLong(r ? 1 : 0);
}

// MockProducer_error_next(handle, code, message | None) -> bool
static PyObject* py_MockProducer_error_next(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    int code;
    const char* message = NULL;
    if (!PyArg_ParseTuple(args, "Kiz", &ptr, &code, &message)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err = mock_error_from_code(code, message);
    if (err == NULL) return NULL;
    int8_t r = 0;
    kafka_common_Error_t* call_err;
    Py_BEGIN_ALLOW_THREADS
    call_err = (r = kafka_producer_MockProducer_error_next_err(p->mp, err, &ffi_err), ffi_take(&ffi_err));  // ownership moves to the mock
    Py_END_ALLOW_THREADS
    if (ffi_raise(call_err)) return NULL;
    return PyBool_FromLong(r ? 1 : 0);
}

static PyObject* py_MockProducer_history_count(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_List_t* history = NULL;  // owned
    if (ffi_raise((history = kafka_producer_MockProducer_history_err(p->mp, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = history ? (n = kafka_List_size_err(history, &ffi_err), ffi_take(&ffi_err)) : NULL;
    if (history) ffi_unraisable((kafka_List_destroy_err(history, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(err)) return NULL;
    return PyLong_FromLong((long)n);
}

static PyObject* py_MockProducer_clear(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    if (ffi_raise((kafka_producer_MockProducer_clear_err(p->mp, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    Py_RETURN_NONE;
}

// MockProducer_set_commit_transaction_error(handle, code | None, message | None)
// None clears the injected error.
static PyObject* py_MockProducer_set_commit_transaction_error(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    PyObject* code_obj;
    const char* message = NULL;
    if (!PyArg_ParseTuple(args, "KO|z", &ptr, &code_obj, &message)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err = NULL;
    if (code_obj != Py_None) {
        long code = PyLong_AsLong(code_obj);
        if (code == -1 && PyErr_Occurred()) return NULL;
        err = mock_error_from_code((int)code, message);
        if (err == NULL) return NULL;
    }
    // ownership moves
    if (ffi_raise((kafka_producer_MockProducer_set_commit_transaction_error_err(p->mp, err, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    Py_RETURN_NONE;
}

static PyObject* py_MockProducer_sent_offsets(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    int8_t sent = 0;
    if (ffi_raise((sent = kafka_producer_MockProducer_sent_offsets_err(p->mp, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(sent ? 1 : 0);
}

// MockProducer_committed_offset(handle, group_id, topic, partition)
//   -> (offset, leader_epoch | None, metadata) or None
static PyObject* py_MockProducer_committed_offset(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    const char* group;
    const char* topic;
    int partition;
    if (!PyArg_ParseTuple(args, "Kssi", &ptr, &group, &topic, &partition)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_TopicPartition_t* tp = NULL;
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, partition, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    kafka_consumer_OffsetAndMetadata_t* oam = NULL;  // owned or NULL
    kafka_common_Error_t* err = (oam = kafka_producer_MockProducer_committed_offset_err(p->mp, group, tp, &ffi_err), ffi_take(&ffi_err));
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(err)) return NULL;
    if (oam == NULL) Py_RETURN_NONE;
    int32_t epoch = -1;  // -1 = Optional.empty()
    const char* metadata = NULL;
    int64_t offset = 0;
    PyObject* out = NULL;
    if (!ffi_raise((epoch = kafka_consumer_OffsetAndMetadata_leader_epoch_err(oam, &ffi_err), ffi_take(&ffi_err)))
        && !ffi_raise((metadata = kafka_consumer_OffsetAndMetadata_metadata_err(oam, &ffi_err), ffi_take(&ffi_err)))
        && !ffi_raise((offset = kafka_consumer_OffsetAndMetadata_offset_err(oam, &ffi_err), ffi_take(&ffi_err)))) {
        PyObject* py_epoch = epoch >= 0 ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None);
        out = py_epoch ? Py_BuildValue("(LNs)", (long long)offset, py_epoch, metadata ? metadata : "") : NULL;
    }
    ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(oam, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// KafkaError accessor/destroy functions
static PyObject* py_KafkaError_code(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    kafka_common_Error_e code = kafka_common_Error_UNKNOWN_SERVER_ERROR;
    if (ffi_raise((code = kafka_common_Error__enum_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLong(code);
}

static PyObject* py_KafkaError_message(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    const char *msg = NULL;
    if (ffi_raise((msg = kafka_common_Error_message_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (msg == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(msg);
}

static PyObject* py_KafkaError_is_retriable(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    int8_t retriable = 0;
    if (ffi_raise((retriable = kafka_common_Error_is_retriable_error_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(retriable ? 1 : 0);
}

// `RequestUtils.isFatalException` composed from the exported predicates and the
// error code, which is what the FFI's own note on
// kafka_common_Error_is_retriable_error_err prescribes: fatality lives in
// `org.apache.kafka.common.requests`, a package Kafka disclaims as unsupported,
// so CLAUDE.md §4 forbids giving it a C binding of its own.
//
// The composition mirrors `request_utils::is_fatal_error` term for term — the
// two authorization/authentication predicates, plus the five coded classes it
// lists — so `KafkaError.is_fatal`, which this backs, keeps answering exactly
// what it did before.
static int error_is_fatal(const kafka_common_Error_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    if (e == NULL) return 0;
    int8_t authentication = 0;
    int8_t authorization = 0;
    kafka_common_Error_e code = kafka_common_Error_UNKNOWN_SERVER_ERROR;
    if (ffi_raise((authentication = kafka_common_Error_is_authentication_error_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((authorization = kafka_common_Error_is_authorization_error_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((code = kafka_common_Error__enum_err(e, &ffi_err), ffi_take(&ffi_err)))) return -1;
    if (authentication || authorization) return 1;
    switch (code) {
        case kafka_common_Error_MISMATCHED_ENDPOINT_TYPE:
        case kafka_common_Error_SECURITY_DISABLED:
        case kafka_common_Error_UNSUPPORTED_VERSION:
        case kafka_common_Error_UNSUPPORTED_ENDPOINT_TYPE:
        case kafka_common_Error_UNSUPPORTED_FOR_MESSAGE_FORMAT:
            return 1;
        default:
            return 0;
    }
}

static PyObject* py_KafkaError_is_fatal(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    int fatal = error_is_fatal(e);
    if (fatal < 0) return NULL;
    return PyBool_FromLong(fatal);
}

// Java's `TransactionAbortableException` test. The Python-visible name stays
// `txn_requires_abort` (librdkafka's spelling, and the public API); only the
// FFI symbol it calls was renamed by the error redesign.
static PyObject* py_KafkaError_txn_requires_abort(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    int8_t abortable = 0;
    if (ffi_raise((abortable = kafka_common_Error_is_transaction_abortable_error_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(abortable ? 1 : 0);
}

// KafkaError_source(handle) -> borrowed_error_int (0 when there is none).
// Java's getCause(): the handle is BORROWED from `handle`, valid while it is,
// and must never be passed to KafkaError_destroy.
static PyObject* py_KafkaError_source(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    const kafka_common_Error_t* e = (const kafka_common_Error_t*)(uintptr_t)ptr;
    const kafka_common_Error_t* source = NULL;
    if (ffi_raise((source = kafka_common_Error_source_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return error_handle_to_py(source);
}

// KafkaError_callback_exception(handle) -> BaseException | None: the exception
// object of the first foreign callback error in the handle's cause chain, the
// handle included (`callback_exception_of`), as a new reference, or None. A
// Python callback that raised reports its exception object through that error
// (`callback_error_from_py`), so the consumer can swap the original back in.
// The handle, still Python's, holds the error's own reference, so the object
// is alive here.
static PyObject* py_KafkaError_callback_exception(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    PyObject* exc = NULL;
    if (callback_exception_of((const kafka_common_Error_t*)(uintptr_t)ptr, &exc) < 0) return NULL;
    if (exc == NULL) Py_RETURN_NONE;
    Py_INCREF(exc);
    return exc;
}

// KafkaError_is_api_error(handle) -> bool: Java's `instanceof ApiException`,
// which tells an UnknownServerError apart from a bare KafkaException (both
// report code -1).
static PyObject* py_KafkaError_is_api_error(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    const kafka_common_Error_t* e = (const kafka_common_Error_t*)(uintptr_t)ptr;
    int8_t api = 0;
    if (ffi_raise((api = kafka_common_Error_is_api_error_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(api ? 1 : 0);
}

// KafkaError_new(code, message | None) -> owned_error_int: an error a Python
// caller hands to the core. -1 (UNKNOWN_SERVER_ERROR, the id the Python base
// KafkaError carries) builds a bare KafkaException; any other code goes
// through mock_error_from_code (ValueError for an unmapped one).
static PyObject* py_KafkaError_new(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    int code;
    const char* message = NULL;
    if (!PyArg_ParseTuple(args, "iz", &code, &message)) return NULL;
    kafka_common_Error_t* err;
    if (code == kafka_common_Error_UNKNOWN_SERVER_ERROR) {
        err = message != NULL ? ffi_built(kafka_common_Error_kafka_message_err(message, &ffi_err), &ffi_err) : ffi_built(kafka_common_Error_kafka_err(&ffi_err), &ffi_err);
    } else {
        err = mock_error_from_code(code, message);
        if (err == NULL) return NULL;
    }
    return error_handle_to_py(err);
}

static PyObject* py_KafkaError_destroy(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    // The error is freed even when its destroy panicked: raise that panic.
    if (ffi_raise((kafka_common_Error_destroy_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    Py_RETURN_NONE;
}

// ===========================================================================
// Consumer — marshaling bridge to the Rust consumer FFI.
//
// The consumer C layer runs NO threads of its own and holds NO business logic:
// it converts Python objects to/from the C FFI types and bridges the FFI's
// callback protocols (CLAUDE.md §4) back into Python.
//
//   * Blocking entry points (`kafka_consumer_Consumer_<op>`) run on the calling
//     thread with the GIL released; the interface methods they trigger (the
//     rebalance listener, the commit callback) are invoked DIRECTLY on that
//     thread by Rust, so the trampolines below re-acquire the GIL.
//   * `_cb` twins (`kafka_consumer_Consumer_<op>_cb`) return at once; the
//     operation runs on the consumer's runtime and both the interface methods
//     it triggers and the completion `cb` are queued on the consumer's callbacks
//     vector, run only by `Consumer__execute_callbacks` (the asyncio pump).
//     `Consumer__set_callbacks_notify` registers the Python callable the Rust
//     notify hook fires once each time the vector goes from empty to non-empty
//     (it may only schedule the pump, never run callbacks).
//   * Interface methods carry an `int64_t callback_id` and are reported through
//     `kafka_consumer_Consumer__set_callback_result_err`, synchronously by the
//     trampoline when the Python adapter returns, or later from Python
//     (`Consumer__set_callback_result`) when the adapter deferred (a coroutine).
//
// Ownership (CLAUDE.md §4): `kafka_common_Error_t *` returns are owned (NULL =
// success) and converted to tuples right away; Rust-returned containers own
// their elements and are freed with `_destroy` after conversion; C-built
// containers hold BORROWED elements, freed one by one after the call (the FFI
// copies its inputs during the call). Records' keys and values are
// `kafka_Bytes_t *` owned by the records handle (NULL deserializers), copied
// into Python `bytes` while that handle is alive.
// ===========================================================================

// ---- shared helpers (also used by the producer and admin sections) --------

static PyObject* node_to_py(const kafka_common_Node_t* node) {
    kafka_common_Error_t *ffi_err = NULL;
    if (node == NULL) Py_RETURN_NONE;
    int32_t id = 0;
    const char* host = NULL;  // NUL-terminated, borrowed
    int32_t port = 0;
    const char* rack = NULL;  // NULL when Java's rack is null
    if (ffi_raise((id = kafka_common_Node_id_err(node, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((host = kafka_common_Node_host_err(node, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((port = kafka_common_Node_port_err(node, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((rack = kafka_common_Node_rack_err(node, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* py_host = PyUnicode_FromString(host ? host : "");
    PyObject* py_rack = rack ? PyUnicode_FromString(rack) : (Py_INCREF(Py_None), Py_None);
    if (py_host == NULL || py_rack == NULL) { Py_XDECREF(py_host); Py_XDECREF(py_rack); return NULL; }
    PyObject* out = Py_BuildValue("(iOiO)", id, py_host, port, py_rack);
    Py_DECREF(py_host); Py_DECREF(py_rack);
    return out;
}

// Converts an owned list of `kafka_common_Node_t *` into a list of node tuples
// and frees it. A NULL list (Java null) becomes None, distinct from an empty
// list.
static PyObject* node_list_to_py(kafka_List_t* nodes) {
    kafka_common_Error_t *ffi_err = NULL;
    if (nodes == NULL) Py_RETURN_NONE;
    int32_t n = 0;
    PyObject* out = ffi_raise((n = kafka_List_size_err(nodes, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n);
    for (int32_t i = 0; out != NULL && i < n; i++) {
        void* node = NULL;
        PyObject* item = ffi_raise((node = kafka_List_get_err(nodes, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : node_to_py((const kafka_common_Node_t*)node);
        if (item == NULL) { Py_CLEAR(out); break; }
        PyList_SET_ITEM(out, i, item);
    }
    ffi_unraisable((kafka_List_destroy_err(nodes, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

static PyObject* partition_info_to_py(const kafka_common_PartitionInfo_t* info) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* topic = NULL;  // NUL-terminated
    int32_t partition = 0;
    const kafka_common_Node_t* leader_node = NULL;
    if (ffi_raise((topic = kafka_common_PartitionInfo_topic_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((partition = kafka_common_PartitionInfo_partition_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((leader_node = kafka_common_PartitionInfo_leader_err(info, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* leader = node_to_py(leader_node);
    PyObject* replicas = NULL;
    PyObject* isr = NULL;
    PyObject* offline = NULL;
    PyObject* py_topic = NULL;
    kafka_List_t* nodes = NULL;  // owned, freed by node_list_to_py
    if (leader != NULL && !ffi_raise((nodes = kafka_common_PartitionInfo_replicas_err(info, &ffi_err), ffi_take(&ffi_err)))) {
        replicas = node_list_to_py(nodes);
    }
    nodes = NULL;
    if (replicas != NULL && !ffi_raise((nodes = kafka_common_PartitionInfo_in_sync_replicas_err(info, &ffi_err), ffi_take(&ffi_err)))) {
        isr = node_list_to_py(nodes);
    }
    nodes = NULL;
    if (isr != NULL && !ffi_raise((nodes = kafka_common_PartitionInfo_offline_replicas_err(info, &ffi_err), ffi_take(&ffi_err)))) {
        offline = node_list_to_py(nodes);
    }
    if (offline != NULL) py_topic = PyUnicode_FromString(topic ? topic : "");
    if (!leader || !replicas || !isr || !offline || !py_topic) {
        Py_XDECREF(leader); Py_XDECREF(replicas); Py_XDECREF(isr); Py_XDECREF(offline);
        Py_XDECREF(py_topic);
        return NULL;
    }
    // Py_BuildValue "N" steals refs to py_topic/leader/replicas/isr/offline
    return Py_BuildValue("(NiNNNN)", py_topic, partition, leader, replicas, isr, offline);
}

// Converts a BORROWED list of `kafka_common_PartitionInfo_t *` (owned by an
// enclosing container) into a list of partition-info tuples.
static PyObject* partition_info_list_to_py_borrowed(const kafka_List_t* infos) {
    kafka_common_Error_t *ffi_err = NULL;
    if (infos == NULL) return PyList_New(0);
    int32_t n = 0;
    if (ffi_raise((n = kafka_List_size_err(infos, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* out = PyList_New(n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        void* info = NULL;
        PyObject* item = ffi_raise((info = kafka_List_get_err(infos, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : partition_info_to_py((const kafka_common_PartitionInfo_t*)info);
        if (item == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, item);
    }
    return out;
}

// Convert the currently set Python exception into an error handle, clearing the
// indicator (it must be clean before returning into Rust). The GIL must be held.
// The bare KafkaException is what Java wraps an arbitrary listener throwable in;
// its code is UnknownServerError (-1).
static kafka_common_Error_t* error_from_py_exception(const char* fallback) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject *type = NULL, *value = NULL, *tb = NULL;
    PyErr_Fetch(&type, &value, &tb);  // clears the indicator
    PyErr_NormalizeException(&type, &value, &tb);
    PyObject* text = value ? PyObject_Str(value) : NULL;
    const char* msg = text ? PyUnicode_AsUTF8(text) : NULL;
    kafka_common_Error_t* err = ffi_built(kafka_common_Error_kafka_message_err(msg ? msg : fallback, &ffi_err), &ffi_err);
    Py_XDECREF(text);
    Py_XDECREF(type); Py_XDECREF(value); Py_XDECREF(tb);
    PyErr_Clear();  // defensive: PyObject_Str / NormalizeException may re-set it
    return err;
}

// The release function of a foreign callback error built by
// `callback_error_from_py`: drops the reference the error owns to its exception
// object. Rust calls it once, when the last copy of the error drops, on any
// thread (a Rust task included), so it takes the GIL. Once the interpreter is
// finalizing it does nothing, leaking the one object instead of touching a
// dying interpreter.
static void release_py_object(void* opaque) {
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    Py_DECREF((PyObject*)opaque);
    PyGILState_Release(g);
}

// A foreign callback error for an exception object: its str() as the message,
// for the client's logs (the core logs a producer callback's one), or its type's
// name when that is empty (a bare `CancelledError`), as Java's toString() is the
// class name for a null message; and the object itself as the opaque pointer.
// The error owns one reference to the object, released by `release_py_object`
// when its last copy drops, so the object lives while any handle to the error
// (or to an error whose cause it is) does, and `KafkaError_callback_exception`
// reads it back. The GIL must be held.
static kafka_common_Error_t* callback_error_from_py(PyObject* exc) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* text = PyObject_Str(exc);
    const char* msg = text ? PyUnicode_AsUTF8(text) : NULL;
    if (msg == NULL) PyErr_Clear();
    if (msg != NULL && msg[0] == '\0') msg = Py_TYPE(exc)->tp_name;
    Py_INCREF(exc);
    kafka_common_Error_t* err = ffi_built(kafka_common_Error_local_callback_err(msg ? msg : "callback raised an exception",
                                                                      (void*)exc, release_py_object, &ffi_err), &ffi_err);
    Py_XDECREF(text);
    return err;
}

// Convert the currently set Python exception, raised by a callback, into a
// foreign callback error (`callback_error_from_py`), clearing the indicator. The
// GIL must be held.
static kafka_common_Error_t* callback_error_from_py_exception(void) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject *type = NULL, *value = NULL, *tb = NULL;
    PyErr_Fetch(&type, &value, &tb);  // clears the indicator
    PyErr_NormalizeException(&type, &value, &tb);
    kafka_common_Error_t* err = value ? callback_error_from_py(value)
                                      : ffi_built(kafka_common_Error_kafka_message_err("callback raised an exception", &ffi_err), &ffi_err);
    Py_XDECREF(type); Py_XDECREF(value); Py_XDECREF(tb);
    PyErr_Clear();  // defensive: NormalizeException / PyObject_Str may re-set it
    return err;
}

// The int Python receives for an OWNED error (0 for NULL): Python takes
// ownership (`from_ffi_error` frees it, a free path `KafkaError_destroy`).
static PyObject* err_result(kafka_common_Error_t* err) {
    return error_handle_to_py(err);
}

// A new (value, error) pair: `value` a borrowed reference, `err` an OWNED error
// handed to Python as an int (0 for NULL), as `err_result`.
static PyObject* cvalue_error(PyObject* value, kafka_common_Error_t* err) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* py_err = error_handle_to_py(err);
    if (py_err == NULL) { if (err) ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    return Py_BuildValue("(ON)", value, py_err);
}

// ---- conversions: C values -> Python ---------------------------------------

// kafka_Bytes_t* -> bytes (copied) or None for NULL / Java null.
static PyObject* bytes_to_py(const kafka_Bytes_t* b) {
    if (b == NULL || b->data == NULL) Py_RETURN_NONE;
    return PyBytes_FromStringAndSize((const char*)b->data, (Py_ssize_t)b->len);
}

static PyObject* tp_to_py(const kafka_common_TopicPartition_t* tp) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = 0;
    if (ffi_raise((topic = kafka_common_TopicPartition_topic_err(tp, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((partition = kafka_common_TopicPartition_partition_err(tp, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return Py_BuildValue("(si)", topic ? topic : "", (int)partition);
}

// BORROWED list of TopicPartition_t* -> list[(topic, partition)].
static PyObject* tp_list_to_py_borrowed(const kafka_List_t* list) {
    kafka_common_Error_t *ffi_err = NULL;
    if (list == NULL) return PyList_New(0);
    int32_t n = 0;
    if (ffi_raise((n = kafka_List_size_err(list, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* out = PyList_New(n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        void* tp = NULL;
        PyObject* t = ffi_raise((tp = kafka_List_get_err(list, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : tp_to_py((const kafka_common_TopicPartition_t*)tp);
        if (t == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, t);
    }
    return out;
}

// OWNED list of owned TopicPartition_t* -> list[(topic, partition)]; frees it.
// NULL (never returned by the FFI, which gives empty containers while an
// operation is in flight) is treated as empty.
static PyObject* tp_list_to_py(kafka_List_t* list) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* out = tp_list_to_py_borrowed(list);
    if (list) ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// OWNED list of owned char* -> list[str]; frees it.
static PyObject* string_list_to_py(kafka_List_t* list) {
    kafka_common_Error_t *ffi_err = NULL;
    if (list == NULL) return PyList_New(0);
    int32_t n = 0;
    PyObject* out = ffi_raise((n = kafka_List_size_err(list, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n);
    for (int32_t i = 0; out != NULL && i < n; i++) {
        void* s = NULL;
        PyObject* t = ffi_raise((s = kafka_List_get_err(list, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : PyUnicode_FromString(s ? (const char*)s : "");
        if (t == NULL) { Py_CLEAR(out); break; }
        PyList_SET_ITEM(out, i, t);
    }
    ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// BORROWED OffsetAndMetadata -> (offset, metadata, leader_epoch | None), or
// None for a NULL value (Java null: no committed offset).
static PyObject* oam_to_py(const kafka_consumer_OffsetAndMetadata_t* oam) {
    kafka_common_Error_t *ffi_err = NULL;
    if (oam == NULL) Py_RETURN_NONE;
    int32_t epoch = -1;  // -1 = Optional.empty()
    const char* metadata = NULL;
    int64_t offset = 0;
    if (ffi_raise((epoch = kafka_consumer_OffsetAndMetadata_leader_epoch_err(oam, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((metadata = kafka_consumer_OffsetAndMetadata_metadata_err(oam, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((offset = kafka_consumer_OffsetAndMetadata_offset_err(oam, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* py_epoch = epoch >= 0 ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None);
    if (py_epoch == NULL) return NULL;
    return Py_BuildValue("(LsN)", (long long)offset, metadata ? metadata : "", py_epoch);
}

// BORROWED map TopicPartition_t* -> OffsetAndMetadata_t* ->
// {(topic, partition): (offset, metadata, leader_epoch | None) | None}.
static PyObject* offset_map_to_py_borrowed(const kafka_Map_t* map) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) return d;
    int32_t n = 0;
    if (ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(d); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* k = NULL;
        void* v = NULL;
        if (ffi_raise((k = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) || ffi_raise((v = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(d);
            return NULL;
        }
        PyObject* key = tp_to_py((const kafka_common_TopicPartition_t*)k);
        PyObject* val = key ? oam_to_py((const kafka_consumer_OffsetAndMetadata_t*)v) : NULL;
        if (key == NULL || val == NULL || PyDict_SetItem(d, key, val) != 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    return d;
}

// OWNED variant of the above; frees the map (and its owned entries).
static PyObject* oam_map_to_py(kafka_Map_t* map) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* d = offset_map_to_py_borrowed(map);
    if (map) ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
    return d;
}

// OWNED map TopicPartition_t* -> int64_t* -> {(topic, partition): int}; frees it.
static PyObject* long_map_to_py(kafka_Map_t* map) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) { if (map) ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err))); return d; }
    int32_t n = 0;
    if (ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) n = -1;
    for (int32_t i = 0; i < n; i++) {
        void* k = NULL;
        void* raw = NULL;
        PyObject* key = NULL;
        PyObject* val = NULL;
        if (!ffi_raise((k = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) && !ffi_raise((raw = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))) {
            const int64_t* v = (const int64_t*)raw;
            key = tp_to_py((const kafka_common_TopicPartition_t*)k);
            val = v ? PyLong_FromLongLong((long long)*v) : (Py_INCREF(Py_None), Py_None);
        }
        if (key == NULL || val == NULL || PyDict_SetItem(d, key, val) != 0) {
            Py_XDECREF(key); Py_XDECREF(val); n = -1; break;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
    if (n < 0) Py_CLEAR(d);
    return d;
}

// OWNED map TopicPartition_t* -> OffsetAndTimestamp_t* ->
// {(topic, partition): (offset, timestamp, leader_epoch | None) | None}; frees it.
static PyObject* oat_map_to_py(kafka_Map_t* map) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) { if (map) ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err))); return d; }
    int32_t n = 0;
    if (ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) n = -1;
    for (int32_t i = 0; i < n; i++) {
        void* k = NULL;
        void* raw = NULL;
        PyObject* key = NULL;
        PyObject* val = NULL;
        if (!ffi_raise((k = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) && !ffi_raise((raw = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))) {
            const kafka_consumer_OffsetAndTimestamp_t* v = (const kafka_consumer_OffsetAndTimestamp_t*)raw;
            key = tp_to_py((const kafka_common_TopicPartition_t*)k);
            if (key != NULL && v == NULL) {
                val = (Py_INCREF(Py_None), Py_None);
            } else if (key != NULL) {
                int32_t epoch = -1;
                int64_t offset = 0;
                int64_t timestamp = 0;
                if (!ffi_raise((epoch = kafka_consumer_OffsetAndTimestamp_leader_epoch_err(v, &ffi_err), ffi_take(&ffi_err)))
                    && !ffi_raise((offset = kafka_consumer_OffsetAndTimestamp_offset_err(v, &ffi_err), ffi_take(&ffi_err)))
                    && !ffi_raise((timestamp = kafka_consumer_OffsetAndTimestamp_timestamp_err(v, &ffi_err), ffi_take(&ffi_err)))) {
                    PyObject* py_epoch = epoch >= 0 ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None);
                    val = py_epoch ? Py_BuildValue("(LLN)", (long long)offset, (long long)timestamp, py_epoch)
                                   : NULL;
                }
            }
        }
        if (key == NULL || val == NULL || PyDict_SetItem(d, key, val) != 0) {
            Py_XDECREF(key); Py_XDECREF(val); n = -1; break;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
    if (n < 0) Py_CLEAR(d);
    return d;
}

// ---- Error payloads ---------------------------------------------------------
//
// A Java exception with payload getters (`TopicAuthorizationException.
// unauthorizedTopics()`, `LogTruncationException.divergentOffsets()`, ...) is
// read through the two-step pattern of CLAUDE.md §4: the borrowed sub-handle
// `kafka_common_Error_<type>(err)` (NULL when the error is not of that class),
// then `kafka_common_<Type>Error_<getter>(sub)`. `KafkaError_payload` returns
// the raw payload as a dict keyed by accessor name, which `_errors.py` turns
// into the Java constructor's arguments, or None when the error has none.

// dict[key] = value, taking `value`'s reference; -1 on failure (value NULL too).
static int payload_set(PyObject* d, const char* key, PyObject* value) {
    if (value == NULL) return -1;
    int rc = PyDict_SetItemString(d, key, value);
    Py_DECREF(value);
    return rc;
}

// A borrowed C string, or None for NULL; a new reference.
static PyObject* str_or_none(const char* s) {
    if (s == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(s);
}

// KafkaError_payload(err) -> dict | None. The handle is not consumed: owned or
// borrowed, its owner still frees it.
static PyObject* py_KafkaError_payload(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)self;
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    const kafka_common_Error_t* e = (const kafka_common_Error_t*)(uintptr_t)ptr;
    if (e == NULL) Py_RETURN_NONE;

    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int ok = 0;

    // Each probe is a borrowed sub-handle, NULL when `e` is not of that class.
    const kafka_common_TopicAuthorizationError_t* ta = NULL;
    if (ffi_raise((ta = kafka_common_Error_topic_authorization_error_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (ta != NULL) {
        kafka_List_t* topics = NULL;
        ok = payload_set(d, "unauthorized_topics",
                         ffi_raise((topics = kafka_common_TopicAuthorizationError_unauthorized_topics_err(ta, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : string_list_to_py(topics)) == 0;
        goto done;
    }
    const kafka_common_GroupAuthorizationError_t* ga = NULL;
    if (ffi_raise((ga = kafka_common_Error_group_authorization_error_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (ga != NULL) {
        const char* group_id = NULL;
        ok = payload_set(d, "group_id",
                         ffi_raise((group_id = kafka_common_GroupAuthorizationError_group_id_err(ga, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : str_or_none(group_id)) == 0;
        goto done;
    }
    const kafka_common_InvalidTopicError_t* it = NULL;
    if (ffi_raise((it = kafka_common_Error_invalid_topic_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (it != NULL) {
        kafka_List_t* topics = NULL;
        ok = payload_set(d, "invalid_topics",
                         ffi_raise((topics = kafka_common_InvalidTopicError_invalid_topics_err(it, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : string_list_to_py(topics)) == 0;
        goto done;
    }
    const kafka_common_ThrottlingQuotaExceededError_t* tq = NULL;
    if (ffi_raise((tq = kafka_common_Error_throttling_quota_exceeded_error_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (tq != NULL) {
        int32_t throttle_time_ms = 0;
        ok = payload_set(d, "throttle_time_ms",
                         ffi_raise((throttle_time_ms = kafka_common_ThrottlingQuotaExceededError_throttle_time_ms_err(tq, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : PyLong_FromLong(throttle_time_ms)) == 0;
        goto done;
    }
    const kafka_common_QuotaViolationError_t* qv = NULL;
    if (ffi_raise((qv = kafka_common_Error_quota_violation_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (qv != NULL) {
        const kafka_common_MetricName_t* mn = NULL;
        const char* mn_name = NULL;
        const char* mn_group = NULL;
        double value = 0.0;
        double bound = 0.0;
        if (ffi_raise((mn = kafka_common_QuotaViolationError_metric_name_err(qv, &ffi_err), ffi_take(&ffi_err)))
            || (mn != NULL && ffi_raise((mn_name = kafka_common_MetricName_name_err(mn, &ffi_err), ffi_take(&ffi_err))))
            || (mn != NULL && ffi_raise((mn_group = kafka_common_MetricName_group_err(mn, &ffi_err), ffi_take(&ffi_err))))
            || ffi_raise((value = kafka_common_QuotaViolationError_value_err(qv, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((bound = kafka_common_QuotaViolationError_bound_err(qv, &ffi_err), ffi_take(&ffi_err)))) goto done;
        ok = payload_set(d, "metric_name", str_or_none(mn_name)) == 0
          && payload_set(d, "metric_group", str_or_none(mn_group)) == 0
          && payload_set(d, "value", PyFloat_FromDouble(value)) == 0
          && payload_set(d, "bound", PyFloat_FromDouble(bound)) == 0;
        goto done;
    }
    // LogTruncationException extends OffsetOutOfRangeException: probe it
    // first, so a truncation reports its own (larger) payload.
    const kafka_common_ConsumerLogTruncationError_t* lt = NULL;
    if (ffi_raise((lt = kafka_common_Error_consumer_log_truncation_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (lt != NULL) {
        kafka_Map_t* oor_map = NULL;
        kafka_Map_t* divergent = NULL;
        ok = payload_set(d, "offset_out_of_range_partitions",
                         ffi_raise((oor_map = kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions_err(lt, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : long_map_to_py(oor_map)) == 0
          && payload_set(d, "divergent_offsets",
                         ffi_raise((divergent = kafka_common_ConsumerLogTruncationError_divergent_offsets_err(lt, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : oam_map_to_py(divergent)) == 0;
        goto done;
    }
    const kafka_common_ConsumerOffsetOutOfRangeError_t* oor = NULL;
    if (ffi_raise((oor = kafka_common_Error_consumer_offset_out_of_range_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (oor != NULL) {
        kafka_Map_t* oor_map = NULL;
        ok = payload_set(d, "offset_out_of_range_partitions",
                         ffi_raise((oor_map = kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions_err(oor, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : long_map_to_py(oor_map)) == 0;
        goto done;
    }
    const kafka_common_ConsumerNoOffsetForPartitionError_t* no = NULL;
    if (ffi_raise((no = kafka_common_Error_consumer_no_offset_for_partition_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (no != NULL) {
        kafka_List_t* partitions = NULL;
        ok = payload_set(d, "partitions",
                         ffi_raise((partitions = kafka_common_ConsumerNoOffsetForPartitionError_partitions_err(no, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : tp_list_to_py(partitions)) == 0;
        goto done;
    }
    const kafka_common_RecordTooLargeError_t* rtl = NULL;
    if (ffi_raise((rtl = kafka_common_Error_record_too_large_error_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (rtl != NULL) {
        // NULL is Java's null field (no partitions recorded), not an empty map.
        kafka_Map_t* m = NULL;
        if (ffi_raise((m = kafka_common_RecordTooLargeError_record_too_large_partitions_err(rtl, &ffi_err), ffi_take(&ffi_err)))) goto done;
        ok = payload_set(d, "record_too_large_partitions",
                         m != NULL ? long_map_to_py(m) : (Py_INCREF(Py_None), Py_None)) == 0;
        goto done;
    }
    const kafka_common_DuplicateResourceError_t* dr = NULL;
    if (ffi_raise((dr = kafka_common_Error_duplicate_resource_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (dr != NULL) {
        const char* resource = NULL;
        ok = payload_set(d, "resource",
                         ffi_raise((resource = kafka_common_DuplicateResourceError_resource_err(dr, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : str_or_none(resource)) == 0;
        goto done;
    }
    const kafka_common_ResourceNotFoundError_t* rnf = NULL;
    if (ffi_raise((rnf = kafka_common_Error_resource_not_found_err(e, &ffi_err), ffi_take(&ffi_err)))) goto done;
    if (rnf != NULL) {
        const char* resource = NULL;
        ok = payload_set(d, "resource",
                         ffi_raise((resource = kafka_common_ResourceNotFoundError_resource_err(rnf, &ffi_err), ffi_take(&ffi_err)))
                             ? NULL : str_or_none(resource)) == 0;
        goto done;
    }

    // No typed payload for this error.
    Py_DECREF(d);
    Py_RETURN_NONE;

done:
    if (!ok) { Py_DECREF(d); return NULL; }
    return d;
}

// OWNED map char* -> kafka_List_t* of PartitionInfo_t* (all owned by the map)
// -> {topic: [partition_info_tuple]}; frees it.
static PyObject* topics_map_to_py(kafka_Map_t* map) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) { if (map) ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err))); return d; }
    int32_t n = 0;
    if (ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) n = -1;
    for (int32_t i = 0; i < n; i++) {
        void* topic = NULL;
        void* infos = NULL;
        PyObject* val = NULL;
        if (!ffi_raise((topic = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) && !ffi_raise((infos = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))) {
            val = partition_info_list_to_py_borrowed((const kafka_List_t*)infos);
        }
        if (val == NULL || PyDict_SetItemString(d, topic ? (const char*)topic : "", val) != 0) {
            Py_XDECREF(val); n = -1; break;
        }
        Py_DECREF(val);
    }
    ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
    if (n < 0) Py_CLEAR(d);
    return d;
}

// ---- conversions: Python -> C input containers -----------------------------
//
// C-built containers hold BORROWED elements: the TopicPartition_t* / char* /
// int64_t* below are owned by these helper structs and freed after the call.

// list[(topic, partition)] -> kafka_List_t of TopicPartition_t*.
typedef struct {
    kafka_List_t* list;
    kafka_common_TopicPartition_t** tps;
    Py_ssize_t n;
} tp_list_t;

static void tp_list_free(tp_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->tps && l->tps[i]) ffi_unraisable((kafka_common_TopicPartition_destroy_err(l->tps[i], &ffi_err), ffi_take(&ffi_err)));
    }
    PyMem_Free(l->tps);
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    memset(l, 0, sizeof(*l));
}

// Returns 0 on success (exception set otherwise).
static int tp_list_build(PyObject* seq, tp_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "partitions must be a sequence of (topic, partition)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return -1; }
    l->tps = n > 0 ? PyMem_Calloc(n, sizeof(*l->tps)) : NULL;
    if (n > 0 && l->tps == NULL) { Py_DECREF(fast); tp_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);  // borrowed
        const char* t = NULL; int p = 0;
        if (!PyArg_ParseTuple(item, "si", &t, &p)
            || ffi_raise((l->tps[i] = kafka_common_TopicPartition_new_err(t, (int32_t)p, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((kafka_List_add_err(l->list, l->tps[i], &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(fast); tp_list_free(l); return -1;
        }
    }
    Py_DECREF(fast);
    return 0;
}

// list[str] -> kafka_List_t of borrowed char* (owned by the str objects of the
// fast sequence kept alive in `seq`).
typedef struct {
    kafka_List_t* list;
    PyObject* seq;
} str_list_t;

static void str_list_free(str_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    Py_XDECREF(l->seq);
    memset(l, 0, sizeof(*l));
}

static int str_list_build(PyObject* seq, str_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "topics must be a sequence of str");
    if (fast == NULL) return -1;
    l->seq = fast;
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { str_list_free(l); return -1; }
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);  // borrowed
        const char* s = PyUnicode_Check(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (s == NULL) {
            if (!PyErr_Occurred()) PyErr_SetString(PyExc_TypeError, "topics must be str");
            str_list_free(l);
            return -1;
        }
        if (ffi_raise((kafka_List_add_err(l->list, (void*)s, &ffi_err), ffi_take(&ffi_err)))) { str_list_free(l); return -1; }
    }
    return 0;
}

// list[(topic, partition, int64)] -> kafka_Map_t of TopicPartition_t* -> int64_t*.
typedef struct {
    kafka_Map_t* map;
    kafka_common_TopicPartition_t** tps;
    int64_t* vals;
    Py_ssize_t n;
} tp_i64_map_t;

static void tp_i64_map_free(tp_i64_map_t* m) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < m->n; i++) {
        if (m->tps && m->tps[i]) ffi_unraisable((kafka_common_TopicPartition_destroy_err(m->tps[i], &ffi_err), ffi_take(&ffi_err)));
    }
    PyMem_Free(m->tps);
    PyMem_Free(m->vals);
    if (m->map) ffi_unraisable((kafka_Map_destroy_err(m->map, &ffi_err), ffi_take(&ffi_err)));
    memset(m, 0, sizeof(*m));
}

static int tp_i64_map_build(PyObject* seq, tp_i64_map_t* m) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(m, 0, sizeof(*m));
    PyObject* fast = PySequence_Fast(seq, "expected a sequence of (topic, partition, value)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (ffi_raise((m->map = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return -1; }
    m->tps = n > 0 ? PyMem_Calloc(n, sizeof(*m->tps)) : NULL;
    m->vals = n > 0 ? PyMem_Calloc(n, sizeof(*m->vals)) : NULL;
    if (n > 0 && (m->tps == NULL || m->vals == NULL)) {
        Py_DECREF(fast); tp_i64_map_free(m); PyErr_NoMemory(); return -1;
    }
    m->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);  // borrowed
        const char* t = NULL; int p = 0; long long v = 0;
        if (!PyArg_ParseTuple(item, "siL", &t, &p, &v)) { Py_DECREF(fast); tp_i64_map_free(m); return -1; }
        m->vals[i] = (int64_t)v;
        if (ffi_raise((m->tps[i] = kafka_common_TopicPartition_new_err(t, (int32_t)p, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((kafka_Map_put_err(m->map, m->tps[i], &m->vals[i], &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(fast); tp_i64_map_free(m); return -1;
        }
    }
    Py_DECREF(fast);
    return 0;
}

// ---- ConsumerRecords / ConsumerRecord extension types ----------------------

// ---- _BorrowedBytes: zero-copy buffer exporter over a ConsumerRecords batch --
//
// A record's key, value and header values are memoryview(_BorrowedBytes(batch,
// ptr, len)). The memoryview holds a reference to the _BorrowedBytes (the
// buffer protocol's view->obj), and the _BorrowedBytes a strong reference to
// the owning ConsumerRecords batch, so the Rust buffer the slice points into
// lives as long as any view over it (consumer-threading.md §27).
typedef struct {
    PyObject_HEAD
    PyObject* batch;        // strong ref to the owning ConsumerRecordsObject
    const uint8_t* ptr;     // borrowed slice into the batch buffer
    Py_ssize_t len;
} BorrowedBytesObject;

static int BorrowedBytes_getbuffer(PyObject* exporter, Py_buffer* view, int flags) {
    BorrowedBytesObject* self = (BorrowedBytesObject*)exporter;
    // PyBuffer_FillInfo sets view->obj = exporter and INCREFs it.
    return PyBuffer_FillInfo(view, exporter, (void*)self->ptr, self->len, 1 /* readonly */, flags);
}

static PyBufferProcs BorrowedBytes_as_buffer = {
    .bf_getbuffer = BorrowedBytes_getbuffer,
    .bf_releasebuffer = NULL,
};

static void BorrowedBytes_dealloc(BorrowedBytesObject* self) {
    Py_XDECREF(self->batch);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyTypeObject BorrowedBytesType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_confluentkafka._BorrowedBytes",
    .tp_doc = "Zero-copy buffer exporter over a ConsumerRecords batch",
    .tp_basicsize = sizeof(BorrowedBytesObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)BorrowedBytes_dealloc,
    .tp_as_buffer = &BorrowedBytes_as_buffer,
};

// memoryview(_BorrowedBytes(batch, b->data, b->len)), or None for a NULL `b`
// or a NULL data pointer (Java null).
static PyObject* borrowed_memoryview(PyObject* batch, const kafka_Bytes_t* b) {
    if (b == NULL || b->data == NULL) Py_RETURN_NONE;
    BorrowedBytesObject* bb = PyObject_New(BorrowedBytesObject, &BorrowedBytesType);
    if (bb == NULL) return NULL;
    Py_INCREF(batch);
    bb->batch = batch;
    bb->ptr = b->data;
    bb->len = (Py_ssize_t)b->len;
    PyObject* mv = PyMemoryView_FromObject((PyObject*)bb);
    Py_DECREF(bb);  // the memoryview holds its own reference
    return mv;
}

// Owns the Rust batch handle; every record object borrows from it.
//
// `get(i)` indexes a flat array of the batch's records, built on first use from
// `partitions()` x `records_with_partition()` (the C API has no indexed
// accessor): Java's iterator order, partition by partition. The pointers are
// borrowed from the batch; the lists that carried them are freed at once.
//
// `copy_bytes` is set for a MockConsumer's batch: its records share the
// buffers pinned by add_record, which live only until the consumer is
// destroyed, so their bytes are copied out rather than viewed.
typedef struct {
    PyObject_HEAD
    kafka_consumer_ConsumerRecords_t* records;  // owned; freed in dealloc
    int copy_bytes;
    const kafka_consumer_ConsumerRecord_t** flat;  // NULL until built
    Py_ssize_t flat_len;
} ConsumerRecordsObject;

static void ConsumerRecords_dealloc(ConsumerRecordsObject* self) {
    kafka_common_Error_t *ffi_err = NULL;
    PyMem_Free((void*)self->flat);
    self->flat = NULL;
    if (self->records != NULL) {
        ffi_unraisable((kafka_consumer_ConsumerRecords_destroy_err(self->records, &ffi_err), ffi_take(&ffi_err)));
        self->records = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

// A key / value / header value of a record of `batch`: a view into the batch,
// or a bytes copy for a mock batch (see ConsumerRecordsObject).
static PyObject* record_bytes_to_py(PyObject* batch, const kafka_Bytes_t* b) {
    if (((ConsumerRecordsObject*)batch)->copy_bytes) return bytes_to_py(b);
    return borrowed_memoryview(batch, b);
}

static PyTypeObject ConsumerRecordsType;  // forward decl (defined after record)
static PyTypeObject ConsumerRecordType;

// Borrows a record from the batch (strong ref to it). The key and value are
// `kafka_Bytes_t *` -- owned by the batch for a KafkaConsumer with NULL
// deserializers, shared with the mock's add_record caller for a MockConsumer --
// and are wrapped when the record object is created: memoryviews into the
// batch, or bytes copies for a mock batch (`record_bytes_to_py`).
typedef struct {
    PyObject_HEAD
    PyObject* batch;                            // strong ref to ConsumerRecordsObject
    const kafka_consumer_ConsumerRecord_t* rec; // borrowed from the batch
    PyObject* key;                              // memoryview, bytes or None
    PyObject* value;                            // memoryview, bytes or None
} ConsumerRecordObject;

static void ConsumerRecord_dealloc(ConsumerRecordObject* self) {
    Py_XDECREF(self->key);
    Py_XDECREF(self->value);
    Py_XDECREF(self->batch);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* ConsumerRecord_get_topic(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* topic = NULL;
    if (ffi_raise((topic = kafka_consumer_ConsumerRecord_topic_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (topic == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(topic);
}

static PyObject* ConsumerRecord_get_partition(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t partition = 0;
    if (ffi_raise((partition = kafka_consumer_ConsumerRecord_partition_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLong(partition);
}

static PyObject* ConsumerRecord_get_offset(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    int64_t offset = 0;
    if (ffi_raise((offset = kafka_consumer_ConsumerRecord_offset_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLongLong(offset);
}

static PyObject* ConsumerRecord_get_timestamp(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    int64_t timestamp = 0;
    if (ffi_raise((timestamp = kafka_consumer_ConsumerRecord_timestamp_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLongLong(timestamp);
}

// Java's TimestampType.id: -1 NO_TIMESTAMP_TYPE, 0 CREATE_TIME, 1 LOG_APPEND_TIME.
static PyObject* ConsumerRecord_get_timestamp_type(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_common_record_TimestampType_t* t = NULL;
    if (ffi_raise((t = kafka_consumer_ConsumerRecord_timestamp_type_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (t == NULL) Py_RETURN_NONE;
    int32_t id = 0;
    if (ffi_raise((id = kafka_common_record_TimestampType_id_err(t, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLong(id);
}

static PyObject* ConsumerRecord_get_key(ConsumerRecordObject* self, void* closure) {
    Py_INCREF(self->key);
    return self->key;
}

static PyObject* ConsumerRecord_get_value(ConsumerRecordObject* self, void* closure) {
    Py_INCREF(self->value);
    return self->value;
}

static PyObject* ConsumerRecord_get_serialized_key_size(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t size = 0;
    if (ffi_raise((size = kafka_consumer_ConsumerRecord_serialized_key_size_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLong(size);
}

static PyObject* ConsumerRecord_get_serialized_value_size(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t size = 0;
    if (ffi_raise((size = kafka_consumer_ConsumerRecord_serialized_value_size_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLong(size);
}

static PyObject* ConsumerRecord_get_leader_epoch(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t epoch = -1;  // -1 = Optional.empty()
    if (ffi_raise((epoch = kafka_consumer_ConsumerRecord_leader_epoch_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (epoch < 0) Py_RETURN_NONE;
    return PyLong_FromLong(epoch);
}

static PyObject* ConsumerRecord_get_delivery_count(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    int16_t count = -1;  // -1 = Optional.empty()
    if (ffi_raise((count = kafka_consumer_ConsumerRecord_delivery_count_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (count < 0) Py_RETURN_NONE;
    return PyLong_FromLong(count);
}

// headers -> list[(key: str, value: memoryview | None)], in insertion order
// (bytes for a mock batch, see `record_bytes_to_py`).
static PyObject* ConsumerRecord_get_headers(ConsumerRecordObject* self, void* closure) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_common_header_internals_RecordHeaders_t* rh = NULL;
    if (ffi_raise((rh = kafka_consumer_ConsumerRecord_headers_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (rh == NULL) return PyList_New(0);
    // The interface view is borrowed from the class handle (never destroyed);
    // the `__as_` conversion takes the handle non-const.
    kafka_common_header_Headers_t* headers = NULL;
    kafka_List_t* arr = NULL;  // owned list of borrowed headers
    if (ffi_raise((headers = kafka_common_header_internals_RecordHeaders__as_Headers_err(
            (kafka_common_header_internals_RecordHeaders_t*)rh, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((arr = kafka_common_header_Headers_to_array_err(headers, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (arr == NULL) return PyList_New(0);
    int32_t n = 0;
    if (ffi_raise((n = kafka_List_size_err(arr, &ffi_err), ffi_take(&ffi_err)))) { ffi_unraisable((kafka_List_destroy_err(arr, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    PyObject* list = PyList_New(n);
    if (list == NULL) { ffi_unraisable((kafka_List_destroy_err(arr, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* rec_header = NULL;
        const kafka_common_header_Header_t* h = NULL;
        const char* hkey = NULL;
        kafka_Bytes_t hval = {0};
        if (ffi_raise((rec_header = kafka_List_get_err(arr, i, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((h = kafka_common_header_internals_RecordHeader__as_Header_err(
                   (const kafka_common_header_internals_RecordHeader_t*)rec_header, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((hkey = kafka_common_header_Header_key_err(h, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((hval = kafka_common_header_Header_value_err(h, &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(list); ffi_unraisable((kafka_List_destroy_err(arr, &ffi_err), ffi_take(&ffi_err)));
            return NULL;
        }
        PyObject* pykey = PyUnicode_FromString(hkey ? hkey : "");
        PyObject* pyval = record_bytes_to_py(self->batch, &hval);
        if (pykey == NULL || pyval == NULL) {
            Py_XDECREF(pykey); Py_XDECREF(pyval); Py_DECREF(list); ffi_unraisable((kafka_List_destroy_err(arr, &ffi_err), ffi_take(&ffi_err)));
            return NULL;
        }
        PyObject* tuple = Py_BuildValue("(NN)", pykey, pyval);  // steals both
        if (tuple == NULL) { Py_DECREF(list); ffi_unraisable((kafka_List_destroy_err(arr, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(list, i, tuple);  // steals ref
    }
    ffi_unraisable((kafka_List_destroy_err(arr, &ffi_err), ffi_take(&ffi_err)));
    return list;
}

static PyObject* ConsumerRecord_repr(ConsumerRecordObject* self) {
    kafka_common_Error_t *ffi_err = NULL;
    char* s = NULL;
    if (ffi_raise((s = kafka_consumer_ConsumerRecord_to_string_err(self->rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (s == NULL) return PyUnicode_FromString("ConsumerRecord(...)");
    PyObject* out = PyUnicode_FromString(s);
    ffi_unraisable((kafka_string_destroy_err(s, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

static PyGetSetDef ConsumerRecord_getsetters[] = {
    {"topic", (getter)ConsumerRecord_get_topic, NULL, "Topic name", NULL},
    {"partition", (getter)ConsumerRecord_get_partition, NULL, "Partition", NULL},
    {"offset", (getter)ConsumerRecord_get_offset, NULL, "Offset", NULL},
    {"timestamp", (getter)ConsumerRecord_get_timestamp, NULL, "Timestamp", NULL},
    {"timestamp_type", (getter)ConsumerRecord_get_timestamp_type, NULL, "Timestamp type id", NULL},
    {"key", (getter)ConsumerRecord_get_key, NULL, "Key (memoryview, bytes for a mock batch, or None)", NULL},
    {"value", (getter)ConsumerRecord_get_value, NULL, "Value (memoryview, bytes for a mock batch, or None)", NULL},
    {"serialized_key_size", (getter)ConsumerRecord_get_serialized_key_size, NULL, "Serialized key size", NULL},
    {"serialized_value_size", (getter)ConsumerRecord_get_serialized_value_size, NULL, "Serialized value size", NULL},
    {"leader_epoch", (getter)ConsumerRecord_get_leader_epoch, NULL, "Leader epoch or None", NULL},
    {"delivery_count", (getter)ConsumerRecord_get_delivery_count, NULL, "Delivery count or None", NULL},
    {"headers", (getter)ConsumerRecord_get_headers, NULL, "Headers list of (key, value)", NULL},
    {NULL}
};

static PyTypeObject ConsumerRecordType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_confluentkafka.ConsumerRecord",
    .tp_doc = "A single consumed record (borrows from its batch)",
    .tp_basicsize = sizeof(ConsumerRecordObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)ConsumerRecord_dealloc,
    .tp_repr = (reprfunc)ConsumerRecord_repr,
    .tp_getset = ConsumerRecord_getsetters,
};

// Wraps a BORROWED record of `batch` into a ConsumerRecord object, wrapping its
// key and value now (`record_bytes_to_py`).
static PyObject* wrap_record(ConsumerRecordsObject* batch, const kafka_consumer_ConsumerRecord_t* rec) {
    kafka_common_Error_t *ffi_err = NULL;
    void* key_ptr = NULL;
    void* value_ptr = NULL;
    if (ffi_raise((key_ptr = kafka_consumer_ConsumerRecord_key_err(rec, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((value_ptr = kafka_consumer_ConsumerRecord_value_err(rec, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* key = record_bytes_to_py((PyObject*)batch, (const kafka_Bytes_t*)key_ptr);
    PyObject* value = record_bytes_to_py((PyObject*)batch, (const kafka_Bytes_t*)value_ptr);
    if (key == NULL || value == NULL) { Py_XDECREF(key); Py_XDECREF(value); return NULL; }
    ConsumerRecordObject* obj = PyObject_New(ConsumerRecordObject, &ConsumerRecordType);
    if (obj == NULL) { Py_DECREF(key); Py_DECREF(value); return NULL; }
    Py_INCREF((PyObject*)batch);
    obj->batch = (PyObject*)batch;
    obj->rec = rec;
    obj->key = key;
    obj->value = value;
    return (PyObject*)obj;
}

// Appends the records of an OWNED list of BORROWED records to `out`, then
// frees the list (never the records). Returns 0 on success.
static int append_record_list(ConsumerRecordsObject* batch, kafka_List_t* recs, PyObject* out) {
    kafka_common_Error_t *ffi_err = NULL;
    if (recs == NULL) return 0;
    int32_t n = 0;
    int rc = ffi_raise((n = kafka_List_size_err(recs, &ffi_err), ffi_take(&ffi_err))) ? -1 : 0;
    for (int32_t i = 0; rc == 0 && i < n; i++) {
        void* rec = NULL;
        PyObject* r = ffi_raise((rec = kafka_List_get_err(recs, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : wrap_record(batch, (const kafka_consumer_ConsumerRecord_t*)rec);
        if (r == NULL || PyList_Append(out, r) != 0) rc = -1;
        Py_XDECREF(r);
    }
    ffi_unraisable((kafka_List_destroy_err(recs, &ffi_err), ffi_take(&ffi_err)));
    return rc;
}

// ConsumerRecords.records() -> list[ConsumerRecord], partition by partition in
// fetch order (Java's iterator order).
static PyObject* ConsumerRecords_records(ConsumerRecordsObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* out = PyList_New(0);
    if (out == NULL) return NULL;
    kafka_List_t* parts = NULL;  // owned, owns its TPs
    if (ffi_raise((parts = kafka_consumer_ConsumerRecords_partitions_err(self->records, &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(out); return NULL; }
    if (parts != NULL) {
        int32_t n = 0;
        if (ffi_raise((n = kafka_List_size_err(parts, &ffi_err), ffi_take(&ffi_err)))) Py_CLEAR(out);
        for (int32_t i = 0; out != NULL && i < n; i++) {
            void* tp = NULL;
            kafka_List_t* recs = NULL;
            if (ffi_raise((tp = kafka_List_get_err(parts, i, &ffi_err), ffi_take(&ffi_err)))
                || ffi_raise((recs = kafka_consumer_ConsumerRecords_records_with_partition_err(
                       self->records, (const kafka_common_TopicPartition_t*)tp, &ffi_err), ffi_take(&ffi_err)))
                || append_record_list(self, recs, out) < 0) {
                Py_CLEAR(out);
            }
        }
        ffi_unraisable((kafka_List_destroy_err(parts, &ffi_err), ffi_take(&ffi_err)));
    }
    return out;
}

// ConsumerRecords.partitions() -> list[(topic, partition)]
static PyObject* ConsumerRecords_partitions(ConsumerRecordsObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* parts = NULL;
    if (ffi_raise((parts = kafka_consumer_ConsumerRecords_partitions_err(self->records, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return tp_list_to_py(parts);
}

// ConsumerRecords.records_with_partition(topic, partition) -> list[ConsumerRecord]
static PyObject* ConsumerRecords_records_with_partition(ConsumerRecordsObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* topic; int partition;
    if (!PyArg_ParseTuple(args, "si", &topic, &partition)) return NULL;
    PyObject* out = PyList_New(0);
    if (out == NULL) return NULL;
    kafka_common_TopicPartition_t* tp = NULL;
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(out); return NULL; }
    kafka_List_t* recs = NULL;
    int failed = ffi_raise((recs = kafka_consumer_ConsumerRecords_records_with_partition_err(self->records, tp, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err)));
    if (failed || append_record_list(self, recs, out) < 0) { Py_DECREF(out); return NULL; }
    return out;
}

// ConsumerRecords.records_with_topic(topic) -> list[ConsumerRecord]
static PyObject* ConsumerRecords_records_with_topic(ConsumerRecordsObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* topic;
    if (!PyArg_ParseTuple(args, "s", &topic)) return NULL;
    PyObject* out = PyList_New(0);
    if (out == NULL) return NULL;
    kafka_List_t* recs = NULL;
    if (ffi_raise((recs = kafka_consumer_ConsumerRecords_records_with_topic_err(self->records, topic, &ffi_err), ffi_take(&ffi_err)))
        || append_record_list(self, recs, out) < 0) { Py_DECREF(out); return NULL; }
    return out;
}

// ConsumerRecords.next_offsets() -> {(topic, partition): (offset, metadata, leader_epoch | None)}
static PyObject* ConsumerRecords_next_offsets(ConsumerRecordsObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_Map_t* offsets = NULL;
    if (ffi_raise((offsets = kafka_consumer_ConsumerRecords_next_offsets_err(self->records, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return oam_map_to_py(offsets);
}

static PyObject* ConsumerRecords_count(ConsumerRecordsObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t count = 0;
    if (ffi_raise((count = kafka_consumer_ConsumerRecords_count_err(self->records, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLong(count);
}

// Builds `self->flat` (see ConsumerRecordsObject). Returns 0 on success.
static int ConsumerRecords_build_flat(ConsumerRecordsObject* self) {
    kafka_common_Error_t *ffi_err = NULL;
    if (self->flat != NULL) return 0;
    int32_t total = 0;
    if (ffi_raise((total = kafka_consumer_ConsumerRecords_count_err(self->records, &ffi_err), ffi_take(&ffi_err)))) return -1;
    const kafka_consumer_ConsumerRecord_t** flat =
        PyMem_Calloc(total > 0 ? (size_t)total : 1, sizeof(*flat));
    if (flat == NULL) { PyErr_NoMemory(); return -1; }
    Py_ssize_t len = 0;
    int failed = 0;
    kafka_List_t* parts = NULL;  // owned, owns its TPs
    if (ffi_raise((parts = kafka_consumer_ConsumerRecords_partitions_err(self->records, &ffi_err), ffi_take(&ffi_err)))) failed = 1;
    if (parts != NULL) {
        int32_t n = 0;
        if (ffi_raise((n = kafka_List_size_err(parts, &ffi_err), ffi_take(&ffi_err)))) failed = 1;
        for (int32_t i = 0; !failed && i < n; i++) {
            void* tp = NULL;
            kafka_List_t* recs = NULL;
            if (ffi_raise((tp = kafka_List_get_err(parts, i, &ffi_err), ffi_take(&ffi_err)))
                || ffi_raise((recs = kafka_consumer_ConsumerRecords_records_with_partition_err(
                       self->records, (const kafka_common_TopicPartition_t*)tp, &ffi_err), ffi_take(&ffi_err)))) {
                failed = 1;
                break;
            }
            if (recs == NULL) continue;
            int32_t m = 0;
            if (ffi_raise((m = kafka_List_size_err(recs, &ffi_err), ffi_take(&ffi_err)))) failed = 1;
            for (int32_t j = 0; !failed && j < m && len < total; j++) {
                void* rec = NULL;
                if (ffi_raise((rec = kafka_List_get_err(recs, j, &ffi_err), ffi_take(&ffi_err)))) { failed = 1; break; }
                flat[len++] = (const kafka_consumer_ConsumerRecord_t*)rec;
            }
            ffi_unraisable((kafka_List_destroy_err(recs, &ffi_err), ffi_take(&ffi_err)));  // the list only: the records stay in the batch
        }
        ffi_unraisable((kafka_List_destroy_err(parts, &ffi_err), ffi_take(&ffi_err)));
    }
    if (failed) { PyMem_Free((void*)flat); return -1; }
    self->flat = flat;
    self->flat_len = len;
    return 0;
}

// ConsumerRecords.get(i) -> ConsumerRecord: the i-th record in Java's iterator
// order.
static PyObject* ConsumerRecords_get(ConsumerRecordsObject* self, PyObject* args) {
    Py_ssize_t i;
    if (!PyArg_ParseTuple(args, "n", &i)) return NULL;
    if (ConsumerRecords_build_flat(self) < 0) return NULL;
    if (i < 0 || i >= self->flat_len) {
        PyErr_SetString(PyExc_IndexError, "record index out of range");
        return NULL;
    }
    return wrap_record(self, self->flat[i]);
}

static PyObject* ConsumerRecords_is_empty(ConsumerRecordsObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    int8_t empty = 0;
    if (ffi_raise((empty = kafka_consumer_ConsumerRecords_is_empty_err(self->records, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(empty ? 1 : 0);
}

static PyMethodDef ConsumerRecords_methods[] = {
    {"count", (PyCFunction)ConsumerRecords_count, METH_NOARGS, "Number of records"},
    {"is_empty", (PyCFunction)ConsumerRecords_is_empty, METH_NOARGS, "Whether the batch is empty"},
    {"get", (PyCFunction)ConsumerRecords_get, METH_VARARGS, "The i-th record, in iterator order"},
    {"records", (PyCFunction)ConsumerRecords_records, METH_NOARGS, "All records, partition by partition"},
    {"partitions", (PyCFunction)ConsumerRecords_partitions, METH_NOARGS, "Partitions with records"},
    {"records_with_partition", (PyCFunction)ConsumerRecords_records_with_partition, METH_VARARGS,
     "Records of one partition"},
    {"records_with_topic", (PyCFunction)ConsumerRecords_records_with_topic, METH_VARARGS,
     "Records of one topic"},
    {"next_offsets", (PyCFunction)ConsumerRecords_next_offsets, METH_NOARGS, "Next offsets per partition"},
    {NULL}
};

static PyTypeObject ConsumerRecordsType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_confluentkafka.ConsumerRecords",
    .tp_doc = "An owned batch of consumed records",
    .tp_basicsize = sizeof(ConsumerRecordsObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)ConsumerRecords_dealloc,
    .tp_methods = ConsumerRecords_methods,
};

// Wrap an OWNED records handle into a ConsumerRecordsObject (NULL -> None);
// `copy_bytes` for a MockConsumer's batch (see ConsumerRecordsObject).
static PyObject* wrap_records(kafka_consumer_ConsumerRecords_t* records, int copy_bytes) {
    kafka_common_Error_t *ffi_err = NULL;
    if (records == NULL) Py_RETURN_NONE;
    ConsumerRecordsObject* obj = PyObject_New(ConsumerRecordsObject, &ConsumerRecordsType);
    if (obj == NULL) {
        ffi_unraisable((kafka_consumer_ConsumerRecords_destroy_err(records, &ffi_err), ffi_take(&ffi_err)));
        return NULL;
    }
    obj->records = records;
    obj->copy_bytes = copy_bytes;
    obj->flat = NULL;
    obj->flat_len = 0;
    return (PyObject*)obj;
}

// ---- the consumer handle ---------------------------------------------------

// The `void *self` registered with kafka_consumer_ConsumerRebalanceListener_new_err.
// Owns one reference to the Python adapter (consumer.py's _ListenerAdapter).
// Lives until the registration that copied it is replaced by a later
// subscribe*, or until the consumer is destroyed (an unsubscribe leaves the
// Java listener registered, so the context survives it too).
typedef struct {
    PyObject* adapter;
    kafka_consumer_Consumer_t* consumer;  // reports through __set_callback_result
} ListenerCtx;

// The `void *self` registered with kafka_consumer_OffsetCommitCallback_new_err.
// Lives until `onComplete` fired. When the commit that took it returned Err
// the callback may never fire, so the context is parked on the consumer's
// orphan list and reclaimed at destroy (after `_destroy` ran every pending
// callback): a late `onComplete` finds it alive, a never-firing one is freed
// with the consumer.
typedef struct CommitCbCtx {
    PyObject* adapter;                    // callable(offsets_dict, error), or NULL to discard
    kafka_consumer_Consumer_t* consumer;
    int orphaned;
    struct CommitCbCtx* next;
} CommitCbCtx;

// A key or value buffer handed to the mock's add_record: the records the mock
// hands out share the pointer (CLAUDE.md §4: the caller keeps it alive until
// the records holding it are destroyed), so it is pinned until the consumer is
// destroyed.
typedef struct PinnedBytes {
    kafka_Bytes_t bytes;
    struct PinnedBytes* next;
    uint8_t data[];
} PinnedBytes;

typedef struct {
    int is_mock;
    kafka_consumer_MockConsumer_t* mc;     // owned when is_mock
    kafka_consumer_Consumer_t* consumer;   // borrowed __as_Consumer view (mock) or owned (KafkaConsumer_new)
    PyObject* notify_cb;                   // asyncio pump scheduler, or NULL
    ListenerCtx* listener;                 // the registered listener's `self`, or NULL
    CommitCbCtx* orphans;                  // see CommitCbCtx
    PinnedBytes* pinned;                   // see PinnedBytes
} Consumer;

static Consumer* consumer_from_handle(unsigned long long h) {
    return (Consumer*)(uintptr_t)h;
}

static kafka_consumer_Consumer_t* consumer_self(unsigned long long h) {
    return consumer_from_handle(h)->consumer;
}

static const kafka_consumer_ConsumerHandle_t* handle_self(unsigned long long h) {
    return (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
}

static ListenerCtx* listener_ctx_new(PyObject* adapter, kafka_consumer_Consumer_t* consumer) {
    ListenerCtx* ctx = (ListenerCtx*)PyMem_Malloc(sizeof(ListenerCtx));
    if (ctx == NULL) { PyErr_NoMemory(); return NULL; }
    Py_INCREF(adapter);
    ctx->adapter = adapter;
    ctx->consumer = consumer;
    return ctx;
}

static void listener_ctx_free(ListenerCtx* ctx) {
    if (ctx == NULL) return;
    Py_DECREF(ctx->adapter);
    PyMem_Free(ctx);
}

// Settles the listener registration after a subscribe* completed: on success
// the new context (NULL for a listenerless subscribe, Java's NoOp listener)
// replaces the old one, which Rust no longer references; on failure the old
// registration stands and the new context is dropped. GIL held.
static void consumer_listener_commit(Consumer* c, ListenerCtx* new_ctx, int failed) {
    if (failed) {
        listener_ctx_free(new_ctx);
        return;
    }
    listener_ctx_free(c->listener);
    c->listener = new_ctx;
}

static CommitCbCtx* commit_ctx_new(Consumer* c, PyObject* adapter) {
    CommitCbCtx* ctx = (CommitCbCtx*)PyMem_Malloc(sizeof(CommitCbCtx));
    if (ctx == NULL) { PyErr_NoMemory(); return NULL; }
    Py_XINCREF(adapter);
    ctx->adapter = adapter;
    ctx->consumer = c->consumer;
    ctx->orphaned = 0;
    ctx->next = NULL;
    return ctx;
}

static void commit_ctx_free(CommitCbCtx* ctx) {
    Py_XDECREF(ctx->adapter);
    PyMem_Free(ctx);
}

static void consumer_orphan_commit(Consumer* c, CommitCbCtx* ctx) {
    ctx->orphaned = 1;
    ctx->next = c->orphans;
    c->orphans = ctx;
}

// ---- rebalance-listener trampolines ----------------------------------------
//
// Invoked by Rust on the calling thread (blocking entry point) or from
// Consumer__execute_callbacks (`_cb` entry point), both with the GIL released by
// the wrapper. The adapter method receives the partitions as
// list[(topic, partition)] plus the callback id and returns:
//   * None  -> the listener returned: report success now;
//   * True  -> the adapter deferred (coroutine scheduled on the loop) and will
//              report through Consumer__set_callback_result itself;
//   * raise -> report the exception as a foreign callback error that owns a
//              reference to the exception object (released when the last copy
//              of the error is dropped); the client wraps it like a Java
//              listener's Throwable and the consumer swaps the original back in
//              when the operation fails with it.
static void listener_fire(const char* method, void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    kafka_common_Error_t *ffi_err = NULL;
    ListenerCtx* ctx = (ListenerCtx*)self_;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) {
        // The listener did not run, so the rebalance fails as if it had raised.
        ffi_discard_without_gil((kafka_consumer_Consumer__set_callback_result_err(ctx->consumer, callback_id, NULL,
            interpreter_finalizing_error("rebalance listener"), &ffi_err), ffi_take(&ffi_err)));
        return;
    }
    kafka_common_Error_t* err = NULL;
    int deferred = 0;
    PyObject* py_parts = tp_list_to_py_borrowed(partitions);
    if (py_parts == NULL) {
        err = error_from_py_exception("failed to convert the rebalance partitions");
    } else {
        PyObject* r = PyObject_CallMethod(ctx->adapter, method, "OL", py_parts, (long long)callback_id);
        Py_DECREF(py_parts);
        if (r == NULL) {
            err = callback_error_from_py_exception();
        } else {
            deferred = (r == Py_True);
            Py_DECREF(r);
        }
    }
    if (!deferred) ffi_unraisable((kafka_consumer_Consumer__set_callback_result_err(ctx->consumer, callback_id, NULL, err, &ffi_err), ffi_take(&ffi_err)));
    PyGILState_Release(g);
}

static void listener_on_revoked(void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    listener_fire("_on_revoked", self_, partitions, callback_id);
}

static void listener_on_assigned(void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    listener_fire("_on_assigned", self_, partitions, callback_id);
}

static void listener_on_lost(void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    listener_fire("_on_lost", self_, partitions, callback_id);
}

// Builds the listener registration for `adapter` (None -> no listener). On
// success *out_ctx / *out_listener are set (both NULL for None); returns 0.
static int listener_build(Consumer* c, PyObject* adapter, ListenerCtx** out_ctx,
                          kafka_consumer_ConsumerRebalanceListener_t** out_listener) {
    kafka_common_Error_t *ffi_err = NULL;
    *out_ctx = NULL;
    *out_listener = NULL;
    if (adapter == Py_None) return 0;
    ListenerCtx* ctx = listener_ctx_new(adapter, c->consumer);
    if (ctx == NULL) return -1;
    if (ffi_raise((*(out_listener) = kafka_consumer_ConsumerRebalanceListener_new_err(
            ctx, listener_on_revoked, listener_on_assigned, listener_on_lost, &ffi_err), ffi_take(&ffi_err)))) {
        listener_ctx_free(ctx);
        return -1;
    }
    *out_ctx = ctx;
    return 0;
}

// ---- commit-callback trampoline ---------------------------------------------
//
// Java's OffsetCommitCallback.onComplete returns void, so the report carries
// no result, only the error it raised. The adapter (consumer/_base.py's _wrap_commit_callback) receives
// the committed offsets, the BORROWED error handle and the callback id, and
// follows listener_fire's split:
//   * None  -> the callback returned: report success now;
//   * True  -> the consumer deferred the awaitable an `async def` callback
//              returned (AsyncConsumer) and reports through
//              Consumer__set_callback_result once it is awaited;
//   * raise -> report it as a foreign callback error owning a reference to
//              the exception: the core propagates it out of the delivering
//              call, as Java's onComplete is called without a try, and the
//              call reads the exception back (consumer/_base.py's
//              _raise_error).
// The core waits for the report before it delivers the next commit callback.
// The context is released after the call -- unless orphaned, see above; a
// deferred report goes through the consumer's handle, not the context.
static void commit_on_complete(void* self_, const kafka_Map_t* offsets,
                               const kafka_common_Error_t* error, int64_t callback_id) {
    kafka_common_Error_t *ffi_err = NULL;
    CommitCbCtx* ctx = (CommitCbCtx*)self_;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) {
        // `ctx` is leaked with the rest; the callback did not run, so the
        // delivering call fails as if it had raised.
        ffi_discard_without_gil((kafka_consumer_Consumer__set_callback_result_err(ctx->consumer, callback_id, NULL,
            interpreter_finalizing_error("commit callback"), &ffi_err), ffi_take(&ffi_err)));
        return;
    }
    kafka_common_Error_t* err = NULL;
    int deferred = 0;
    if (ctx->adapter != NULL) {
        PyObject* py_offsets = offset_map_to_py_borrowed(offsets);
        PyObject* py_err = error_handle_to_py(error);  // borrowed: converted, never destroyed
        PyObject* py_id = PyLong_FromLongLong((long long)callback_id);
        if (py_offsets != NULL && py_err != NULL && py_id != NULL) {
            PyObject* r = PyObject_CallFunctionObjArgs(ctx->adapter, py_offsets, py_err, py_id, NULL);
            if (r == NULL) {
                err = callback_error_from_py_exception();
            } else {
                deferred = (r == Py_True);
                Py_DECREF(r);
            }
        } else {
            PyErr_WriteUnraisable(ctx->adapter);
        }
        Py_XDECREF(py_offsets);
        Py_XDECREF(py_err);
        Py_XDECREF(py_id);
    }
    if (!deferred) ffi_unraisable((kafka_consumer_Consumer__set_callback_result_err(ctx->consumer, callback_id, NULL, err, &ffi_err), ffi_take(&ffi_err)));
    if (!ctx->orphaned) commit_ctx_free(ctx);
    PyGILState_Release(g);
}

// ---- notify hook / pump ----------------------------------------------------

// Fired by Rust once each time the callbacks vector goes from empty to
// non-empty, from a Rust task. It only schedules. `notify_cb` is read without
// the GIL: it is set before any operation can queue a callback and released
// only after the handle is destroyed (no hook fires past that point).
static void consumer_callbacks_notify(void* opaque) {
    Consumer* c = (Consumer*)opaque;
    if (c->notify_cb == NULL) return;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* r = PyObject_CallNoArgs(c->notify_cb);
    if (r == NULL) PyErr_WriteUnraisable(c->notify_cb); else Py_DECREF(r);
    PyGILState_Release(g);
}

// Consumer__set_callbacks_notify(handle, callable | None): registers the Python
// callable the notify hook invokes. Call before the first `_cb` operation.
static PyObject* py_Consumer__set_callbacks_notify(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Consumer* c = consumer_from_handle(h);
    PyObject* old = c->notify_cb;
    if (cb == Py_None) {
        c->notify_cb = NULL;
    } else {
        Py_INCREF(cb);
        c->notify_cb = cb;
    }
    Py_XDECREF(old);
    Py_RETURN_NONE;
}

// Consumer__execute_callbacks(handle) -> int: runs the queued callbacks on this
// thread (GIL released around the drain; each callback re-acquires it).
static PyObject* py_Consumer__execute_callbacks(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    int32_t n = 0;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (n = kafka_consumer_Consumer__execute_callbacks_err(s, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (ffi_raise(err)) return NULL;
    return PyLong_FromLong((long)n);
}

// Consumer__set_callback_result(handle, callback_id, error): the deferred
// report of a listener or commit-callback invocation. None = success; an
// exception object = the exception the callback raised, reported as a foreign
// callback error owning a reference to it; a str = a message, reported as a
// KafkaException. The listener methods and onComplete are void, so the C
// `result` is NULL.
static PyObject* py_Consumer__set_callback_result(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long callback_id; PyObject* error;
    if (!PyArg_ParseTuple(args, "KLO", &h, &callback_id, &error)) return NULL;
    kafka_common_Error_t* err = NULL;
    if (PyExceptionInstance_Check(error)) {
        err = callback_error_from_py(error);
    } else if (error != Py_None) {
        const char* msg = PyUnicode_AsUTF8(error);
        if (msg == NULL) return NULL;
        err = ffi_built(kafka_common_Error_kafka_message_err(msg, &ffi_err), &ffi_err);
    }
    if (ffi_raise((kafka_consumer_Consumer__set_callback_result_err(consumer_self(h), (int64_t)callback_id, NULL, err, &ffi_err), ffi_take(&ffi_err)))) {
        return NULL;
    }
    Py_RETURN_NONE;
}

// ---- constructors / lifecycle ----------------------------------------------

static Consumer* consumer_alloc(void) {
    Consumer* c = (Consumer*)PyMem_Malloc(sizeof(Consumer));
    if (c == NULL) { PyErr_NoMemory(); return NULL; }
    memset(c, 0, sizeof(*c));
    return c;
}

// Consumer_MockConsumer_new(offset_reset_strategy) -> handle. Raises
// RuntimeError with the Rust error's message for an unknown strategy.
static PyObject* py_Consumer_MockConsumer_new(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* auto_offset_reset;
    if (!PyArg_ParseTuple(args, "s", &auto_offset_reset)) return NULL;
    Consumer* c = consumer_alloc();
    if (c == NULL) return NULL;
    kafka_common_Error_t* err = (c->mc = kafka_consumer_MockConsumer_new(auto_offset_reset, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        const char* msg = NULL;
        if (!ffi_raise((msg = kafka_common_Error_message_err(err, &ffi_err), ffi_take(&ffi_err)))) {
            PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create MockConsumer");
        }
        ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
        PyMem_Free(c);
        return NULL;
    }
    c->is_mock = 1;
    if (ffi_raise((c->consumer = kafka_consumer_MockConsumer__as_Consumer_err(c->mc, &ffi_err), ffi_take(&ffi_err)))  // borrowed view
        || ffi_raise((kafka_consumer_Consumer__set_callbacks_notify_err(c->consumer, consumer_callbacks_notify, c, &ffi_err), ffi_take(&ffi_err)))) {
        ffi_unraisable((kafka_consumer_MockConsumer_destroy_err(c->mc, &ffi_err), ffi_take(&ffi_err)));
        PyMem_Free(c);
        return NULL;
    }
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)c);
}

// Consumer_KafkaConsumer_new(config: dict[str, str]) -> (handle, error): NULL
// deserializers, so records carry `kafka_Bytes_t *` keys and values. On failure
// the handle is 0 and `error` the owned error handle (an int, 0 on success);
// the construction runs with the GIL released (it resolves the bootstrap
// servers).
static PyObject* py_Consumer_KafkaConsumer_new(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* config_dict;
    if (!PyArg_ParseTuple(args, "O!", &PyDict_Type, &config_dict)) return NULL;
    // A C-built kafka_Map_t holds BORROWED char* -> char*: the strings stay
    // owned by the dict's str objects, which outlive this call.
    kafka_Map_t* props = NULL;
    if (ffi_raise((props = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_Check(key) ? str_utf8_borrowed(key) : NULL;
        const char* v = (k != NULL && PyUnicode_Check(value)) ? str_utf8_borrowed(value) : NULL;
        if (k == NULL || v == NULL || ffi_raise((kafka_Map_put_err(props, (void*)k, (void*)v, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_Map_destroy_err(props, &ffi_err), ffi_take(&ffi_err)));
            if (!PyErr_Occurred())
                PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
    }
    kafka_consumer_ConsumerConfig_t* config = NULL;
    kafka_common_Error_t* err = (config = kafka_consumer_ConsumerConfig_new(props, &ffi_err), ffi_take(&ffi_err));
    ffi_unraisable((kafka_Map_destroy_err(props, &ffi_err), ffi_take(&ffi_err)));
    kafka_consumer_Consumer_t* consumer = NULL;
    if (err == NULL) {
        kafka_common_Error_t* destroy_err;
        Py_BEGIN_ALLOW_THREADS
        err = (consumer = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &ffi_err), ffi_take(&ffi_err));  // config copied
        Py_END_ALLOW_THREADS
        destroy_err = (kafka_consumer_ConsumerConfig_destroy_err(config, &ffi_err), ffi_take(&ffi_err));
        ffi_unraisable(destroy_err);
    }
    if (err != NULL) {
        PyObject* py_err = error_handle_to_py(err);
        if (py_err == NULL) { ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        return Py_BuildValue("(iN)", 0, py_err);
    }
    Consumer* c = consumer_alloc();
    if (c != NULL) {
        c->consumer = consumer;  // owned
        if (ffi_raise((kafka_consumer_Consumer__set_callbacks_notify_err(c->consumer, consumer_callbacks_notify, c, &ffi_err), ffi_take(&ffi_err)))) {
            PyMem_Free(c);
            c = NULL;
        }
    }
    if (c == NULL) {
        Py_BEGIN_ALLOW_THREADS
        err = (kafka_consumer_Consumer_destroy_err(consumer, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
        ffi_unraisable(err);
        return NULL;
    }
    return Py_BuildValue("(Ki)", (unsigned long long)(uintptr_t)c, 0);
}

// Consumer_destroy(handle): destroys the Rust consumer (which runs every still
// pending callback, hence the released GIL), then reclaims the C contexts no
// callback can reference any more.
static PyObject* py_Consumer_destroy(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    Consumer* c = consumer_from_handle(h);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (c->is_mock) err = (kafka_consumer_MockConsumer_destroy_err(c->mc, &ffi_err), ffi_take(&ffi_err));
    else err = (kafka_consumer_Consumer_destroy_err(c->consumer, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    // The handle is freed even after a panic: report it, release the C side.
    ffi_unraisable(err);
    listener_ctx_free(c->listener);
    while (c->orphans != NULL) {
        CommitCbCtx* next = c->orphans->next;
        commit_ctx_free(c->orphans);
        c->orphans = next;
    }
    while (c->pinned != NULL) {
        PinnedBytes* next = c->pinned->next;
        PyMem_Free(c->pinned);
        c->pinned = next;
    }
    Py_XDECREF(c->notify_cb);
    PyMem_Free(c);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_wakeup(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    if (ffi_raise((kafka_consumer_Consumer_wakeup_err(consumer_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    Py_RETURN_NONE;
}

// ---- sync state reads -------------------------------------------------------
//
// Each returns (value | None, err | None), as poll does: assignment,
// subscription, paused, current_lag and group_metadata fail with
// LOCAL_CONCURRENT_MODIFICATION while another call holds the consumer (Java's
// acquire() throws ConcurrentModificationException); metrics, which Java reads
// without acquire(), answers directly.

// A (value, err) pair for an owned list read: the error when `err` is set,
// else `convert(list)` (which frees the list).
static PyObject* list_read_result(kafka_common_Error_t* err, kafka_List_t* list,
                                  PyObject* (*convert)(kafka_List_t*)) {
    kafka_common_Error_t *ffi_err = NULL;
    if (err != NULL) {
        if (list) ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err)));
        return cvalue_error(Py_None, err);
    }
    PyObject* value = convert(list);
    if (value == NULL) return NULL;
    PyObject* ret = cvalue_error(value, NULL);
    Py_DECREF(value);
    return ret;
}

// Consumer_assignment(h) -> (list[(topic, partition)] | None, err | None)
static PyObject* py_Consumer_assignment(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = (list = kafka_consumer_Consumer_assignment(consumer_self(h), &ffi_err), ffi_take(&ffi_err));
    return list_read_result(err, list, tp_list_to_py);
}

// Consumer_paused(h) -> (list[(topic, partition)] | None, err | None)
static PyObject* py_Consumer_paused(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = (list = kafka_consumer_Consumer_paused(consumer_self(h), &ffi_err), ffi_take(&ffi_err));
    return list_read_result(err, list, tp_list_to_py);
}

// Consumer_subscription(h) -> (list[str] | None, err | None)
static PyObject* py_Consumer_subscription(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = (list = kafka_consumer_Consumer_subscription(consumer_self(h), &ffi_err), ffi_take(&ffi_err));
    return list_read_result(err, list, string_list_to_py);
}

// Consumer_metrics -> list[dict] (name/group/description/tags/value/kind), the
// same shape as Producer_metrics; answers while an operation is in flight.
static PyObject* py_Consumer_metrics(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_Map_t* metrics = NULL;
    if (ffi_raise((metrics = kafka_consumer_Consumer_metrics_err(consumer_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return metrics_map_to_py(metrics);
}

// Consumer_group_metadata(h) -> (ConsumerGroupMetadata | None, err | None); the
// object RETAINS the owned Rust handle (freed in its tp_dealloc).
static PyObject* py_Consumer_group_metadata(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerGroupMetadata_t* m = NULL;
    kafka_common_Error_t* err = (m = kafka_consumer_Consumer_group_metadata(consumer_self(h), &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        if (m) ffi_unraisable((kafka_consumer_ConsumerGroupMetadata_destroy_err(m, &ffi_err), ffi_take(&ffi_err)));
        return cvalue_error(Py_None, err);
    }
    ConsumerGroupMetadataObject* obj =
        PyObject_New(ConsumerGroupMetadataObject, &ConsumerGroupMetadataType);
    if (obj == NULL) {
        ffi_unraisable((kafka_consumer_ConsumerGroupMetadata_destroy_err(m, &ffi_err), ffi_take(&ffi_err)));
        return NULL;
    }
    obj->handle = m;
    PyObject* ret = cvalue_error((PyObject*)obj, NULL);
    Py_DECREF(obj);
    return ret;
}

static PyObject* py_Consumer_client_id(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    const char* s = NULL;  // borrowed
    if (ffi_raise((s = kafka_consumer_Consumer_client_id_err(consumer_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    if (s == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(s);
}

// Consumer_current_lag(handle, topic, partition) -> (int | None, err | None),
// None for an empty lag (-1)
static PyObject* py_Consumer_current_lag(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* topic; int partition;
    if (!PyArg_ParseTuple(args, "Ksi", &h, &topic, &partition)) return NULL;
    kafka_common_TopicPartition_t* tp = NULL;
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    int64_t lag = -1;
    kafka_common_Error_t* err = (lag = kafka_consumer_Consumer_current_lag(consumer_self(h), tp, &ffi_err), ffi_take(&ffi_err));
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err)));
    if (err != NULL) return cvalue_error(Py_None, err);
    if (lag < 0) return cvalue_error(Py_None, NULL);
    PyObject* value = PyLong_FromLongLong((long long)lag);
    if (value == NULL) return NULL;
    PyObject* ret = cvalue_error(value, NULL);
    Py_DECREF(value);
    return ret;
}

// ---- `_cb` completion trampolines ------------------------------------------
//
// Each runs from Consumer__execute_callbacks (GIL released by that wrapper), or
// inline from a ConsumerHandle `_cb` whose consumer is already destroyed, and
// hands copied Python values to the registered callable. The void shape reuses
// the producer's VoidCbCtx / producer_void_cb.

// Delivers cb(value, error); `value` is a new reference (or NULL with an
// exception set), `err` an OWNED error or NULL, handed to the callable as an
// int it takes ownership of (0 for none). When the call cannot be made the
// error is destroyed here.
static void deliver_value_cb(PyObject* cb, PyObject* value, kafka_common_Error_t* err) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* py_err = value != NULL ? error_handle_to_py(err) : NULL;
    if (value != NULL && py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(cb, value, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(cb); else Py_DECREF(r);
    } else {
        PyErr_WriteUnraisable(cb);
        if (err != NULL) ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
    }
    Py_XDECREF(value);
    Py_XDECREF(py_err);
}

// A poll's completion context: the callable and whether the batch copies its
// bytes (a MockConsumer's, see ConsumerRecordsObject).
typedef struct {
    PyObject* cb;
    int copy_bytes;
} PollCbCtx;

// poll: cb(ConsumerRecords | None, error)
static void consumer_poll_cb(kafka_consumer_ConsumerRecords_t* records, kafka_common_Error_t* error, void* opaque) {
    PollCbCtx* ctx = (PollCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    deliver_value_cb(ctx->cb, wrap_records(records, ctx->copy_bytes), error);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// position: cb(int, error)   (-1 beside an error)
static void consumer_i64_cb(int64_t value, kafka_common_Error_t* error, void* opaque) {
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    deliver_value_cb(ctx->cb, PyLong_FromLongLong((long long)value), error);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

typedef PyObject* (*owned_map_conv_fn)(kafka_Map_t*);
typedef PyObject* (*owned_list_conv_fn)(kafka_List_t*);

typedef struct {
    PyObject* cb;
    owned_map_conv_fn conv_map;
    owned_list_conv_fn conv_list;
} ValueCbCtx;

static ValueCbCtx* value_cb_ctx_new(PyObject* cb, owned_map_conv_fn conv_map, owned_list_conv_fn conv_list) {
    ValueCbCtx* ctx = (ValueCbCtx*)PyMem_Malloc(sizeof(ValueCbCtx));
    if (ctx == NULL) { PyErr_NoMemory(); return NULL; }
    Py_INCREF(cb);
    ctx->cb = cb;
    ctx->conv_map = conv_map;
    ctx->conv_list = conv_list;
    return ctx;
}

// map results: cb(dict | None, error); the owned map is converted
// and freed here.
static void consumer_map_cb(kafka_Map_t* value, kafka_common_Error_t* error, void* opaque) {
    ValueCbCtx* ctx = (ValueCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* py_value = value ? ctx->conv_map(value) : (Py_INCREF(Py_None), Py_None);
    deliver_value_cb(ctx->cb, py_value, error);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// list results: cb(list | None, error); the owned list is converted
// and freed here.
static void consumer_list_cb(kafka_List_t* value, kafka_common_Error_t* error, void* opaque) {
    ValueCbCtx* ctx = (ValueCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* py_value = value ? ctx->conv_list(value) : (Py_INCREF(Py_None), Py_None);
    deliver_value_cb(ctx->cb, py_value, error);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// ---- operation wrappers ----------------------------------------------------
//
// Every operation comes as a pair: `py_<name>` calls the blocking FFI entry
// point with the GIL released and returns the result (an err tuple | None, or a
// (value, err tuple | None) pair); `py_<name>_cb` takes a trailing callable and
// submits the `_cb` twin, whose completion the pump delivers to the callable
// with the same payload. The same shapes serve the Consumer (`consumer_self`)
// and the ConsumerHandle (`handle_self`), hence the macros.

// Shape: (self) -> err. py(h) / py_cb(h, cb)
#define DEF_NOARG_VOID_OPS(pyname, SELF_T, SELF_FN, OP, OP_CB)                          \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h;                                                               \
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;                                  \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP(s, &err);                                                                        \
    Py_END_ALLOW_THREADS                                                                \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* cb;                                                 \
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;                            \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) return NULL;                                                       \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_CB(s, producer_void_cb, ctx);                                                    \
    Py_END_ALLOW_THREADS                                                                \
    Py_RETURN_NONE;                                                                     \
}

// Shape: (self, list<TopicPartition>) -> err. py(h, tps) / py_cb(h, tps, cb)
#define DEF_TPLIST_VOID_OPS(pyname, SELF_T, SELF_FN, OP, OP_CB)                         \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h; PyObject* tps;                                                \
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;                           \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP(s, l.list, &err);                                                                \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* tps; PyObject* cb;                                  \
    if (!PyArg_ParseTuple(args, "KOO", &h, &tps, &cb)) return NULL;                     \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) { tp_list_free(&l); return NULL; }                                 \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_CB(s, l.list, producer_void_cb, ctx);                                            \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    Py_RETURN_NONE;                                                                     \
}

// Shape: (self, list<TopicPartition>[, timeout_ms]) -> map. A negative
// timeout_ms selects the Java overload without a timeout (the `_NT` variant
// below serves the ConsumerHandle, whose Java methods have no timeout form).
// py(h, tps, ms) -> (dict | None, err) / py_cb(h, tps, ms, cb)
#define DEF_TPLIST_MAP_OPS(pyname, SELF_T, SELF_FN, OP, OP_T, OP_CB, OP_T_CB, CONV)     \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; PyObject* tps; long long ms;                                  \
    if (!PyArg_ParseTuple(args, "KOL", &h, &tps, &ms)) return NULL;                     \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) out = OP_T(s, l.list, (int64_t)ms, &err);                              \
    else out = OP(s, l.list, &err);                                                     \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    if (err != NULL) { if (out) ffi_unraisable((kafka_Map_destroy_err(out, &ffi_err), ffi_take(&ffi_err))); return cvalue_error(Py_None, err); } \
    PyObject* value = CONV(out);                                                        \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = cvalue_error(value, NULL);                                          \
    Py_DECREF(value);                                                                   \
    return ret;                                                                         \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* tps; long long ms; PyObject* cb;                    \
    if (!PyArg_ParseTuple(args, "KOLO", &h, &tps, &ms, &cb)) return NULL;               \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    ValueCbCtx* ctx = value_cb_ctx_new(cb, CONV, NULL);                                 \
    if (ctx == NULL) { tp_list_free(&l); return NULL; }                                 \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) OP_T_CB(s, l.list, (int64_t)ms, consumer_map_cb, ctx);                 \
    else OP_CB(s, l.list, consumer_map_cb, ctx);                                        \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    Py_RETURN_NONE;                                                                     \
}

// Shape: offsets_for_times: (self, map<TopicPartition, int64>[, timeout_ms]) -> map.
// py(h, [(t, p, ts)], ms) -> (dict | None, err) / py_cb(h, spec, ms, cb)
#define DEF_TIMESTAMPS_MAP_OPS(pyname, SELF_T, SELF_FN, OP, OP_T, OP_CB, OP_T_CB)       \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; PyObject* spec; long long ms;                                 \
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &ms)) return NULL;                    \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) out = OP_T(s, m.map, (int64_t)ms, &err);                               \
    else out = OP(s, m.map, &err);                                                      \
    Py_END_ALLOW_THREADS                                                                \
    tp_i64_map_free(&m);                                                                \
    if (err != NULL) { if (out) ffi_unraisable((kafka_Map_destroy_err(out, &ffi_err), ffi_take(&ffi_err))); return cvalue_error(Py_None, err); } \
    PyObject* value = oat_map_to_py(out);                                               \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = cvalue_error(value, NULL);                                          \
    Py_DECREF(value);                                                                   \
    return ret;                                                                         \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* spec; long long ms; PyObject* cb;                   \
    if (!PyArg_ParseTuple(args, "KOLO", &h, &spec, &ms, &cb)) return NULL;              \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    ValueCbCtx* ctx = value_cb_ctx_new(cb, oat_map_to_py, NULL);                        \
    if (ctx == NULL) { tp_i64_map_free(&m); return NULL; }                              \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) OP_T_CB(s, m.map, (int64_t)ms, consumer_map_cb, ctx);                  \
    else OP_CB(s, m.map, consumer_map_cb, ctx);                                         \
    Py_END_ALLOW_THREADS                                                                \
    tp_i64_map_free(&m);                                                                \
    Py_RETURN_NONE;                                                                     \
}

// Shape: seek(self, TopicPartition, int64 offset) -> err and
// seek(self, TopicPartition, OffsetAndMetadata) -> err.
// py_seek(h, topic, partition, offset) / py_seek_cb(h, topic, partition, offset, cb)
// py_seek_with_metadata(h, topic, partition, offset, leader_epoch, metadata | None)
#define DEF_SEEK_OPS(prefix, SELF_T, SELF_FN, OP, OP_CB, OP_M, OP_M_CB)                 \
static PyObject* py_##prefix##_seek(PyObject* self, PyObject* args) {                   \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; const char* topic; int partition; long long offset;           \
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &offset)) return NULL;  \
    kafka_common_TopicPartition_t* tp = NULL;                                           \
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) return NULL; \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP(s, tp, (int64_t)offset, &err);                                                   \
    Py_END_ALLOW_THREADS                                                                \
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##prefix##_seek_cb(PyObject* self, PyObject* args) {                \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; const char* topic; int partition; long long offset; PyObject* cb; \
    if (!PyArg_ParseTuple(args, "KsiLO", &h, &topic, &partition, &offset, &cb)) return NULL; \
    kafka_common_TopicPartition_t* tp = NULL;                                           \
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) return NULL; \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) { ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); return NULL; } \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_CB(s, tp, (int64_t)offset, producer_void_cb, ctx);                               \
    Py_END_ALLOW_THREADS                                                                \
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); \
    Py_RETURN_NONE;                                                                     \
}                                                                                       \
static PyObject* py_##prefix##_seek_with_metadata(PyObject* self, PyObject* args) {     \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; const char* topic; int partition; long long offset; int epoch; const char* metadata; \
    if (!PyArg_ParseTuple(args, "KsiLiz", &h, &topic, &partition, &offset, &epoch, &metadata)) return NULL; \
    kafka_consumer_OffsetAndMetadata_t* oam = NULL;                                     \
    kafka_common_Error_t* err = (oam = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata( \
        (int64_t)offset, (int32_t)epoch, metadata, &ffi_err), ffi_take(&ffi_err));      \
    if (err != NULL) return err_result(err);                                            \
    kafka_common_TopicPartition_t* tp = NULL;                                           \
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) { \
        ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(oam, &ffi_err), ffi_take(&ffi_err))); \
        return NULL;                                                                    \
    }                                                                                   \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_M(s, tp, oam, &err);                                                             \
    Py_END_ALLOW_THREADS                                                                \
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); \
    ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(oam, &ffi_err), ffi_take(&ffi_err))); \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##prefix##_seek_with_metadata_cb(PyObject* self, PyObject* args) {  \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; const char* topic; int partition; long long offset; int epoch; const char* metadata; PyObject* cb; \
    if (!PyArg_ParseTuple(args, "KsiLizO", &h, &topic, &partition, &offset, &epoch, &metadata, &cb)) return NULL; \
    kafka_consumer_OffsetAndMetadata_t* oam = NULL;                                     \
    kafka_common_Error_t* err = (oam = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata( \
        (int64_t)offset, (int32_t)epoch, metadata, &ffi_err), ffi_take(&ffi_err));      \
    if (err != NULL) {                                                                  \
        /* deliver the construction error through the callback like any other */        \
        PyObject* py_err = err_result(err);  /* owned: the callable takes it */         \
        if (py_err == NULL) { ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err))); return NULL; } \
        PyObject* r = PyObject_CallFunctionObjArgs(cb, py_err, NULL);                   \
        Py_DECREF(py_err);                                                              \
        if (r == NULL) return NULL;                                                     \
        Py_DECREF(r);                                                                   \
        Py_RETURN_NONE;                                                                 \
    }                                                                                   \
    kafka_common_TopicPartition_t* tp = NULL;                                           \
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) { \
        ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(oam, &ffi_err), ffi_take(&ffi_err))); \
        return NULL;                                                                    \
    }                                                                                   \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) {                                                                  \
        ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); \
        ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(oam, &ffi_err), ffi_take(&ffi_err))); \
        return NULL;                                                                    \
    }                                                                                   \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_M_CB(s, tp, oam, producer_void_cb, ctx);                                         \
    Py_END_ALLOW_THREADS                                                                \
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); \
    ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(oam, &ffi_err), ffi_take(&ffi_err))); \
    Py_RETURN_NONE;                                                                     \
}

// Shape: position(self, TopicPartition[, timeout_ms]) -> int64.
// py(h, topic, partition, ms) -> (int, err) / py_cb(h, topic, partition, ms, cb)
#define DEF_POSITION_OPS(pyname, SELF_T, SELF_FN, OP, OP_T, OP_CB, OP_T_CB)             \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; const char* topic; int partition; long long ms;               \
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &ms)) return NULL;      \
    kafka_common_TopicPartition_t* tp = NULL;                                           \
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) return NULL; \
    SELF_T s = SELF_FN(h);                                                              \
    int64_t pos = -1;                                                                   \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) pos = OP_T(s, tp, (int64_t)ms, &err);                                  \
    else pos = OP(s, tp, &err);                                                         \
    Py_END_ALLOW_THREADS                                                                \
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); \
    PyObject* value = PyLong_FromLongLong((long long)(err ? -1 : pos));                 \
    if (value == NULL) { if (err) ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err))); return NULL; } \
    PyObject* ret = cvalue_error(value, err);                                           \
    Py_DECREF(value);                                                                   \
    return ret;                                                                         \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; const char* topic; int partition; long long ms; PyObject* cb; \
    if (!PyArg_ParseTuple(args, "KsiLO", &h, &topic, &partition, &ms, &cb)) return NULL; \
    kafka_common_TopicPartition_t* tp = NULL;                                           \
    if (ffi_raise((tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err)))) return NULL; \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) { ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); return NULL; } \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) OP_T_CB(s, tp, (int64_t)ms, consumer_i64_cb, ctx);                     \
    else OP_CB(s, tp, consumer_i64_cb, ctx);                                            \
    Py_END_ALLOW_THREADS                                                                \
    ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err))); \
    Py_RETURN_NONE;                                                                     \
}

// Shape: commit_sync(self[, offsets][, timeout_ms]) -> err.
// py(h, spec | None, ms) / py_cb(h, spec | None, ms, cb). The `_NT` variant
// below serves the ConsumerHandle, whose Java methods have no timeout form.
#define DEF_COMMIT_SYNC_OPS(pyname, SELF_T, SELF_FN, OP, OP_T, OP_O, OP_OT, OP_CB, OP_T_CB, OP_O_CB, OP_OT_CB) \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h; PyObject* spec; long long ms;                                 \
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &ms)) return NULL;                    \
    offsets_map_t m; memset(&m, 0, sizeof(m));                                          \
    int with_offsets = spec != Py_None;                                                 \
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;                   \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (with_offsets) {                                                                 \
        if (ms >= 0) OP_OT(s, m.map, (int64_t)ms, &err);                                \
        else OP_O(s, m.map, &err);                                                      \
    } else {                                                                            \
        if (ms >= 0) OP_T(s, (int64_t)ms, &err);                                        \
        else OP(s, &err);                                                               \
    }                                                                                   \
    Py_END_ALLOW_THREADS                                                                \
    if (with_offsets) offsets_map_free(&m);                                             \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* spec; long long ms; PyObject* cb;                   \
    if (!PyArg_ParseTuple(args, "KOLO", &h, &spec, &ms, &cb)) return NULL;              \
    offsets_map_t m; memset(&m, 0, sizeof(m));                                          \
    int with_offsets = spec != Py_None;                                                 \
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;                   \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) { if (with_offsets) offsets_map_free(&m); return NULL; }           \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (with_offsets) {                                                                 \
        if (ms >= 0) OP_OT_CB(s, m.map, (int64_t)ms, producer_void_cb, ctx);            \
        else OP_O_CB(s, m.map, producer_void_cb, ctx);                                  \
    } else {                                                                            \
        if (ms >= 0) OP_T_CB(s, (int64_t)ms, producer_void_cb, ctx);                    \
        else OP_CB(s, producer_void_cb, ctx);                                           \
    }                                                                                   \
    Py_END_ALLOW_THREADS                                                                \
    if (with_offsets) offsets_map_free(&m);                                             \
    Py_RETURN_NONE;                                                                     \
}

// ---- Consumer operations ---------------------------------------------------

DEF_TPLIST_VOID_OPS(Consumer_assign, kafka_consumer_Consumer_t*, consumer_self,
                    kafka_consumer_Consumer_assign, kafka_consumer_Consumer_assign_cb)
DEF_TPLIST_VOID_OPS(Consumer_seek_to_beginning, kafka_consumer_Consumer_t*, consumer_self,
                    kafka_consumer_Consumer_seek_to_beginning, kafka_consumer_Consumer_seek_to_beginning_cb)
DEF_TPLIST_VOID_OPS(Consumer_seek_to_end, kafka_consumer_Consumer_t*, consumer_self,
                    kafka_consumer_Consumer_seek_to_end, kafka_consumer_Consumer_seek_to_end_cb)
DEF_TPLIST_VOID_OPS(Consumer_pause, kafka_consumer_Consumer_t*, consumer_self,
                    kafka_consumer_Consumer_pause, kafka_consumer_Consumer_pause_cb)
DEF_TPLIST_VOID_OPS(Consumer_resume, kafka_consumer_Consumer_t*, consumer_self,
                    kafka_consumer_Consumer_resume, kafka_consumer_Consumer_resume_cb)

DEF_NOARG_VOID_OPS(Consumer_unsubscribe, kafka_consumer_Consumer_t*, consumer_self,
                   kafka_consumer_Consumer_unsubscribe, kafka_consumer_Consumer_unsubscribe_cb)
DEF_NOARG_VOID_OPS(Consumer_enforce_rebalance, kafka_consumer_Consumer_t*, consumer_self,
                   kafka_consumer_Consumer_enforce_rebalance, kafka_consumer_Consumer_enforce_rebalance_cb)

DEF_TPLIST_MAP_OPS(Consumer_committed, kafka_consumer_Consumer_t*, consumer_self,
                   kafka_consumer_Consumer_committed, kafka_consumer_Consumer_committed_with_timeout,
                   kafka_consumer_Consumer_committed_cb, kafka_consumer_Consumer_committed_with_timeout_cb,
                   oam_map_to_py)
DEF_TPLIST_MAP_OPS(Consumer_beginning_offsets, kafka_consumer_Consumer_t*, consumer_self,
                   kafka_consumer_Consumer_beginning_offsets, kafka_consumer_Consumer_beginning_offsets_with_timeout,
                   kafka_consumer_Consumer_beginning_offsets_cb, kafka_consumer_Consumer_beginning_offsets_with_timeout_cb,
                   long_map_to_py)
DEF_TPLIST_MAP_OPS(Consumer_end_offsets, kafka_consumer_Consumer_t*, consumer_self,
                   kafka_consumer_Consumer_end_offsets, kafka_consumer_Consumer_end_offsets_with_timeout,
                   kafka_consumer_Consumer_end_offsets_cb, kafka_consumer_Consumer_end_offsets_with_timeout_cb,
                   long_map_to_py)
DEF_TIMESTAMPS_MAP_OPS(Consumer_offsets_for_times, kafka_consumer_Consumer_t*, consumer_self,
                       kafka_consumer_Consumer_offsets_for_times, kafka_consumer_Consumer_offsets_for_times_with_timeout,
                       kafka_consumer_Consumer_offsets_for_times_cb, kafka_consumer_Consumer_offsets_for_times_with_timeout_cb)

DEF_SEEK_OPS(Consumer, kafka_consumer_Consumer_t*, consumer_self,
             kafka_consumer_Consumer_seek_with_offset, kafka_consumer_Consumer_seek_with_offset_cb,
             kafka_consumer_Consumer_seek_with_offset_and_metadata, kafka_consumer_Consumer_seek_with_offset_and_metadata_cb)

DEF_POSITION_OPS(Consumer_position, kafka_consumer_Consumer_t*, consumer_self,
                 kafka_consumer_Consumer_position, kafka_consumer_Consumer_position_with_timeout,
                 kafka_consumer_Consumer_position_cb, kafka_consumer_Consumer_position_with_timeout_cb)

DEF_COMMIT_SYNC_OPS(Consumer_commit_sync, kafka_consumer_Consumer_t*, consumer_self,
                    kafka_consumer_Consumer_commit_sync, kafka_consumer_Consumer_commit_sync_with_timeout,
                    kafka_consumer_Consumer_commit_sync_with_offsets, kafka_consumer_Consumer_commit_sync_with_offsets_timeout,
                    kafka_consumer_Consumer_commit_sync_cb, kafka_consumer_Consumer_commit_sync_with_timeout_cb,
                    kafka_consumer_Consumer_commit_sync_with_offsets_cb, kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb)

// Consumer_enforce_rebalance_with_reason(h, reason) -> err | None
static PyObject* py_Consumer_enforce_rebalance_with_reason(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* reason;
    if (!PyArg_ParseTuple(args, "Ks", &h, &reason)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (kafka_consumer_Consumer_enforce_rebalance_with_reason(s, reason, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    return err_result(err);
}

static PyObject* py_Consumer_enforce_rebalance_with_reason_cb(PyObject* self, PyObject* args) {
    unsigned long long h; const char* reason; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsO", &h, &reason, &cb)) return NULL;
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    Py_BEGIN_ALLOW_THREADS
    kafka_consumer_Consumer_enforce_rebalance_with_reason_cb(s, reason, producer_void_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// Consumer_poll(h, timeout_ms) -> (ConsumerRecords | None, err | None)
static PyObject* py_Consumer_poll(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long ms;
    if (!PyArg_ParseTuple(args, "KL", &h, &ms)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_consumer_ConsumerRecords_t* records = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (records = kafka_consumer_Consumer_poll(s, (int64_t)ms, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (records) ffi_unraisable((kafka_consumer_ConsumerRecords_destroy_err(records, &ffi_err), ffi_take(&ffi_err)));
        return cvalue_error(Py_None, err);
    }
    PyObject* value = wrap_records(records, consumer_from_handle(h)->is_mock);
    if (value == NULL) return NULL;
    PyObject* ret = cvalue_error(value, NULL);
    Py_DECREF(value);
    return ret;
}

static PyObject* py_Consumer_poll_cb(PyObject* self, PyObject* args) {
    unsigned long long h; long long ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &ms, &cb)) return NULL;
    PollCbCtx* ctx = (PollCbCtx*)PyMem_Malloc(sizeof(PollCbCtx));
    if (ctx == NULL) return PyErr_NoMemory();
    Py_INCREF(cb);
    ctx->cb = cb;
    ctx->copy_bytes = consumer_from_handle(h)->is_mock;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    Py_BEGIN_ALLOW_THREADS
    kafka_consumer_Consumer_poll_cb(s, (int64_t)ms, consumer_poll_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// The CloseOptions for Consumer_close(_cb)'s (timeout_ms, operation): NULL for
// Java's close() (no timeout and the DEFAULT operation); otherwise
// CloseOptions.timeout(ms) and/or groupMembershipOperation(op). `op` is the
// Python discriminant: 0 DEFAULT, 1 LEAVE_GROUP, 2 REMAIN_IN_GROUP. Returns 0,
// or -1 with ValueError set for an unknown `op`.
static int close_options_build(long long ms, int op, kafka_consumer_CloseOptions_t** out) {
    kafka_common_Error_t *ffi_err = NULL;
    *out = NULL;
    const kafka_consumer_CloseOptions_GroupMembershipOperation_t* operation = NULL;
    kafka_common_Error_t* err;
    switch (op) {
    case 0: err = (operation = kafka_consumer_CloseOptions_GroupMembershipOperation_default_err(&ffi_err), ffi_take(&ffi_err)); break;
    case 1: err = (operation = kafka_consumer_CloseOptions_GroupMembershipOperation_leave_group_err(&ffi_err), ffi_take(&ffi_err)); break;
    case 2: err = (operation = kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group_err(&ffi_err), ffi_take(&ffi_err)); break;
    default:
        PyErr_Format(PyExc_ValueError, "unknown group membership operation %d", op);
        return -1;
    }
    if (ffi_raise(err)) return -1;
    if (ms < 0 && op == 0) return 0;
    if (ms >= 0) {
        if (ffi_raise((*(out) = kafka_consumer_CloseOptions_new_with_timeout_err((int64_t)ms, &ffi_err), ffi_take(&ffi_err)))) return -1;
        if (op != 0 && ffi_raise((kafka_consumer_CloseOptions_with_group_membership_operation_err(*out, operation, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_consumer_CloseOptions_destroy_err(*out, &ffi_err), ffi_take(&ffi_err)));
            *out = NULL;
            return -1;
        }
    } else {
        if (ffi_raise((*(out) = kafka_consumer_CloseOptions_with_operation_err(operation, &ffi_err), ffi_take(&ffi_err)))) return -1;
    }
    return 0;
}

// Consumer_close(h, timeout_ms, operation) -> error. A negative timeout and the
// DEFAULT operation select Java's close(); otherwise close(CloseOptions), see
// close_options_build.
static PyObject* py_Consumer_close(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long ms; int op;
    if (!PyArg_ParseTuple(args, "KLi", &h, &ms, &op)) return NULL;
    kafka_consumer_CloseOptions_t* opts;
    if (close_options_build(ms, op, &opts) < 0) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = opts ? (kafka_consumer_Consumer_close_with_options(s, opts, &ffi_err), ffi_take(&ffi_err)) : (kafka_consumer_Consumer_close(s, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (opts) ffi_unraisable((kafka_consumer_CloseOptions_destroy_err(opts, &ffi_err), ffi_take(&ffi_err)));
    return err_result(err);
}

static PyObject* py_Consumer_close_cb(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long ms; int op; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLiO", &h, &ms, &op, &cb)) return NULL;
    kafka_consumer_CloseOptions_t* opts;
    if (close_options_build(ms, op, &opts) < 0) return NULL;
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) { if (opts) ffi_unraisable((kafka_consumer_CloseOptions_destroy_err(opts, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    kafka_consumer_Consumer_t* s = consumer_self(h);
    Py_BEGIN_ALLOW_THREADS
    if (opts) kafka_consumer_Consumer_close_with_options_cb(s, opts, producer_void_cb, ctx);
    else kafka_consumer_Consumer_close_cb(s, producer_void_cb, ctx);
    Py_END_ALLOW_THREADS
    if (opts) ffi_unraisable((kafka_consumer_CloseOptions_destroy_err(opts, &ffi_err), ffi_take(&ffi_err)));  // copied during the call
    Py_RETURN_NONE;
}

// ---- subscribe -------------------------------------------------------------

// Completion of a `_cb` subscribe: settles the listener registration (see
// consumer_listener_commit) before delivering cb(error).
typedef struct {
    PyObject* cb;
    Consumer* c;
    ListenerCtx* lctx;
} SubscribeCbCtx;

static void consumer_subscribe_cb(kafka_common_Error_t* error, void* opaque) {
    kafka_common_Error_t *ffi_err = NULL;
    SubscribeCbCtx* ctx = (SubscribeCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    consumer_listener_commit(ctx->c, ctx->lctx, error != NULL);
    PyObject* py_err = error_handle_to_py(error);  // owned: the callable takes it
    if (py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
        Py_DECREF(py_err);
    } else {
        if (error != NULL) ffi_unraisable((kafka_common_Error_destroy_err(error, &ffi_err), ffi_take(&ffi_err)));
        PyErr_WriteUnraisable(ctx->cb);
    }
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

static SubscribeCbCtx* subscribe_cb_ctx_new(PyObject* cb, Consumer* c, ListenerCtx* lctx) {
    SubscribeCbCtx* ctx = (SubscribeCbCtx*)PyMem_Malloc(sizeof(SubscribeCbCtx));
    if (ctx == NULL) { PyErr_NoMemory(); return NULL; }
    Py_INCREF(cb);
    ctx->cb = cb;
    ctx->c = c;
    ctx->lctx = lctx;
    return ctx;
}

// Consumer_subscribe(h, topics: list[str], adapter | None) -> err | None
static PyObject* py_Consumer_subscribe(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* topics; PyObject* adapter;
    if (!PyArg_ParseTuple(args, "KOO", &h, &topics, &adapter)) return NULL;
    Consumer* c = consumer_from_handle(h);
    str_list_t tl;
    if (str_list_build(topics, &tl) < 0) return NULL;
    ListenerCtx* lctx; kafka_consumer_ConsumerRebalanceListener_t* listener;
    if (listener_build(c, adapter, &lctx, &listener) < 0) { str_list_free(&tl); return NULL; }
    kafka_consumer_Consumer_t* s = c->consumer;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = listener ? (kafka_consumer_Consumer_subscribe_with_topics_listener(s, tl.list, listener, &ffi_err), ffi_take(&ffi_err))
                   : (kafka_consumer_Consumer_subscribe_with_topics(s, tl.list, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (listener) ffi_unraisable((kafka_consumer_ConsumerRebalanceListener_destroy_err(listener, &ffi_err), ffi_take(&ffi_err)));  // registration copied
    str_list_free(&tl);
    consumer_listener_commit(c, lctx, err != NULL);
    return err_result(err);
}

static PyObject* py_Consumer_subscribe_cb(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* topics; PyObject* adapter; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOOO", &h, &topics, &adapter, &cb)) return NULL;
    Consumer* c = consumer_from_handle(h);
    str_list_t tl;
    if (str_list_build(topics, &tl) < 0) return NULL;
    ListenerCtx* lctx; kafka_consumer_ConsumerRebalanceListener_t* listener;
    if (listener_build(c, adapter, &lctx, &listener) < 0) { str_list_free(&tl); return NULL; }
    SubscribeCbCtx* ctx = subscribe_cb_ctx_new(cb, c, lctx);
    if (ctx == NULL) {
        if (listener) ffi_unraisable((kafka_consumer_ConsumerRebalanceListener_destroy_err(listener, &ffi_err), ffi_take(&ffi_err)));
        listener_ctx_free(lctx); str_list_free(&tl);
        return NULL;
    }
    kafka_consumer_Consumer_t* s = c->consumer;
    Py_BEGIN_ALLOW_THREADS
    if (listener) kafka_consumer_Consumer_subscribe_with_topics_listener_cb(s, tl.list, listener, consumer_subscribe_cb, ctx);
    else kafka_consumer_Consumer_subscribe_with_topics_cb(s, tl.list, consumer_subscribe_cb, ctx);
    Py_END_ALLOW_THREADS
    if (listener) ffi_unraisable((kafka_consumer_ConsumerRebalanceListener_destroy_err(listener, &ffi_err), ffi_take(&ffi_err)));  // registration copied
    str_list_free(&tl);
    Py_RETURN_NONE;
}

// Consumer_subscribe_pattern(h, pattern: str, adapter | None) -> err | None
// (Java's subscribe(SubscriptionPattern[, listener]), evaluated broker-side).
static PyObject* py_Consumer_subscribe_pattern(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* pattern; PyObject* adapter;
    if (!PyArg_ParseTuple(args, "KsO", &h, &pattern, &adapter)) return NULL;
    Consumer* c = consumer_from_handle(h);
    ListenerCtx* lctx; kafka_consumer_ConsumerRebalanceListener_t* listener;
    if (listener_build(c, adapter, &lctx, &listener) < 0) return NULL;
    kafka_consumer_SubscriptionPattern_t* sp = NULL;
    if (ffi_raise((sp = kafka_consumer_SubscriptionPattern_new_err(pattern, &ffi_err), ffi_take(&ffi_err)))) {
        if (listener) ffi_unraisable((kafka_consumer_ConsumerRebalanceListener_destroy_err(listener, &ffi_err), ffi_take(&ffi_err)));
        listener_ctx_free(lctx);
        return NULL;
    }
    kafka_consumer_Consumer_t* s = c->consumer;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = listener ? (kafka_consumer_Consumer_subscribe_with_pattern_listener(s, sp, listener, &ffi_err), ffi_take(&ffi_err))
                   : (kafka_consumer_Consumer_subscribe_with_pattern(s, sp, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    ffi_unraisable((kafka_consumer_SubscriptionPattern_destroy_err(sp, &ffi_err), ffi_take(&ffi_err)));
    if (listener) ffi_unraisable((kafka_consumer_ConsumerRebalanceListener_destroy_err(listener, &ffi_err), ffi_take(&ffi_err)));
    consumer_listener_commit(c, lctx, err != NULL);
    return err_result(err);
}

static PyObject* py_Consumer_subscribe_pattern_cb(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* pattern; PyObject* adapter; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsOO", &h, &pattern, &adapter, &cb)) return NULL;
    Consumer* c = consumer_from_handle(h);
    ListenerCtx* lctx; kafka_consumer_ConsumerRebalanceListener_t* listener;
    if (listener_build(c, adapter, &lctx, &listener) < 0) return NULL;
    kafka_consumer_SubscriptionPattern_t* sp = NULL;
    SubscribeCbCtx* ctx = ffi_raise((sp = kafka_consumer_SubscriptionPattern_new_err(pattern, &ffi_err), ffi_take(&ffi_err)))
        ? NULL : subscribe_cb_ctx_new(cb, c, lctx);
    if (ctx == NULL) {
        if (sp) ffi_unraisable((kafka_consumer_SubscriptionPattern_destroy_err(sp, &ffi_err), ffi_take(&ffi_err)));
        if (listener) ffi_unraisable((kafka_consumer_ConsumerRebalanceListener_destroy_err(listener, &ffi_err), ffi_take(&ffi_err)));
        listener_ctx_free(lctx);
        return NULL;
    }
    kafka_consumer_Consumer_t* s = c->consumer;
    Py_BEGIN_ALLOW_THREADS
    if (listener) kafka_consumer_Consumer_subscribe_with_pattern_listener_cb(s, sp, listener, consumer_subscribe_cb, ctx);
    else kafka_consumer_Consumer_subscribe_with_pattern_cb(s, sp, consumer_subscribe_cb, ctx);
    Py_END_ALLOW_THREADS
    ffi_unraisable((kafka_consumer_SubscriptionPattern_destroy_err(sp, &ffi_err), ffi_take(&ffi_err)));
    if (listener) ffi_unraisable((kafka_consumer_ConsumerRebalanceListener_destroy_err(listener, &ffi_err), ffi_take(&ffi_err)));
    Py_RETURN_NONE;
}

// ---- commit_async ----------------------------------------------------------

// Consumer_commit_async(h, spec | None, adapter | None) -> err | None
//
// Java's commitAsync(), commitAsync(callback) and commitAsync(offsets, callback).
// The FFI's offsets form always takes a callback, so `offsets` without a user
// callback registers a discarding one (Java's commitAsync(offsets, null)).
static PyObject* py_Consumer_commit_async(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; PyObject* adapter;
    if (!PyArg_ParseTuple(args, "KOO", &h, &spec, &adapter)) return NULL;
    Consumer* c = consumer_from_handle(h);
    kafka_consumer_Consumer_t* s = c->consumer;
    kafka_common_Error_t* err;
    if (spec == Py_None && adapter == Py_None) {
        Py_BEGIN_ALLOW_THREADS
        err = (kafka_consumer_Consumer_commit_async(s, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
        return err_result(err);
    }
    offsets_map_t m; memset(&m, 0, sizeof(m));
    int with_offsets = spec != Py_None;
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;
    CommitCbCtx* ctx = commit_ctx_new(c, adapter == Py_None ? NULL : adapter);
    if (ctx == NULL) { if (with_offsets) offsets_map_free(&m); return NULL; }
    kafka_consumer_OffsetCommitCallback_t* cbh = NULL;
    if (ffi_raise((cbh = kafka_consumer_OffsetCommitCallback_new_err(ctx, commit_on_complete, &ffi_err), ffi_take(&ffi_err)))) {
        // Orphaned, not freed: the registration may have been made.
        consumer_orphan_commit(c, ctx);
        if (with_offsets) offsets_map_free(&m);
        return NULL;
    }
    Py_BEGIN_ALLOW_THREADS
    err = with_offsets ? (kafka_consumer_Consumer_commit_async_with_offsets_callback(s, m.map, cbh, &ffi_err), ffi_take(&ffi_err))
                       : (kafka_consumer_Consumer_commit_async_with_callback(s, cbh, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    ffi_unraisable((kafka_consumer_OffsetCommitCallback_destroy_err(cbh, &ffi_err), ffi_take(&ffi_err)));  // registration copied
    if (with_offsets) offsets_map_free(&m);
    if (err != NULL) consumer_orphan_commit(c, ctx);
    return err_result(err);
}

// Completion of a `_cb` commit_async: an Err'd commit orphans its callback
// context (see CommitCbCtx) before delivering cb(error).
typedef struct {
    PyObject* cb;
    Consumer* c;
    CommitCbCtx* cctx;
} CommitAsyncCbCtx;

static void consumer_commit_async_cb(kafka_common_Error_t* error, void* opaque) {
    kafka_common_Error_t *ffi_err = NULL;
    CommitAsyncCbCtx* ctx = (CommitAsyncCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    if (error != NULL) consumer_orphan_commit(ctx->c, ctx->cctx);
    PyObject* py_err = error_handle_to_py(error);  // owned: the callable takes it
    if (py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
        Py_DECREF(py_err);
    } else {
        if (error != NULL) ffi_unraisable((kafka_common_Error_destroy_err(error, &ffi_err), ffi_take(&ffi_err)));
        PyErr_WriteUnraisable(ctx->cb);
    }
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

static PyObject* py_Consumer_commit_async_cb(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; PyObject* adapter; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOOO", &h, &spec, &adapter, &cb)) return NULL;
    Consumer* c = consumer_from_handle(h);
    kafka_consumer_Consumer_t* s = c->consumer;
    if (spec == Py_None && adapter == Py_None) {
        VoidCbCtx* vctx = void_cb_ctx_new(cb);
        if (vctx == NULL) return NULL;
        Py_BEGIN_ALLOW_THREADS
        kafka_consumer_Consumer_commit_async_cb(s, producer_void_cb, vctx);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }
    offsets_map_t m; memset(&m, 0, sizeof(m));
    int with_offsets = spec != Py_None;
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;
    CommitCbCtx* cctx = commit_ctx_new(c, adapter == Py_None ? NULL : adapter);
    if (cctx == NULL) { if (with_offsets) offsets_map_free(&m); return NULL; }
    CommitAsyncCbCtx* ctx = (CommitAsyncCbCtx*)PyMem_Malloc(sizeof(CommitAsyncCbCtx));
    if (ctx == NULL) { commit_ctx_free(cctx); if (with_offsets) offsets_map_free(&m); PyErr_NoMemory(); return NULL; }
    Py_INCREF(cb);
    ctx->cb = cb;
    ctx->c = c;
    ctx->cctx = cctx;
    kafka_consumer_OffsetCommitCallback_t* cbh = NULL;
    if (ffi_raise((cbh = kafka_consumer_OffsetCommitCallback_new_err(cctx, commit_on_complete, &ffi_err), ffi_take(&ffi_err)))) {
        // Orphaned, not freed: the registration may have been made.
        consumer_orphan_commit(c, cctx);
        Py_DECREF(ctx->cb);
        PyMem_Free(ctx);
        if (with_offsets) offsets_map_free(&m);
        return NULL;
    }
    Py_BEGIN_ALLOW_THREADS
    if (with_offsets) kafka_consumer_Consumer_commit_async_with_offsets_callback_cb(s, m.map, cbh, consumer_commit_async_cb, ctx);
    else kafka_consumer_Consumer_commit_async_with_callback_cb(s, cbh, consumer_commit_async_cb, ctx);
    Py_END_ALLOW_THREADS
    ffi_unraisable((kafka_consumer_OffsetCommitCallback_destroy_err(cbh, &ffi_err), ffi_take(&ffi_err)));  // registration copied
    if (with_offsets) offsets_map_free(&m);
    Py_RETURN_NONE;
}

// Consumer_partitions_for(h, topic, timeout_ms) -> (list | None, err | None)
static PyObject* py_Consumer_partitions_for(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* topic; long long ms;
    if (!PyArg_ParseTuple(args, "KsL", &h, &topic, &ms)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_List_t* infos = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (ms >= 0) err = (infos = kafka_consumer_Consumer_partitions_for_with_timeout(s, topic, (int64_t)ms, &ffi_err), ffi_take(&ffi_err));
    else err = (infos = kafka_consumer_Consumer_partitions_for(s, topic, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (infos) ffi_unraisable((kafka_List_destroy_err(infos, &ffi_err), ffi_take(&ffi_err)));
        return cvalue_error(Py_None, err);
    }
    PyObject* value = partition_info_list_to_py(infos);
    if (value == NULL) return NULL;
    PyObject* ret = cvalue_error(value, NULL);
    Py_DECREF(value);
    return ret;
}

static PyObject* py_Consumer_partitions_for_cb(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; long long ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsLO", &h, &topic, &ms, &cb)) return NULL;
    ValueCbCtx* ctx = value_cb_ctx_new(cb, NULL, partition_info_list_to_py);
    if (ctx == NULL) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    Py_BEGIN_ALLOW_THREADS
    if (ms >= 0) kafka_consumer_Consumer_partitions_for_with_timeout_cb(s, topic, (int64_t)ms, consumer_list_cb, ctx);
    else kafka_consumer_Consumer_partitions_for_cb(s, topic, consumer_list_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// Consumer_list_topics(h, timeout_ms) -> (dict | None, err | None)
static PyObject* py_Consumer_list_topics(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long ms;
    if (!PyArg_ParseTuple(args, "KL", &h, &ms)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_Map_t* out = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (ms >= 0) err = (out = kafka_consumer_Consumer_list_topics_with_timeout(s, (int64_t)ms, &ffi_err), ffi_take(&ffi_err));
    else err = (out = kafka_consumer_Consumer_list_topics(s, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (out) ffi_unraisable((kafka_Map_destroy_err(out, &ffi_err), ffi_take(&ffi_err)));
        return cvalue_error(Py_None, err);
    }
    PyObject* value = topics_map_to_py(out);
    if (value == NULL) return NULL;
    PyObject* ret = cvalue_error(value, NULL);
    Py_DECREF(value);
    return ret;
}

static PyObject* py_Consumer_list_topics_cb(PyObject* self, PyObject* args) {
    unsigned long long h; long long ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &ms, &cb)) return NULL;
    ValueCbCtx* ctx = value_cb_ctx_new(cb, topics_map_to_py, NULL);
    if (ctx == NULL) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    Py_BEGIN_ALLOW_THREADS
    if (ms >= 0) kafka_consumer_Consumer_list_topics_with_timeout_cb(s, (int64_t)ms, consumer_map_cb, ctx);
    else kafka_consumer_Consumer_list_topics_cb(s, consumer_map_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// ---- ConsumerHandle --------------------------------------------------------
//
// Java captures the consumer variable inside a listener; the Rust client
// exposes a ConsumerHandle for the same re-entrancy (consumer-threading.md
// §31/§41). The blocking forms run the operation on the runtime and wait on a
// channel, so they are safe inside a rebalance listener; the `_cb` forms are
// for coroutine listeners. The handle outliving its consumer fails every
// operation with LocalIllegalState("consumer destroyed").

// Consumer_handle(h) -> handle address (owned; ConsumerHandle_destroy frees it)
static PyObject* py_Consumer_handle(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerHandle_t* hh = NULL;
    if (ffi_raise((hh = kafka_consumer_Consumer_handle_err(consumer_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)hh);
}

static PyObject* py_ConsumerHandle_destroy(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    ffi_unraisable((kafka_consumer_ConsumerHandle_destroy_err((kafka_consumer_ConsumerHandle_t*)(uintptr_t)h, &ffi_err), ffi_take(&ffi_err)));
    Py_RETURN_NONE;
}

static PyObject* py_ConsumerHandle_wakeup(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    if (ffi_raise((kafka_consumer_ConsumerHandle_wakeup_err(handle_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    Py_RETURN_NONE;
}

static PyObject* py_ConsumerHandle_assignment(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_List_t* list = NULL;
    if (ffi_raise((list = kafka_consumer_ConsumerHandle_assignment_err(handle_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return tp_list_to_py(list);
}

static PyObject* py_ConsumerHandle_paused(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_List_t* list = NULL;
    if (ffi_raise((list = kafka_consumer_ConsumerHandle_paused_err(handle_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return tp_list_to_py(list);
}

static PyObject* py_ConsumerHandle_subscription(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_List_t* list = NULL;
    if (ffi_raise((list = kafka_consumer_ConsumerHandle_subscription_err(handle_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return string_list_to_py(list);
}

// No-timeout variants of the map-returning shapes (the ConsumerHandle's Java
// methods have no Duration overload). py(h, tps) / py_cb(h, tps, cb)
#define DEF_TPLIST_MAP_OPS_NT(pyname, SELF_T, SELF_FN, OP, OP_CB, CONV)                 \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; PyObject* tps;                                                \
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;                           \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    out = OP(s, l.list, &err);                                                          \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    if (err != NULL) { if (out) ffi_unraisable((kafka_Map_destroy_err(out, &ffi_err), ffi_take(&ffi_err))); return cvalue_error(Py_None, err); } \
    PyObject* value = CONV(out);                                                        \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = cvalue_error(value, NULL);                                          \
    Py_DECREF(value);                                                                   \
    return ret;                                                                         \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* tps; PyObject* cb;                                  \
    if (!PyArg_ParseTuple(args, "KOO", &h, &tps, &cb)) return NULL;                     \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    ValueCbCtx* ctx = value_cb_ctx_new(cb, CONV, NULL);                                 \
    if (ctx == NULL) { tp_list_free(&l); return NULL; }                                 \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_CB(s, l.list, consumer_map_cb, ctx);                                             \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    Py_RETURN_NONE;                                                                     \
}

// py(h, [(t, p, ts)]) -> (dict | None, err) / py_cb(h, spec, cb)
#define DEF_TIMESTAMPS_MAP_OPS_NT(pyname, SELF_T, SELF_FN, OP, OP_CB)                   \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    kafka_common_Error_t *ffi_err = NULL;                                               \
    unsigned long long h; PyObject* spec;                                               \
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;                          \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    out = OP(s, m.map, &err);                                                           \
    Py_END_ALLOW_THREADS                                                                \
    tp_i64_map_free(&m);                                                                \
    if (err != NULL) { if (out) ffi_unraisable((kafka_Map_destroy_err(out, &ffi_err), ffi_take(&ffi_err))); return cvalue_error(Py_None, err); } \
    PyObject* value = oat_map_to_py(out);                                               \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = cvalue_error(value, NULL);                                          \
    Py_DECREF(value);                                                                   \
    return ret;                                                                         \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* spec; PyObject* cb;                                 \
    if (!PyArg_ParseTuple(args, "KOO", &h, &spec, &cb)) return NULL;                    \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    ValueCbCtx* ctx = value_cb_ctx_new(cb, oat_map_to_py, NULL);                        \
    if (ctx == NULL) { tp_i64_map_free(&m); return NULL; }                              \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_CB(s, m.map, consumer_map_cb, ctx);                                              \
    Py_END_ALLOW_THREADS                                                                \
    tp_i64_map_free(&m);                                                                \
    Py_RETURN_NONE;                                                                     \
}

// Shape: (self[, offsets]) -> err, for the handle's commit_sync / commit_async.
// py(h, spec | None) / py_cb(h, spec | None, cb)
#define DEF_COMMIT_OPS_NT(pyname, SELF_T, SELF_FN, OP, OP_O, OP_CB, OP_O_CB)            \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h; PyObject* spec;                                               \
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;                          \
    offsets_map_t m; memset(&m, 0, sizeof(m));                                          \
    int with_offsets = spec != Py_None;                                                 \
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;                   \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_common_Error_t* err = NULL;                                                   \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (with_offsets) OP_O(s, m.map, &err); else OP(s, &err);                           \
    Py_END_ALLOW_THREADS                                                                \
    if (with_offsets) offsets_map_free(&m);                                             \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; PyObject* spec; PyObject* cb;                                 \
    if (!PyArg_ParseTuple(args, "KOO", &h, &spec, &cb)) return NULL;                    \
    offsets_map_t m; memset(&m, 0, sizeof(m));                                          \
    int with_offsets = spec != Py_None;                                                 \
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;                   \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) { if (with_offsets) offsets_map_free(&m); return NULL; }           \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (with_offsets) OP_O_CB(s, m.map, producer_void_cb, ctx);                         \
    else OP_CB(s, producer_void_cb, ctx);                                               \
    Py_END_ALLOW_THREADS                                                                \
    if (with_offsets) offsets_map_free(&m);                                             \
    Py_RETURN_NONE;                                                                     \
}

#define HANDLE_T const kafka_consumer_ConsumerHandle_t*

DEF_TPLIST_VOID_OPS(ConsumerHandle_assign, HANDLE_T, handle_self,
                    kafka_consumer_ConsumerHandle_assign, kafka_consumer_ConsumerHandle_assign_cb)
DEF_TPLIST_VOID_OPS(ConsumerHandle_seek_to_beginning, HANDLE_T, handle_self,
                    kafka_consumer_ConsumerHandle_seek_to_beginning, kafka_consumer_ConsumerHandle_seek_to_beginning_cb)
DEF_TPLIST_VOID_OPS(ConsumerHandle_seek_to_end, HANDLE_T, handle_self,
                    kafka_consumer_ConsumerHandle_seek_to_end, kafka_consumer_ConsumerHandle_seek_to_end_cb)
DEF_TPLIST_VOID_OPS(ConsumerHandle_pause, HANDLE_T, handle_self,
                    kafka_consumer_ConsumerHandle_pause, kafka_consumer_ConsumerHandle_pause_cb)
DEF_TPLIST_VOID_OPS(ConsumerHandle_resume, HANDLE_T, handle_self,
                    kafka_consumer_ConsumerHandle_resume, kafka_consumer_ConsumerHandle_resume_cb)

DEF_SEEK_OPS(ConsumerHandle, HANDLE_T, handle_self,
             kafka_consumer_ConsumerHandle_seek_with_offset, kafka_consumer_ConsumerHandle_seek_with_offset_cb,
             kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata, kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb)

DEF_POSITION_OPS(ConsumerHandle_position, HANDLE_T, handle_self,
                 kafka_consumer_ConsumerHandle_position, kafka_consumer_ConsumerHandle_position_with_timeout,
                 kafka_consumer_ConsumerHandle_position_cb, kafka_consumer_ConsumerHandle_position_with_timeout_cb)

DEF_TPLIST_MAP_OPS_NT(ConsumerHandle_committed, HANDLE_T, handle_self,
                      kafka_consumer_ConsumerHandle_committed, kafka_consumer_ConsumerHandle_committed_cb, oam_map_to_py)
DEF_TPLIST_MAP_OPS_NT(ConsumerHandle_beginning_offsets, HANDLE_T, handle_self,
                      kafka_consumer_ConsumerHandle_beginning_offsets, kafka_consumer_ConsumerHandle_beginning_offsets_cb, long_map_to_py)
DEF_TPLIST_MAP_OPS_NT(ConsumerHandle_end_offsets, HANDLE_T, handle_self,
                      kafka_consumer_ConsumerHandle_end_offsets, kafka_consumer_ConsumerHandle_end_offsets_cb, long_map_to_py)
DEF_TIMESTAMPS_MAP_OPS_NT(ConsumerHandle_offsets_for_times, HANDLE_T, handle_self,
                          kafka_consumer_ConsumerHandle_offsets_for_times, kafka_consumer_ConsumerHandle_offsets_for_times_cb)

DEF_COMMIT_OPS_NT(ConsumerHandle_commit_sync, HANDLE_T, handle_self,
                  kafka_consumer_ConsumerHandle_commit_sync, kafka_consumer_ConsumerHandle_commit_sync_with_offsets,
                  kafka_consumer_ConsumerHandle_commit_sync_cb, kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb)
DEF_COMMIT_OPS_NT(ConsumerHandle_commit_async, HANDLE_T, handle_self,
                  kafka_consumer_ConsumerHandle_commit_async, kafka_consumer_ConsumerHandle_commit_async_offsets,
                  kafka_consumer_ConsumerHandle_commit_async_cb, kafka_consumer_ConsumerHandle_commit_async_offsets_cb)

// ConsumerHandle_commit_async_callback(h, hh, spec | None, adapter) -> err | None
//
// Java's commitAsync(callback) and commitAsync(offsets, callback) from inside a
// callback, through the ConsumerHandle `hh` of the consumer `h`. Blocking: the
// commit callbacks queued behind the running one run on this thread inside the
// call, as Java's commit() runs executeCallbacks() first. The commit's own
// callback runs inside a later call, like Consumer_commit_async's; its context
// is orphaned when the call fails, since the callback then never fires.
static PyObject* py_ConsumerHandle_commit_async_callback(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; unsigned long long hh; PyObject* spec; PyObject* adapter;
    if (!PyArg_ParseTuple(args, "KKOO", &h, &hh, &spec, &adapter)) return NULL;
    Consumer* c = consumer_from_handle(h);
    const kafka_consumer_ConsumerHandle_t* s = handle_self(hh);
    offsets_map_t m; memset(&m, 0, sizeof(m));
    int with_offsets = spec != Py_None;
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;
    CommitCbCtx* ctx = commit_ctx_new(c, adapter == Py_None ? NULL : adapter);
    if (ctx == NULL) { if (with_offsets) offsets_map_free(&m); return NULL; }
    kafka_consumer_OffsetCommitCallback_t* cbh = NULL;
    if (ffi_raise((cbh = kafka_consumer_OffsetCommitCallback_new_err(ctx, commit_on_complete, &ffi_err), ffi_take(&ffi_err)))) {
        // Orphaned, not freed: the registration may have been made.
        consumer_orphan_commit(c, ctx);
        if (with_offsets) offsets_map_free(&m);
        return NULL;
    }
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = with_offsets ? (kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback(s, m.map, cbh, &ffi_err), ffi_take(&ffi_err))
                       : (kafka_consumer_ConsumerHandle_commit_async_with_callback(s, cbh, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    ffi_unraisable((kafka_consumer_OffsetCommitCallback_destroy_err(cbh, &ffi_err), ffi_take(&ffi_err)));  // registration copied
    if (with_offsets) offsets_map_free(&m);
    if (err != NULL) consumer_orphan_commit(c, ctx);
    return err_result(err);
}

#undef HANDLE_T

// ---- MockConsumer drivers --------------------------------------------------

static kafka_consumer_MockConsumer_t* mock_self(unsigned long long h) {
    return consumer_from_handle(h)->mc;
}

// MockConsumer_rebalance(h, [(topic, partition)]) -> err | None. Blocking: it
// invokes the registered listener on this thread. `_cb` queues it instead.
static PyObject* py_MockConsumer_rebalance(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* tps;
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;
    tp_list_t l;
    if (tp_list_build(tps, &l) < 0) return NULL;
    kafka_consumer_MockConsumer_t* mc = mock_self(h);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (kafka_consumer_MockConsumer_rebalance(mc, l.list, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    tp_list_free(&l);
    return err_result(err);
}

static PyObject* py_MockConsumer_rebalance_cb(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &tps, &cb)) return NULL;
    tp_list_t l;
    if (tp_list_build(tps, &l) < 0) return NULL;
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) { tp_list_free(&l); return NULL; }
    kafka_consumer_MockConsumer_t* mc = mock_self(h);
    Py_BEGIN_ALLOW_THREADS
    kafka_consumer_MockConsumer_rebalance_cb(mc, l.list, producer_void_cb, ctx);
    Py_END_ALLOW_THREADS
    tp_list_free(&l);
    Py_RETURN_NONE;
}

// Pins a copy of `obj` (bytes-like) so the records sharing the pointer stay
// valid until the consumer is destroyed. Returns NULL with an exception set on
// failure; `*out` is NULL for None (a Java null key / value).
static int pin_bytes(Consumer* c, PyObject* obj, kafka_Bytes_t** out) {
    *out = NULL;
    if (obj == Py_None) return 0;
    Py_buffer view;
    if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) < 0) return -1;
    int32_t len;
    if (checked_len_to_int32(view.len, "record bytes", &len) < 0) { PyBuffer_Release(&view); return -1; }
    PinnedBytes* pb = (PinnedBytes*)PyMem_Malloc(sizeof(PinnedBytes) + (size_t)view.len);
    if (pb == NULL) { PyBuffer_Release(&view); PyErr_NoMemory(); return -1; }
    if (view.len > 0) memcpy(pb->data, view.buf, (size_t)view.len);
    pb->bytes.data = pb->data;
    pb->bytes.len = len;
    PyBuffer_Release(&view);
    pb->next = c->pinned;
    c->pinned = pb;
    *out = &pb->bytes;
    return 0;
}

// MockConsumer_add_record(h, topic, partition, offset, key | None, value | None) -> err | None
static PyObject* py_MockConsumer_add_record(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* topic; int partition; long long offset; PyObject* key; PyObject* value;
    if (!PyArg_ParseTuple(args, "KsiLOO", &h, &topic, &partition, &offset, &key, &value)) return NULL;
    Consumer* c = consumer_from_handle(h);
    kafka_Bytes_t* k; kafka_Bytes_t* v;
    if (pin_bytes(c, key, &k) < 0) return NULL;
    if (pin_bytes(c, value, &v) < 0) return NULL;
    kafka_consumer_ConsumerRecord_t* rec = NULL;
    if (ffi_raise((rec = kafka_consumer_ConsumerRecord_new_err(topic, (int32_t)partition, (int64_t)offset, k, v, &ffi_err), ffi_take(&ffi_err)))) {
        return NULL;
    }
    kafka_common_Error_t* err = (kafka_consumer_MockConsumer_add_record(c->mc, rec, &ffi_err), ffi_take(&ffi_err));  // copied
    ffi_unraisable((kafka_consumer_ConsumerRecord_destroy_err(rec, &ffi_err), ffi_take(&ffi_err)));
    return err_result(err);
}

// MockConsumer_update_{beginning,end,duration}_offsets(h, [(topic, partition, offset)])
#define DEF_MOCK_UPDATE_OFFSETS(pyname, OP)                                             \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h; PyObject* spec;                                               \
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;                          \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    kafka_common_Error_t* err = NULL;                                                   \
    OP(mock_self(h), m.map, &err);                                                      \
    tp_i64_map_free(&m);                                                                \
    if (ffi_raise(err)) return NULL;                                                    \
    Py_RETURN_NONE;                                                                     \
}

DEF_MOCK_UPDATE_OFFSETS(MockConsumer_update_beginning_offsets, kafka_consumer_MockConsumer_update_beginning_offsets_err)
DEF_MOCK_UPDATE_OFFSETS(MockConsumer_update_end_offsets, kafka_consumer_MockConsumer_update_end_offsets_err)
DEF_MOCK_UPDATE_OFFSETS(MockConsumer_update_duration_offsets, kafka_consumer_MockConsumer_update_duration_offsets_err)

// MockConsumer_update_partitions(h, topic, [(partition, leader_id, host, port)]) -> err | None
// Each PartitionInfo gets the leader as its only replica / ISR member.
static PyObject* py_MockConsumer_update_partitions(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* topic; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KsO", &h, &topic, &spec)) return NULL;
    PyObject* seq = PySequence_Fast(spec, "partitions must be a sequence");
    if (seq == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
    // C-built lists hold borrowed elements: the Node_t* and PartitionInfo_t*
    // are owned here and freed after the (copying) call.
    // The C objects are also tracked in C arrays, so the cleanup below needs no
    // FFI call to find them.
    kafka_List_t* infos = NULL;
    kafka_common_Node_t** node_arr = PyMem_Calloc(n > 0 ? (size_t)n : 1, sizeof(*node_arr));
    kafka_List_t** replica_arr = PyMem_Calloc(n > 0 ? (size_t)n : 1, sizeof(*replica_arr));  // single-leader lists
    kafka_common_PartitionInfo_t** info_arr = PyMem_Calloc(n > 0 ? (size_t)n : 1, sizeof(*info_arr));
    kafka_common_Error_t* err = NULL;
    int failed = 0;
    if (node_arr == NULL || replica_arr == NULL || info_arr == NULL) { PyErr_NoMemory(); failed = 1; }
    if (!failed && ffi_raise((infos = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) failed = 1;
    for (Py_ssize_t i = 0; !failed && i < n; i++) {
        int partition, leader_id, port; const char* host;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(seq, i), "iisi", &partition, &leader_id, &host, &port)
            || ffi_raise((node_arr[i] = kafka_common_Node_new_err((int32_t)leader_id, host, (int32_t)port, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((replica_arr[i] = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((kafka_List_add_err(replica_arr[i], node_arr[i], &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((info_arr[i] = kafka_common_PartitionInfo_new_err(topic, (int32_t)partition, node_arr[i],
                                                            replica_arr[i], replica_arr[i], &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((kafka_List_add_err(infos, info_arr[i], &ffi_err), ffi_take(&ffi_err)))) {
            failed = 1;
        }
    }
    if (!failed) {
        Py_BEGIN_ALLOW_THREADS
        err = (kafka_consumer_MockConsumer_update_partitions(mock_self(h), topic, infos, &ffi_err), ffi_take(&ffi_err));  // copied
        Py_END_ALLOW_THREADS
    }
    for (Py_ssize_t i = 0; i < n && info_arr != NULL && replica_arr != NULL && node_arr != NULL; i++) {
        if (info_arr[i]) ffi_unraisable((kafka_common_PartitionInfo_destroy_err(info_arr[i], &ffi_err), ffi_take(&ffi_err)));
        if (replica_arr[i]) ffi_unraisable((kafka_List_destroy_err(replica_arr[i], &ffi_err), ffi_take(&ffi_err)));
        if (node_arr[i]) ffi_unraisable((kafka_common_Node_destroy_err(node_arr[i], &ffi_err), ffi_take(&ffi_err)));
    }
    if (infos) ffi_unraisable((kafka_List_destroy_err(infos, &ffi_err), ffi_take(&ffi_err)));
    PyMem_Free(node_arr);
    PyMem_Free(replica_arr);
    PyMem_Free(info_arr);
    Py_DECREF(seq);
    if (failed) return NULL;
    return err_result(err);
}

// Builds the error for set_poll_error / set_offsets_error: a code selects the
// matching Error variant (mock_error_from_code), None a plain KafkaException
// with the message.
static kafka_common_Error_t* mock_error_build(const char* message, PyObject* code) {
    kafka_common_Error_t *ffi_err = NULL;
    if (code == Py_None) return ffi_built(kafka_common_Error_kafka_message_err(message, &ffi_err), &ffi_err);
    long c = PyLong_AsLong(code);
    if (c == -1 && PyErr_Occurred()) return NULL;
    return mock_error_from_code((int)c, message);
}

// MockConsumer_set_poll_error(h, message, code | None)
static PyObject* py_MockConsumer_set_poll_error(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* message; PyObject* code;
    if (!PyArg_ParseTuple(args, "KsO", &h, &message, &code)) return NULL;
    kafka_common_Error_t* err = mock_error_build(message, code);
    if (err == NULL) return NULL;
    if (ffi_raise((kafka_consumer_MockConsumer_set_poll_error_err(mock_self(h), err, &ffi_err), ffi_take(&ffi_err)))) return NULL;  // ownership moves
    Py_RETURN_NONE;
}

// MockConsumer_set_offsets_error(h, message, code | None)
static PyObject* py_MockConsumer_set_offsets_error(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* message; PyObject* code;
    if (!PyArg_ParseTuple(args, "KsO", &h, &message, &code)) return NULL;
    kafka_common_Error_t* err = mock_error_build(message, code);
    if (err == NULL) return NULL;
    if (ffi_raise((kafka_consumer_MockConsumer_set_offsets_error_err(mock_self(h), err, &ffi_err), ffi_take(&ffi_err)))) return NULL;  // ownership moves
    Py_RETURN_NONE;
}

// MockConsumer_set_max_poll_records(h, n) -> err | None
static PyObject* py_MockConsumer_set_max_poll_records(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long n;
    if (!PyArg_ParseTuple(args, "KL", &h, &n)) return NULL;
    return err_result((kafka_consumer_MockConsumer_set_max_poll_records(mock_self(h), (int64_t)n, &ffi_err), ffi_take(&ffi_err)));
}

static PyObject* py_MockConsumer_closed(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    int8_t closed = 0;
    if (ffi_raise((closed = kafka_consumer_MockConsumer_closed_err(mock_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(closed);
}

static PyObject* py_MockConsumer_should_rebalance(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    int8_t should = 0;
    if (ffi_raise((should = kafka_consumer_MockConsumer_should_rebalance_err(mock_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyBool_FromLong(should);
}

static PyObject* py_MockConsumer_reset_should_rebalance(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    if (ffi_raise((kafka_consumer_MockConsumer_reset_should_rebalance_err(mock_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    Py_RETURN_NONE;
}

static PyObject* py_MockConsumer_last_poll_timeout(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    int64_t timeout = 0;
    if (ffi_raise((timeout = kafka_consumer_MockConsumer_last_poll_timeout_err(mock_self(h), &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return PyLong_FromLongLong((long long)timeout);
}

// ===========================================================================
// Admin
//
// Marshaling only: the Python `admin` module owns all orchestration.
//
// Every RPC is a plain synchronous C call that returns a `*Result_t`, whose
// per-key / whole-call `kafka_common_KafkaFuture_t` handles the Python module
// resolves later (Java's `KafkaFuture.get()`): the RPC entry points below
// return a *job* handle -- the result plus the list of futures it exposes --
// and `Admin_resolve` / `Admin_resolve_cb` drive it.
//
//   * `Admin_resolve_cb(job, cb)` is what BOTH Python clients use:
//     `kafka_common_KafkaFuture_get_cb_err` on every future; the callbacks arrive
//     through `Admin__execute_callbacks`, which the Python side runs on the
//     thread that waits for them. The asyncio client drains on the event-loop
//     thread when the `Admin__set_callbacks_notify` hook fires
//     (`loop.call_soon_threadsafe`); the sync client's hook only sets a
//     `threading.Event` that the calling thread waits on in 100 ms slices
//     (`_sync_wait.SyncWaiter`), so a pending `KeyboardInterrupt` is raised
//     promptly instead of once a native block returned. A future that belongs
//     to no client (refinements, `all_of`) fires inline, which the same code
//     handles. `Admin_close_cb` queues `cb()` on the same vector so `close`
//     waits the same way.
//   * `Admin_resolve(job)` and `Admin_close(handle, timeout_ms)` are the
//     blocking forms: `kafka_common_KafkaFuture_get_err` on every future / the
//     blocking `close`, on the calling thread, GIL released. They stay for
//     callers that want a native block (the C / gRPC path); the Python
//     clients no longer use them.
//
// Values delivered by a future are BORROWED from it: they are converted into
// the raw Python tuples the `_to_*` converters in admin.py expect while the
// future is alive, and only then is the future destroyed (with the map that
// owns it where `*_values()` hands one out). Nothing delivered by `get` is
// ever destroyed directly.
//
// The raw shapes are unchanged from the previous binding:
//   keyed results   {key: (error4 | None, value)}  or  {key: error4 | None}
//   single results  (error4 | None, value)
// where error4 is `(code, message, is_retriable, is_fatal)`.
// ===========================================================================

// ---- handle ------------------------------------------------------------------

// The handle Python holds. `admin` is either the owned `kafka_admin_Admin_t *`
// from `AdminClient_create` or the borrowed `__as_Admin` view of the mock,
// which is valid until the mock handle itself is destroyed.
typedef struct {
    kafka_admin_Admin_t* owned;              // AdminClient_create result, or NULL
    kafka_admin_MockAdminClient_t* mock;     // MockAdminClient_create result, or NULL
    const kafka_admin_Admin_t* admin;        // `owned` or the mock's __as_Admin view
    PyObject* notify_cb;                     // Python notify hook, or NULL
} AdminHandle;

static AdminHandle* admin_from_handle(unsigned long long h) {
    return (AdminHandle*)(uintptr_t)h;
}

// Fired by Rust once each time the callbacks vector goes from empty to
// non-empty. It only schedules (see consumer_callbacks_notify).
static void admin_callbacks_notify(void* opaque) {
    AdminHandle* a = (AdminHandle*)opaque;
    // Loaded once: `Admin_destroy` clears the field before tearing the client
    // down and releases the callable only after `_destroy` returned, i.e.
    // after the last notification this hook can receive.
    PyObject* cb = a->notify_cb;
    if (cb == NULL) return;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* r = PyObject_CallNoArgs(cb);
    if (r == NULL) PyErr_WriteUnraisable(cb); else Py_DECREF(r);
    PyGILState_Release(g);
}

static AdminHandle* admin_alloc(void) {
    AdminHandle* a = (AdminHandle*)PyMem_Calloc(1, sizeof(AdminHandle));
    if (a == NULL) PyErr_NoMemory();
    return a;
}

// Admin_AdminClient_new(config: dict[str, str]) -> handle
// Raises RuntimeError with the Rust error's message when the config is
// rejected or the client cannot be created (as KafkaProducer_new does).
static PyObject* py_Admin_AdminClient_new(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* config_dict;
    if (!PyArg_ParseTuple(args, "O", &config_dict)) return NULL;
    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }
    // A C-built kafka_Map_t holds BORROWED char* -> char*: the strings stay
    // owned by the dict's str objects, which outlive this call.
    kafka_Map_t* props = NULL;
    if (ffi_raise((props = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_Check(key) ? PyUnicode_AsUTF8(key) : NULL;
        const char* v = PyUnicode_Check(value) ? PyUnicode_AsUTF8(value) : NULL;
        if (k == NULL || v == NULL || ffi_raise((kafka_Map_put_err(props, (void*)k, (void*)v, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_Map_destroy_err(props, &ffi_err), ffi_take(&ffi_err)));
            if (!PyErr_Occurred())
                PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
    }
    kafka_admin_AdminClientConfig_t* config = NULL;
    kafka_common_Error_t* err = (config = kafka_admin_AdminClientConfig_new(props, &ffi_err), ffi_take(&ffi_err));
    ffi_unraisable((kafka_Map_destroy_err(props, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_Admin_t* client = NULL;
    if (err == NULL) {
        Py_BEGIN_ALLOW_THREADS
        err = (client = kafka_admin_AdminClient_create(config, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
        ffi_unraisable((kafka_admin_AdminClientConfig_destroy_err(config, &ffi_err), ffi_take(&ffi_err)));
    }
    if (err != NULL) {
        const char* msg = NULL;
        if (!ffi_raise((msg = kafka_common_Error_message_err(err, &ffi_err), ffi_take(&ffi_err)))) {
            PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create AdminClient");
        }
        ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
        return NULL;
    }
    AdminHandle* a = admin_alloc();
    if (a != NULL) {
        a->owned = client;
        a->admin = client;
        if (ffi_raise((kafka_admin_Admin__set_callbacks_notify_err(a->admin, admin_callbacks_notify, a, &ffi_err), ffi_take(&ffi_err)))) {
            PyMem_Free(a);
            a = NULL;
        }
    }
    if (a == NULL) {
        Py_BEGIN_ALLOW_THREADS
        err = (kafka_admin_Admin_destroy_err(client, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
        ffi_unraisable(err);
        return NULL;
    }
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)a);
}

// Admin_MockAdminClient_new(num_brokers: int) -> handle
static PyObject* py_Admin_MockAdminClient_new(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    int num_brokers;
    if (!PyArg_ParseTuple(args, "i", &num_brokers)) return NULL;
    kafka_admin_MockAdminClient_t* mock = NULL;
    kafka_common_Error_t* err = (mock = kafka_admin_MockAdminClient_create((int32_t)num_brokers, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        const char* msg = NULL;
        if (!ffi_raise((msg = kafka_common_Error_message_err(err, &ffi_err), ffi_take(&ffi_err)))) {
            PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create MockAdminClient");
        }
        ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
        return NULL;
    }
    AdminHandle* a = admin_alloc();
    if (a != NULL) {
        a->mock = mock;
        if (ffi_raise((a->admin = kafka_admin_MockAdminClient__as_Admin_err(mock, &ffi_err), ffi_take(&ffi_err)))  // borrowed view
            || ffi_raise((kafka_admin_Admin__set_callbacks_notify_err(a->admin, admin_callbacks_notify, a, &ffi_err), ffi_take(&ffi_err)))) {
            PyMem_Free(a);
            a = NULL;
        }
    }
    if (a == NULL) {
        Py_BEGIN_ALLOW_THREADS
        err = (kafka_admin_MockAdminClient_destroy_err(mock, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
        ffi_unraisable(err);
        return NULL;
    }
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)a);
}

// Admin_close(handle, timeout_ms): Java close() / close(Duration). Blocks
// (GIL released); -1 is the no-argument form. Does not free the handle. The
// Python clients use `Admin_close_cb` instead; this stays for callers that
// want a native block.
static PyObject* py_Admin_close(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KL", &h, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (timeout_ms < 0) err = (kafka_admin_Admin_close_err(a->admin, &ffi_err), ffi_take(&ffi_err));
    else err = (kafka_admin_Admin_close_with_timeout_err(a->admin, (int64_t)timeout_ms, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (ffi_raise(err)) return NULL;
    Py_RETURN_NONE;
}

// Completion of `Admin_close_cb`: `cb()` runs from `Admin__execute_callbacks`
// (or from `Admin_destroy`, which runs the still-pending callbacks). The C
// API's close callback carries no error (`kafka_admin_Admin_close_cb_t` takes
// only the opaque), so the Python callback takes no arguments. Reuses the
// producer's `VoidCbCtx` for the callable reference.
static void admin_close_cb(void* opaque) {
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    PyObject* r = PyObject_CallNoArgs(ctx->cb);
    if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// Admin_close_cb(handle, timeout_ms, cb): the queued twin of Admin_close.
// Negative timeout_ms is Java's no-argument close(); otherwise
// close(Duration). Returns at once; `cb()` is queued on the client's
// callbacks vector when the close completed, and the notify hook fires if
// the vector was empty. Does not free the handle: the caller drains the
// completion (the Python waiter / loop pump) and then calls Admin_destroy.
static PyObject* py_Admin_close_cb(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &timeout_ms, &cb)) return NULL;
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) return NULL;
    AdminHandle* a = admin_from_handle(h);
    Py_BEGIN_ALLOW_THREADS
    if (timeout_ms < 0) kafka_admin_Admin_close_cb(a->admin, admin_close_cb, ctx);
    else kafka_admin_Admin_close_with_timeout_cb(a->admin, (int64_t)timeout_ms, admin_close_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// Admin_destroy(handle): frees the client (the owned Admin_t, or the mock
// handle whose __as_Admin view `admin` borrowed) and the handle struct.
// `_destroy` runs every still-pending callback exactly once, so it must run
// with the GIL released (the callbacks re-acquire it).
static PyObject* py_Admin_destroy(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    // Silence the notify hook first: `_destroy` runs the pending callbacks
    // itself, and a scheduled drain racing the teardown would touch a freed
    // client. The callable stays alive until `_destroy` has returned.
    PyObject* notify_cb = a->notify_cb;
    a->notify_cb = NULL;
    kafka_common_Error_t* err = NULL;
    Py_BEGIN_ALLOW_THREADS
    if (a->owned) err = (kafka_admin_Admin_destroy_err(a->owned, &ffi_err), ffi_take(&ffi_err));
    else if (a->mock) err = (kafka_admin_MockAdminClient_destroy_err(a->mock, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    // The client is freed even when its destroy panicked.
    ffi_unraisable(err);
    Py_XDECREF(notify_cb);
    PyMem_Free(a);
    Py_RETURN_NONE;
}

// Admin__set_callbacks_notify(handle, callable | None): the Python callable the
// notify hook invokes (from a Rust task). Call once, right after creation and
// before the first `_cb` resolve: swapping it while a notification is in
// flight is not synchronized against the hook.
static PyObject* py_Admin__set_callbacks_notify(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* old = a->notify_cb;
    if (cb == Py_None) {
        a->notify_cb = NULL;
    } else {
        Py_INCREF(cb);
        a->notify_cb = cb;
    }
    Py_XDECREF(old);
    Py_RETURN_NONE;
}

// Admin__execute_callbacks(handle) -> int: runs the queued callbacks on this
// thread (GIL released around the drain; each callback re-acquires it).
static PyObject* py_Admin__execute_callbacks(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    int32_t n = 0;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = (n = kafka_admin_Admin__execute_callbacks_err(a->admin, &ffi_err), ffi_take(&ffi_err));
    Py_END_ALLOW_THREADS
    if (ffi_raise(err)) return NULL;
    return PyLong_FromLong((long)n);
}

// ---- error helpers -------------------------------------------------------------

// (code, message | None, is_retriable, is_fatal) -- the 4-tuple
// `admin._to_error` turns into a KafkaError; None for a NULL error.
static PyObject* borrowed_error_to_py(const kafka_common_Error_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    if (e == NULL) Py_RETURN_NONE;
    kafka_common_Error_e code = kafka_common_Error_UNKNOWN_SERVER_ERROR;
    const char* message = NULL;
    int8_t retriable = 0;
    int fatal = 0;
    if (ffi_raise((code = kafka_common_Error__enum_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((message = kafka_common_Error_message_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((retriable = kafka_common_Error_is_retriable_error_err(e, &ffi_err), ffi_take(&ffi_err)))
        || (fatal = error_is_fatal(e)) < 0) return NULL;
    return Py_BuildValue("(izii)", code, message, retriable ? 1 : 0, fatal);
}

// Builds the (error, value) pair a keyed result maps a key to, consuming both
// references on every path and returning NULL on failure with an exception
// already set by whichever step failed.
//
// Callers must NOT Py_XDECREF the arguments after a NULL return. Py_BuildValue's
// 'N' unit steals its argument's reference even when the build itself fails:
// CPython's do_mktuple routes a PyTuple_New failure through do_ignore, which
// re-runs do_mkvalue over the remaining format units and releases each result.
static PyObject* error_value_pair(PyObject* err, PyObject* value) {
    if (err == NULL || value == NULL) {
        Py_XDECREF(err);
        Py_XDECREF(value);
        return NULL;
    }
    return Py_BuildValue("(NN)", err, value);
}

// `"<prefix>: <message of err>"` as an owned illegal-argument error; `err` is
// consumed. This is how the per-row marshaling errors the previous binding
// produced in Rust ("acl at index 0: ...", "offset at index 0: ...") are
// composed now that the typed constructors report the bare Java message.
static kafka_common_Error_t* prefixed_error(const char* prefix, kafka_common_Error_t* err) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* msg = NULL;
    kafka_common_Error_t* panic = (msg = kafka_common_Error_message_err(err, &ffi_err), ffi_take(&ffi_err));
    if (panic != NULL) {
        // The accessor's own failure replaces the error it was reading.
        ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
        return panic;
    }
    if (msg == NULL) msg = "";
    size_t len = strlen(prefix) + 2 + strlen(msg) + 1;
    char* buf = (char*)PyMem_Malloc(len);
    kafka_common_Error_t* out;
    if (buf == NULL) {
        out = ffi_built(kafka_common_Error_local_illegal_argument_err(msg, &ffi_err), &ffi_err);
    } else {
        snprintf(buf, len, "%s: %s", prefix, msg);
        out = ffi_built(kafka_common_Error_local_illegal_argument_err(buf, &ffi_err), &ffi_err);
        PyMem_Free(buf);
    }
    ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// The (job_handle, error_handle) pair every RPC entry point returns: exactly one
// of the two is non-zero. The error is owned by Python (KafkaError._from_c).
static PyObject* job_or_error(void* job, kafka_common_Error_t* err) {
    return Py_BuildValue("(KK)", (unsigned long long)(uintptr_t)job,
                         (unsigned long long)(uintptr_t)err);
}

// ---- resolution job ------------------------------------------------------------

struct AdminJob;
struct AdminSlot;

// Converts the BORROWED value a future delivered into its raw Python shape.
// GIL held. NULL with an exception set on failure.
typedef PyObject* (*admin_conv_fn)(struct AdminJob* job, struct AdminSlot* slot, void* value);
// Runs after a slot resolved (GIL held); may add further slots to the job
// (two-stage results such as listTransactions' byBrokerId). 0 / -1.
typedef int (*admin_cont_fn)(struct AdminJob* job, struct AdminSlot* slot);
// Builds the final payload from the resolved slots (GIL held).
typedef PyObject* (*admin_assemble_fn)(struct AdminJob* job);

typedef struct AdminSlot {
    struct AdminJob* job;
    kafka_common_KafkaFuture_t* future;  // never NULL
    int owned;                           // destroy the future at job end
    PyObject* key;                       // owned, or NULL
    admin_conv_fn conv;                  // NULL: a Void future, value -> None
    admin_cont_fn cont;
    int tag;                             // per-result discriminator
    PyObject* value;                     // owned, set once resolved (None on error)
    PyObject* error;                     // owned error4 tuple, or NULL when ok
    int done;
} AdminSlot;

typedef struct AdminJob {
    AdminSlot** slots;
    int n, cap;
    kafka_Map_t** maps;                  // owned maps (own their futures) destroyed at job end
    int n_maps, maps_cap;
    void* result;
    void (*result_destroy)(void*, kafka_common_Error_t**);  // a `<Rpc>Result_destroy_err`
    admin_assemble_fn assemble;
    void* ctx;                           // result-specific, freed by ctx_free
    void (*ctx_free)(void*);
    PyObject* callback;                  // asyncio: cb(payload, exc); NULL when sync
    int is_async;
    int outstanding;                     // async: futures not yet delivered
    PyObject *exc_type, *exc_value, *exc_tb;  // first conversion failure
} AdminJob;

static AdminJob* job_new(void* result, void (*result_destroy)(void*, kafka_common_Error_t**), admin_assemble_fn assemble) {
    AdminJob* job = (AdminJob*)PyMem_Calloc(1, sizeof(AdminJob));
    if (job == NULL) { PyErr_NoMemory(); return NULL; }
    job->result = result;
    job->result_destroy = result_destroy;
    job->assemble = assemble;
    return job;
}

static void job_free(AdminJob* job) {
    kafka_common_Error_t *ffi_err = NULL;
    for (int i = 0; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        if (s->owned && s->future) ffi_unraisable((kafka_common_KafkaFuture_destroy_err(s->future, &ffi_err), ffi_take(&ffi_err)));
        Py_XDECREF(s->key); Py_XDECREF(s->value); Py_XDECREF(s->error);
        PyMem_Free(s);
    }
    PyMem_Free(job->slots);
    for (int i = 0; i < job->n_maps; i++) ffi_unraisable((kafka_Map_destroy_err(job->maps[i], &ffi_err), ffi_take(&ffi_err)));
    PyMem_Free(job->maps);
    if (job->result && job->result_destroy) {
        kafka_common_Error_t* e = NULL;
        job->result_destroy(job->result, &e);
        ffi_unraisable(e);
    }
    if (job->ctx && job->ctx_free) job->ctx_free(job->ctx);
    Py_XDECREF(job->callback);
    Py_XDECREF(job->exc_type); Py_XDECREF(job->exc_value); Py_XDECREF(job->exc_tb);
    PyMem_Free(job);
}

// Records the currently set Python exception as the job's failure (first one
// wins) and clears the indicator.
static void job_fail(AdminJob* job) {
    if (job->exc_type != NULL || !PyErr_Occurred()) { PyErr_Clear(); return; }
    PyErr_Fetch(&job->exc_type, &job->exc_value, &job->exc_tb);
}

// Adds a slot. `key` is STOLEN (may be NULL). On failure (NULL return) the key
// is released, the future destroyed when `owned`, and an exception is set.
static AdminSlot* job_add_slot(AdminJob* job, kafka_common_KafkaFuture_t* future, int owned,
                               PyObject* key, admin_conv_fn conv, admin_cont_fn cont, int tag) {
    kafka_common_Error_t *ffi_err = NULL;
    if (future == NULL) {
        Py_XDECREF(key);
        PyErr_SetString(PyExc_RuntimeError, "admin result exposed no future");
        return NULL;
    }
    if (job->n == job->cap) {
        int cap = job->cap ? job->cap * 2 : 8;
        AdminSlot** grown = (AdminSlot**)PyMem_Realloc(job->slots, (size_t)cap * sizeof(*grown));
        if (grown == NULL) {
            Py_XDECREF(key);
            if (owned) ffi_unraisable((kafka_common_KafkaFuture_destroy_err(future, &ffi_err), ffi_take(&ffi_err)));
            PyErr_NoMemory();
            return NULL;
        }
        job->slots = grown; job->cap = cap;
    }
    AdminSlot* s = (AdminSlot*)PyMem_Calloc(1, sizeof(AdminSlot));
    if (s == NULL) {
        Py_XDECREF(key);
        if (owned) ffi_unraisable((kafka_common_KafkaFuture_destroy_err(future, &ffi_err), ffi_take(&ffi_err)));
        PyErr_NoMemory();
        return NULL;
    }
    s->job = job; s->future = future; s->owned = owned; s->key = key;
    s->conv = conv; s->cont = cont; s->tag = tag;
    job->slots[job->n++] = s;
    return s;
}

// Registers an owned map (`*_values()` and friends) for destruction at job
// end. A NULL map is ignored.
static int job_own_map(AdminJob* job, kafka_Map_t* map) {
    kafka_common_Error_t *ffi_err = NULL;
    if (map == NULL) return 0;
    if (job->n_maps == job->maps_cap) {
        int cap = job->maps_cap ? job->maps_cap * 2 : 4;
        kafka_Map_t** grown = (kafka_Map_t**)PyMem_Realloc(job->maps, (size_t)cap * sizeof(*grown));
        if (grown == NULL) { ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err))); PyErr_NoMemory(); return -1; }
        job->maps = grown; job->maps_cap = cap;
    }
    job->maps[job->n_maps++] = map;
    return 0;
}

static void job_dispatch_range(AdminJob* job, int from, int to);

// Resolves one slot: `get` (immediate when the future already completed, as
// it has in the `_cb` path) and conversion while the value is still borrowed
// from the live future. GIL held on entry (the sync path releases it around
// the blocking `get`).
static void slot_resolve(AdminJob* job, AdminSlot* slot) {
    kafka_common_Error_t *ffi_err = NULL;
    void* value = NULL;
    kafka_common_Error_t* err;
    if (job->is_async) {
        err = (value = kafka_common_KafkaFuture_get(slot->future, &ffi_err), ffi_take(&ffi_err));
    } else {
        Py_BEGIN_ALLOW_THREADS
        err = (value = kafka_common_KafkaFuture_get(slot->future, &ffi_err), ffi_take(&ffi_err));
        Py_END_ALLOW_THREADS
    }
    slot->done = 1;
    if (err != NULL) {
        slot->error = borrowed_error_to_py(err);
        ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err)));
        if (slot->error == NULL) { job_fail(job); return; }
        Py_INCREF(Py_None);
        slot->value = Py_None;
    } else if (job->exc_type != NULL) {
        // A previous slot already failed: do not convert, the payload is lost.
        Py_INCREF(Py_None);
        slot->value = Py_None;
    } else if (slot->conv != NULL) {
        slot->value = slot->conv(job, slot, value);
        if (slot->value == NULL) { job_fail(job); return; }
    } else {
        Py_INCREF(Py_None);
        slot->value = Py_None;
    }
    if (slot->cont != NULL && job->exc_type == NULL) {
        int before = job->n;
        if (slot->cont(job, slot) < 0) { job_fail(job); return; }
        if (job->is_async && job->n > before) job_dispatch_range(job, before, job->n);
    }
}

// The payload (or NULL with the job's exception restored). GIL held.
static PyObject* job_payload(AdminJob* job) {
    if (job->exc_type != NULL) {
        PyErr_Restore(job->exc_type, job->exc_value, job->exc_tb);
        job->exc_type = job->exc_value = job->exc_tb = NULL;
        return NULL;
    }
    return job->assemble(job);
}

// Async completion: cb(payload, None) or cb(None, exception). GIL held.
static void job_finish_async(AdminJob* job) {
    PyObject* payload = job_payload(job);
    PyObject* r;
    if (payload == NULL) {
        PyObject *type = NULL, *value = NULL, *tb = NULL;
        PyErr_Fetch(&type, &value, &tb);
        PyErr_NormalizeException(&type, &value, &tb);
        PyErr_Clear();
        if (value == NULL) {
            value = PyObject_CallFunction(PyExc_RuntimeError, "s", "admin result conversion failed");
        }
        r = value ? PyObject_CallFunctionObjArgs(job->callback, Py_None, value, NULL) : NULL;
        Py_XDECREF(type); Py_XDECREF(value); Py_XDECREF(tb);
    } else {
        r = PyObject_CallFunctionObjArgs(job->callback, payload, Py_None, NULL);
        Py_DECREF(payload);
    }
    if (r == NULL) PyErr_WriteUnraisable(job->callback); else Py_DECREF(r);
    job_free(job);
}

// `kafka_common_KafkaFuture_get_cb_err` callback. The value and error delivered
// here are not used: the error is released and the slot re-reads the (now
// completed) future with `get`, whose value stays valid until the future is
// destroyed -- the same lifetime rule the sync path relies on. Runs on the
// thread draining `Admin__execute_callbacks` (GIL released there), or inline
// inside `get_cb` for a client-less future (GIL held); PyGILState_Ensure
// covers both.
static void admin_slot_cb(void* value, kafka_common_Error_t* error, void* opaque) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)value;
    AdminSlot* slot = (AdminSlot*)opaque;
    PyGILState_STATE g;
    if (!gil_ensure_unless_finalizing(&g)) return;
    if (error) ffi_unraisable((kafka_common_Error_destroy_err(error, &ffi_err), ffi_take(&ffi_err)));
    AdminJob* job = slot->job;
    slot_resolve(job, slot);
    if (--job->outstanding == 0) job_finish_async(job);
    PyGILState_Release(g);
}

// Issues `get_cb` for slots [from, to). `outstanding` is raised first so an
// inline delivery cannot finish the job before every slot is dispatched.
static void job_dispatch_range(AdminJob* job, int from, int to) {
    job->outstanding += to - from;
    for (int i = from; i < to; i++) {
        // A panic is delivered to admin_slot_cb like any other outcome.
        kafka_common_KafkaFuture_get_cb(job->slots[i]->future, admin_slot_cb, job->slots[i]);
    }
}

// Admin_resolve(job) -> payload. Blocking form: blocks on every future in turn
// with the GIL released, converts, destroys everything, returns the raw
// payload. Not used by the Python clients any more (see the section comment).
static PyObject* py_Admin_resolve(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    AdminJob* job = (AdminJob*)(uintptr_t)h;
    job->is_async = 0;
    // `n` may grow while iterating: a slot's continuation adds the second
    // stage, which this loop then reaches.
    for (int i = 0; i < job->n; i++) slot_resolve(job, job->slots[i]);
    PyObject* payload = job_payload(job);
    job_free(job);
    return payload;
}

// Admin_resolve_cb(job, cb): both Python clients. cb(payload, None) /
// cb(None, exception) once every future delivered, on the thread running
// Admin__execute_callbacks (or inline, for a job with no futures or
// client-less ones).
static PyObject* py_Admin_resolve_cb(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    AdminJob* job = (AdminJob*)(uintptr_t)h;
    job->is_async = 1;
    Py_INCREF(cb);
    job->callback = cb;
    if (job->n == 0) {
        job_finish_async(job);
        Py_RETURN_NONE;
    }
    // Hold one extra count while dispatching so a job whose last future fires
    // inline during the loop still finishes exactly once, here.
    job->outstanding = 1;
    job_dispatch_range(job, 0, job->n);
    if (--job->outstanding == 0) job_finish_async(job);
    Py_RETURN_NONE;
}

// Admin_job_discard(job): frees an unresolved job (its result and futures).
static PyObject* py_Admin_job_discard(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    job_free((AdminJob*)(uintptr_t)h);
    Py_RETURN_NONE;
}

// ---- generic assemblers --------------------------------------------------------

// {key: (error4 | None, value)}
static PyObject* assemble_keyed_pairs(AdminJob* job) {
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    for (int i = 0; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        PyObject* err = s->error ? s->error : Py_None;
        PyObject* pair = Py_BuildValue("(OO)", err, s->value);
        if (pair == NULL || PyDict_SetItem(d, s->key, pair) < 0) {
            Py_XDECREF(pair); Py_DECREF(d); return NULL;
        }
        Py_DECREF(pair);
    }
    return d;
}

// {key: error4 | None}
static PyObject* assemble_keyed_errors(AdminJob* job) {
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    for (int i = 0; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        if (PyDict_SetItem(d, s->key, s->error ? s->error : Py_None) < 0) { Py_DECREF(d); return NULL; }
    }
    return d;
}

// (error4 | None, value) from the single slot.
static PyObject* assemble_single(AdminJob* job) {
    if (job->n != 1) {
        PyErr_SetString(PyExc_RuntimeError, "single-future result with an unexpected slot count");
        return NULL;
    }
    AdminSlot* s = job->slots[0];
    return Py_BuildValue("(OO)", s->error ? s->error : Py_None, s->value);
}

// (error4 | None, (value0, value1, ...)) from several whole-call futures: the
// first failure wins, as it would for Java callers awaiting each in turn.
static PyObject* assemble_multi(AdminJob* job) {
    PyObject* values = PyTuple_New(job->n);
    if (values == NULL) return NULL;
    PyObject* err = Py_None;
    for (int i = 0; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        if (err == Py_None && s->error) err = s->error;
        Py_INCREF(s->value);
        PyTuple_SET_ITEM(values, i, s->value);
    }
    return Py_BuildValue("(ON)", err, values);
}

// ---- key / enum / scalar converters ----------------------------------------------

static PyObject* optional_str_to_py(const char* text) {
    if (text == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(text);
}

// `Uuid.toString()` (base64, the topic-id form the Python surface uses).
static PyObject* uuid_to_py(const kafka_common_Uuid_t* id) {
    kafka_common_Error_t *ffi_err = NULL;
    if (id == NULL) Py_RETURN_NONE;
    char* s = NULL;
    if (ffi_raise((s = kafka_common_Uuid_to_string_err(id, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* out = PyUnicode_FromString(s ? s : "");
    if (s) ffi_unraisable((kafka_string_destroy_err(s, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// An owned Uuid (TopicDescription/TopicListing topic_id getters) -> str; frees it.
static PyObject* owned_uuid_to_py(kafka_common_Uuid_t* id) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* out = uuid_to_py(id);
    if (id) ffi_unraisable((kafka_common_Uuid_destroy_err(id, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// An owned enum `toString()` -> str (None for a NULL singleton). `err` is the
// `_to_string_err` call that produced `s`: raised when set, `s` then unwritten.
static PyObject* owned_string_to_py(kafka_common_Error_t* err, char* s) {
    kafka_common_Error_t *ffi_err = NULL;
    if (ffi_raise(err)) return NULL;
    if (s == NULL) Py_RETURN_NONE;
    PyObject* out = PyUnicode_FromString(s);
    ffi_unraisable((kafka_string_destroy_err(s, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

static PyObject* group_state_to_py(const kafka_common_GroupState_t* s) {
    kafka_common_Error_t *ffi_err = NULL;
    if (s == NULL) Py_RETURN_NONE;
    char* text = NULL;
    kafka_common_Error_t* err = (text = kafka_common_GroupState_to_string_err(s, &ffi_err), ffi_take(&ffi_err));
    return owned_string_to_py(err, text);
}

static PyObject* group_type_to_py(const kafka_common_GroupType_t* t) {
    kafka_common_Error_t *ffi_err = NULL;
    if (t == NULL) Py_RETURN_NONE;
    char* text = NULL;
    kafka_common_Error_t* err = (text = kafka_common_GroupType_to_string_err(t, &ffi_err), ffi_take(&ffi_err));
    return owned_string_to_py(err, text);
}

static PyObject* classic_group_state_to_py(const kafka_common_ClassicGroupState_t* s) {
    kafka_common_Error_t *ffi_err = NULL;
    if (s == NULL) Py_RETURN_NONE;
    char* text = NULL;
    kafka_common_Error_t* err = (text = kafka_common_ClassicGroupState_to_string_err(s, &ffi_err), ffi_take(&ffi_err));
    return owned_string_to_py(err, text);
}

static PyObject* transaction_state_to_py(const kafka_admin_TransactionState_t* s) {
    kafka_common_Error_t *ffi_err = NULL;
    if (s == NULL) Py_RETURN_NONE;
    char* text = NULL;
    kafka_common_Error_t* err = (text = kafka_admin_TransactionState_to_string_err(s, &ffi_err), ffi_take(&ffi_err));
    return owned_string_to_py(err, text);
}

// Java constant names, which the Python `ConfigSource` surface exposes. NULL
// with the error raised when the read fails.
static const char* config_source_name(const kafka_admin_ConfigEntry_ConfigSource_t* s) {
    kafka_common_Error_t *ffi_err = NULL;
    if (s == NULL) return "UNKNOWN";
    kafka_admin_ConfigEntry_ConfigSource_e e = kafka_admin_ConfigEntry_ConfigSource_UNKNOWN;
    if (ffi_raise((e = kafka_admin_ConfigEntry_ConfigSource__enum_err(s, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    switch (e) {
        case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_TOPIC_CONFIG: return "DYNAMIC_TOPIC_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_LOGGER_CONFIG: return "DYNAMIC_BROKER_LOGGER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_CONFIG: return "DYNAMIC_BROKER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_DEFAULT_BROKER_CONFIG: return "DYNAMIC_DEFAULT_BROKER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_CLIENT_METRICS_CONFIG: return "DYNAMIC_CLIENT_METRICS_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_GROUP_CONFIG: return "DYNAMIC_GROUP_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_STATIC_BROKER_CONFIG: return "STATIC_BROKER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_DEFAULT_CONFIG: return "DEFAULT_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_UNKNOWN: return "UNKNOWN";
    }
    return "UNKNOWN";
}

static const char* config_type_name(const kafka_admin_ConfigEntry_ConfigType_t* t) {
    kafka_common_Error_t *ffi_err = NULL;
    if (t == NULL) return "UNKNOWN";
    kafka_admin_ConfigEntry_ConfigType_e e = kafka_admin_ConfigEntry_ConfigType_UNKNOWN;
    if (ffi_raise((e = kafka_admin_ConfigEntry_ConfigType__enum_err(t, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    switch (e) {
        case kafka_admin_ConfigEntry_ConfigType_BOOLEAN: return "BOOLEAN";
        case kafka_admin_ConfigEntry_ConfigType_STRING: return "STRING";
        case kafka_admin_ConfigEntry_ConfigType_INT: return "INT";
        case kafka_admin_ConfigEntry_ConfigType_SHORT: return "SHORT";
        case kafka_admin_ConfigEntry_ConfigType_LONG: return "LONG";
        case kafka_admin_ConfigEntry_ConfigType_DOUBLE: return "DOUBLE";
        case kafka_admin_ConfigEntry_ConfigType_LIST: return "LIST";
        case kafka_admin_ConfigEntry_ConfigType_CLASS: return "CLASS";
        case kafka_admin_ConfigEntry_ConfigType_PASSWORD: return "PASSWORD";
        case kafka_admin_ConfigEntry_ConfigType_UNKNOWN: return "UNKNOWN";
    }
    return "UNKNOWN";
}

// An OWNED list of borrowed AclOperation singletons -> [code, ...], or None
// when the broker did not report the set (NULL list, Java's null). Frees it.
static PyObject* acl_operations_to_py(kafka_List_t* ops) {
    kafka_common_Error_t *ffi_err = NULL;
    if (ops == NULL) Py_RETURN_NONE;
    int32_t n = 0;
    PyObject* out = ffi_raise((n = kafka_List_size_err(ops, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { ffi_unraisable((kafka_List_destroy_err(ops, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* op = NULL;
        int8_t value = 0;
        PyObject* code = ffi_raise((op = kafka_List_get_err(ops, i, &ffi_err), ffi_take(&ffi_err)))
                || ffi_raise((value = kafka_common_acl_AclOperation_code_err(
                       (const kafka_common_acl_AclOperation_t*)op, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : PyLong_FromLong(value);
        if (code == NULL) { Py_DECREF(out); ffi_unraisable((kafka_List_destroy_err(ops, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(out, i, code);
    }
    ffi_unraisable((kafka_List_destroy_err(ops, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// An OWNED list of owned int32_t* -> [int, ...]; frees it. NULL -> [].
static PyObject* int32_list_to_py(kafka_List_t* list) {
    kafka_common_Error_t *ffi_err = NULL;
    if (list == NULL) return PyList_New(0);
    int32_t n = 0;
    PyObject* out = ffi_raise((n = kafka_List_size_err(list, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* item = NULL;
        PyObject* v = ffi_raise((item = kafka_List_get_err(list, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : PyLong_FromLong(*(const int32_t*)item);
        if (v == NULL) { Py_DECREF(out); ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(out, i, v);
    }
    ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

static PyObject* tp_replica_to_py(const kafka_common_TopicPartitionReplica_t* r) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = 0;
    int32_t broker_id = 0;
    if (ffi_raise((topic = kafka_common_TopicPartitionReplica_topic_err(r, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((partition = kafka_common_TopicPartitionReplica_partition_err(r, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((broker_id = kafka_common_TopicPartitionReplica_broker_id_err(r, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return Py_BuildValue("(sii)", topic ? topic : "", (int)partition, (int)broker_id);
}

// (type_id, name) -- the key `_to_describe_configs` rebuilds a ConfigResource from.
static PyObject* config_resource_to_py(const kafka_common_config_ConfigResource_t* r) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* name = NULL;
    const kafka_common_config_ConfigResource_Type_t* type = NULL;
    int8_t type_id = 0;
    if (ffi_raise((name = kafka_common_config_ConfigResource_name_err(r, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((type = kafka_common_config_ConfigResource_type_err(r, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((type_id = kafka_common_config_ConfigResource_Type_id_err(type, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return Py_BuildValue("(is)", (int)type_id, name ? name : "");
}

// (resource_type, name, pattern_type, principal, host, operation, permission)
// -- the four enums as Java's `code()`.
static PyObject* acl_binding_to_py(const kafka_common_acl_AclBinding_t* b) {
    kafka_common_Error_t *ffi_err = NULL;
    if (b == NULL) Py_RETURN_NONE;
    const kafka_common_resource_ResourcePattern_t* pattern = NULL;
    const kafka_common_acl_AccessControlEntry_t* entry = NULL;
    const kafka_common_resource_ResourceType_t* resource_type = NULL;
    const kafka_common_resource_PatternType_t* pattern_type = NULL;
    const kafka_common_acl_AclOperation_t* operation = NULL;
    const kafka_common_acl_AclPermissionType_t* permission = NULL;
    const char *name = NULL, *principal = NULL, *host = NULL;
    int8_t resource_code = 0, pattern_code = 0, operation_code = 0, permission_code = 0;
    if (ffi_raise((pattern = kafka_common_acl_AclBinding_pattern_err(b, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((entry = kafka_common_acl_AclBinding_entry_err(b, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((resource_type = kafka_common_resource_ResourcePattern_resource_type_err(pattern, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((resource_code = kafka_common_resource_ResourceType_code_err(resource_type, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((name = kafka_common_resource_ResourcePattern_name_err(pattern, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((pattern_type = kafka_common_resource_ResourcePattern_pattern_type_err(pattern, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((pattern_code = kafka_common_resource_PatternType_code_err(pattern_type, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((principal = kafka_common_acl_AccessControlEntry_principal_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((host = kafka_common_acl_AccessControlEntry_host_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((operation = kafka_common_acl_AccessControlEntry_operation_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((operation_code = kafka_common_acl_AclOperation_code_err(operation, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((permission = kafka_common_acl_AccessControlEntry_permission_type_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((permission_code = kafka_common_acl_AclPermissionType_code_err(permission, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return Py_BuildValue("(isissii)", (int)resource_code, name, (int)pattern_code, principal, host,
                         (int)operation_code, (int)permission_code);
}

// Same seven fields, but the three strings use 'z' so a NULL (Java's "match
// any") becomes None.
static PyObject* acl_binding_filter_to_py(const kafka_common_acl_AclBindingFilter_t* f) {
    kafka_common_Error_t *ffi_err = NULL;
    if (f == NULL) Py_RETURN_NONE;
    const kafka_common_resource_ResourcePatternFilter_t* pattern = NULL;
    const kafka_common_acl_AccessControlEntryFilter_t* entry = NULL;
    const kafka_common_resource_ResourceType_t* resource_type = NULL;
    const kafka_common_resource_PatternType_t* pattern_type = NULL;
    const kafka_common_acl_AclOperation_t* operation = NULL;
    const kafka_common_acl_AclPermissionType_t* permission = NULL;
    const char *name = NULL, *principal = NULL, *host = NULL;
    int8_t resource_code = 0, pattern_code = 0, operation_code = 0, permission_code = 0;
    if (ffi_raise((pattern = kafka_common_acl_AclBindingFilter_pattern_filter_err(f, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((entry = kafka_common_acl_AclBindingFilter_entry_filter_err(f, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((resource_type = kafka_common_resource_ResourcePatternFilter_resource_type_err(pattern, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((resource_code = kafka_common_resource_ResourceType_code_err(resource_type, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((name = kafka_common_resource_ResourcePatternFilter_name_err(pattern, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((pattern_type = kafka_common_resource_ResourcePatternFilter_pattern_type_err(pattern, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((pattern_code = kafka_common_resource_PatternType_code_err(pattern_type, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((principal = kafka_common_acl_AccessControlEntryFilter_principal_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((host = kafka_common_acl_AccessControlEntryFilter_host_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((operation = kafka_common_acl_AccessControlEntryFilter_operation_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((operation_code = kafka_common_acl_AclOperation_code_err(operation, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((permission = kafka_common_acl_AccessControlEntryFilter_permission_type_err(entry, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((permission_code = kafka_common_acl_AclPermissionType_code_err(permission, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return Py_BuildValue("(izizzii)", (int)resource_code, name, (int)pattern_code, principal, host,
                         (int)operation_code, (int)permission_code);
}

// ((entity_type, entity_name_or_None), ...) -- a None name is Java's null map
// value, the built-in default entity, which is not the empty name.
static PyObject* client_quota_entity_to_py(const kafka_common_quota_ClientQuotaEntity_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    if (e == NULL) Py_RETURN_NONE;
    // `entries()` is an owned map of entity type -> entity name (NULL for the
    // default entity), sorted by type; destroyed once copied out.
    kafka_Map_t* entries = NULL;
    if (ffi_raise((entries = kafka_common_quota_ClientQuotaEntity_entries_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    int32_t n = 0;
    PyObject* pairs = entries && ffi_raise((n = kafka_Map_size_err(entries, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyTuple_New(n < 0 ? 0 : n);
    if (pairs == NULL) { if (entries) ffi_unraisable((kafka_Map_destroy_err(entries, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* key = NULL;
        void* value = NULL;
        PyObject* pair = ffi_raise((key = kafka_Map_key_err(entries, i, &ffi_err), ffi_take(&ffi_err)))
                || ffi_raise((value = kafka_Map_value_err(entries, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : Py_BuildValue("(sz)", (const char*)key, (const char*)value);
        if (pair == NULL) { Py_DECREF(pairs); ffi_unraisable((kafka_Map_destroy_err(entries, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyTuple_SET_ITEM(pairs, i, pair);
    }
    if (entries) ffi_unraisable((kafka_Map_destroy_err(entries, &ffi_err), ffi_take(&ffi_err)));
    return pairs;
}

static PyObject* kafka_principal_to_py(const kafka_common_security_auth_KafkaPrincipal_t* p) {
    kafka_common_Error_t *ffi_err = NULL;
    if (p == NULL) Py_RETURN_NONE;
    const char* principal_type = NULL;
    const char* name = NULL;
    int8_t token_authenticated = 0;
    if (ffi_raise((principal_type = kafka_common_security_auth_KafkaPrincipal_principal_type_err(p, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((name = kafka_common_security_auth_KafkaPrincipal_name_err(p, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((token_authenticated = kafka_common_security_auth_KafkaPrincipal_token_authenticated_err(p, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return Py_BuildValue("(ssO)", principal_type, name, token_authenticated ? Py_True : Py_False);
}

// ---- options -----------------------------------------------------------------

// Runs `call`, an `_err` twin, only while `err` is still NULL, keeping its
// error: a sequence of steps stops at the first failure, and the caller frees
// what the steps built before raising `err`.
#define FFI_STEP(err, call) do { if ((err) == NULL) (err) = (call); } while (0)

// Every `<Rpc>Options_t` has `_set_timeout_ms(int32_t)`; -1 leaves Java's
// timeoutMs null so `default.api.timeout.ms` applies. A step on `err`.
#define ADMIN_SET_TIMEOUT(Rpc, options, timeout_ms, err) \
    do { \
        if ((err) == NULL && (timeout_ms) >= 0) \
            kafka_admin_##Rpc##Options_set_timeout_ms_err((options), (int32_t)(timeout_ms), &(err)); \
    } while (0)

// The setter takes Java's `Integer timeoutMs`, which holds at most INT32_MAX.
// Each admin entry checks its parsed timeout with this right after
// PyArg_ParseTuple, before it builds anything, so a larger value raises
// OverflowError instead of wrapping in ADMIN_SET_TIMEOUT's int32_t cast.
static int admin_timeout_fits(long long timeout_ms) {
    if (timeout_ms > INT32_MAX) {
        PyErr_Format(PyExc_OverflowError, "timeout of %lld ms does not fit a 32-bit signed int (max %d ms)",
                     timeout_ms, (int)INT32_MAX);
        return 0;
    }
    return 1;
}

// ---- Python -> C input builders ------------------------------------------------
//
// C-built containers hold BORROWED elements; every builder below owns what it
// put in and frees it after the RPC call (the RPC copies what it keeps).

// list[str] -> kafka_List_t of borrowed char*. Like str_list_build but with a
// caller-supplied error message.
static int str_list_build_msg(PyObject* seq, str_list_t* l, const char* what) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, what);
    if (fast == NULL) return -1;
    l->seq = fast;
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { str_list_free(l); return -1; }
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);  // borrowed
        const char* s = PyUnicode_Check(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (s == NULL) {
            if (!PyErr_Occurred()) PyErr_SetString(PyExc_TypeError, what);
            str_list_free(l);
            return -1;
        }
        if (ffi_raise((kafka_List_add_err(l->list, (void*)s, &ffi_err), ffi_take(&ffi_err)))) { str_list_free(l); return -1; }
    }
    return 0;
}

// list[int] -> kafka_List_t of int32_t* (owned array).
typedef struct {
    kafka_List_t* list;
    int32_t* vals;
    Py_ssize_t n;
} i32_list_t;

static void i32_list_free(i32_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    PyMem_Free(l->vals);
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    memset(l, 0, sizeof(*l));
}

static int i32_list_build(PyObject* seq, i32_list_t* l, const char* what) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, what);
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return -1; }
    l->vals = n > 0 ? PyMem_Calloc(n, sizeof(*l->vals)) : NULL;
    if (n > 0 && l->vals == NULL) { Py_DECREF(fast); i32_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        long v = PyLong_AsLong(PySequence_Fast_GET_ITEM(fast, i));
        if (v == -1 && PyErr_Occurred()) { Py_DECREF(fast); i32_list_free(l); return -1; }
        l->vals[i] = (int32_t)v;
        if (ffi_raise((kafka_List_add_err(l->list, &l->vals[i], &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); i32_list_free(l); return -1; }
    }
    Py_DECREF(fast);
    return 0;
}

// list[int] -> kafka_List_t of int64_t* (owned array).
typedef struct {
    kafka_List_t* list;
    int64_t* vals;
    Py_ssize_t n;
} i64_list_t;

static void i64_list_free(i64_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    PyMem_Free(l->vals);
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    memset(l, 0, sizeof(*l));
}

static int i64_list_build(PyObject* seq, i64_list_t* l, const char* what) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, what);
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return -1; }
    l->vals = n > 0 ? PyMem_Calloc(n, sizeof(*l->vals)) : NULL;
    if (n > 0 && l->vals == NULL) { Py_DECREF(fast); i64_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        long long v = PyLong_AsLongLong(PySequence_Fast_GET_ITEM(fast, i));
        if (v == -1 && PyErr_Occurred()) { Py_DECREF(fast); i64_list_free(l); return -1; }
        l->vals[i] = (int64_t)v;
        if (ffi_raise((kafka_List_add_err(l->list, &l->vals[i], &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); i64_list_free(l); return -1; }
    }
    Py_DECREF(fast);
    return 0;
}

// list[(topic, partition, broker_id)] -> kafka_List_t of TopicPartitionReplica_t*.
typedef struct {
    kafka_List_t* list;
    kafka_common_TopicPartitionReplica_t** replicas;
    Py_ssize_t n;
} tpr_list_t;

static void tpr_list_free(tpr_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->replicas && l->replicas[i]) ffi_unraisable((kafka_common_TopicPartitionReplica_destroy_err(l->replicas[i], &ffi_err), ffi_take(&ffi_err)));
    }
    PyMem_Free(l->replicas);
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    memset(l, 0, sizeof(*l));
}

// Each row is (topic, partition, broker_id) or, with `with_dir`, (topic,
// partition, broker_id, log_dir): the fourth column is returned through
// `dirs` (borrowed char*, owned by `fast`, which the caller must keep alive).
static int tpr_list_build(PyObject* seq, tpr_list_t* l, int with_dir, PyObject** fast_out,
                          const char*** dirs_out) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "expected a sequence of (topic, partition, broker_id) rows");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return -1; }
    l->replicas = n > 0 ? PyMem_Calloc(n, sizeof(*l->replicas)) : NULL;
    const char** dirs = (with_dir && n > 0) ? PyMem_Calloc(n, sizeof(*dirs)) : NULL;
    if (n > 0 && (l->replicas == NULL || (with_dir && dirs == NULL))) {
        Py_DECREF(fast); PyMem_Free(dirs); tpr_list_free(l); PyErr_NoMemory(); return -1;
    }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);
        const char* t = NULL; int p = 0; int b = 0; const char* dir = NULL;
        int ok = with_dir ? PyArg_ParseTuple(item, "siis", &t, &p, &b, &dir)
                          : PyArg_ParseTuple(item, "sii", &t, &p, &b);
        if (!ok) { Py_DECREF(fast); PyMem_Free(dirs); tpr_list_free(l); return -1; }
        if (ffi_raise((l->replicas[i] = kafka_common_TopicPartitionReplica_new_err(t, (int32_t)p, (int32_t)b, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((kafka_List_add_err(l->list, l->replicas[i], &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(fast); PyMem_Free(dirs); tpr_list_free(l); return -1;
        }
        if (with_dir) dirs[i] = dir;
    }
    if (fast_out) *fast_out = fast; else Py_DECREF(fast);
    if (dirs_out) *dirs_out = dirs; else PyMem_Free(dirs);
    return 0;
}

// list[str] of base64 topic ids -> kafka_List_t of Uuid_t*. A malformed id is
// reported as an owned error (`*err_out`, "invalid topic id: ...") rather than
// a Python exception, so the Python wrapper raises KafkaError as before.
typedef struct {
    kafka_List_t* list;
    kafka_common_Uuid_t** ids;
    Py_ssize_t n;
} uuid_list_t;

static void uuid_list_free(uuid_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->ids && l->ids[i]) ffi_unraisable((kafka_common_Uuid_destroy_err(l->ids[i], &ffi_err), ffi_take(&ffi_err)));
    }
    PyMem_Free(l->ids);
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    memset(l, 0, sizeof(*l));
}

static int uuid_list_build(PyObject* seq, uuid_list_t* l, kafka_common_Error_t** err_out) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    *err_out = NULL;
    PyObject* fast = PySequence_Fast(seq, "topic ids must be a sequence of str");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return -1; }
    l->ids = n > 0 ? PyMem_Calloc(n, sizeof(*l->ids)) : NULL;
    if (n > 0 && l->ids == NULL) { Py_DECREF(fast); uuid_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);
        const char* s = PyUnicode_Check(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (s == NULL) {
            if (!PyErr_Occurred()) PyErr_SetString(PyExc_TypeError, "topic ids must be str");
            Py_DECREF(fast); uuid_list_free(l); return -1;
        }
        kafka_common_Error_t* err = (l->ids[i] = kafka_common_Uuid_from_string(s, &ffi_err), ffi_take(&ffi_err));
        if (err != NULL) {
            *err_out = prefixed_error("invalid topic id", err);
            Py_DECREF(fast); uuid_list_free(l); return -1;
        }
        if (ffi_raise((kafka_List_add_err(l->list, l->ids[i], &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); uuid_list_free(l); return -1; }
    }
    Py_DECREF(fast);
    return 0;
}

// list[(principal_type, name)] -> kafka_List_t of KafkaPrincipal_t*.
typedef struct {
    kafka_List_t* list;
    kafka_common_security_auth_KafkaPrincipal_t** principals;
    Py_ssize_t n;
} principal_list_t;

static void principal_list_free(principal_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->principals && l->principals[i])
            ffi_unraisable((kafka_common_security_auth_KafkaPrincipal_destroy_err(l->principals[i], &ffi_err), ffi_take(&ffi_err)));
    }
    PyMem_Free(l->principals);
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    memset(l, 0, sizeof(*l));
}

static int principal_list_build(PyObject* seq, principal_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "principals must be a sequence of (type, name)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return -1; }
    l->principals = n > 0 ? PyMem_Calloc(n, sizeof(*l->principals)) : NULL;
    if (n > 0 && l->principals == NULL) { Py_DECREF(fast); principal_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* type = NULL; const char* name = NULL;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "ss", &type, &name)) {
            Py_DECREF(fast); principal_list_free(l); return -1;
        }
        if (ffi_raise((l->principals[i] = kafka_common_security_auth_KafkaPrincipal_new_err(type, name, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((kafka_List_add_err(l->list, l->principals[i], &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(fast); principal_list_free(l); return -1;
        }
    }
    Py_DECREF(fast);
    return 0;
}

// Checks that an input list has no sequence problems and returns its fast form
// (new reference) or NULL with an exception set.
static PyObject* fast_rows(PyObject* seq, const char* what) {
    return PySequence_Fast(seq, what);
}

// ---- data-class converters -------------------------------------------------------
//
// Every value here is BORROWED from the future that delivered it (or from a
// container the future owns), so the only things freed are the owned lists /
// maps / strings the *getters* hand out.

// BORROWED list of Node_t* -> [node_tuple]; NULL (Java null) -> None.
static PyObject* node_list_to_py_borrowed(const kafka_List_t* nodes) {
    kafka_common_Error_t *ffi_err = NULL;
    if (nodes == NULL) Py_RETURN_NONE;
    int32_t n = 0;
    if (ffi_raise((n = kafka_List_size_err(nodes, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        void* node = NULL;
        PyObject* item = ffi_raise((node = kafka_List_get_err(nodes, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : node_to_py((const kafka_common_Node_t*)node);
        if (item == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, item);
    }
    return out;
}

// BORROWED list of AclOperation singletons -> [code]; NULL -> None.
static PyObject* acl_operations_to_py_borrowed(const kafka_List_t* ops) {
    kafka_common_Error_t *ffi_err = NULL;
    if (ops == NULL) Py_RETURN_NONE;
    int32_t n = 0;
    if (ffi_raise((n = kafka_List_size_err(ops, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        void* op = NULL;
        int8_t value = 0;
        PyObject* code = ffi_raise((op = kafka_List_get_err(ops, i, &ffi_err), ffi_take(&ffi_err)))
                || ffi_raise((value = kafka_common_acl_AclOperation_code_err(
                       (const kafka_common_acl_AclOperation_t*)op, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : PyLong_FromLong(value);
        if (code == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, code);
    }
    return out;
}

// (partition, leader, [replicas], [isr], [elr] | None, [last_known_elr] | None)
static PyObject* topic_partition_info_to_py(const kafka_common_TopicPartitionInfo_t* info) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t partition = 0;
    const kafka_common_Node_t* leader_node = NULL;
    kafka_List_t *replica_nodes = NULL, *isr_nodes = NULL, *elr_nodes = NULL, *last_known_elr_nodes = NULL;
    PyObject *leader = NULL, *replicas = NULL, *isr = NULL, *elr = NULL, *last_known_elr = NULL;
    // Each owned list is handed to node_list_to_py, which frees it, as soon as
    // it is read, so a later failure leaves nothing to release on the C side.
    if (ffi_raise((partition = kafka_common_TopicPartitionInfo_partition_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((leader_node = kafka_common_TopicPartitionInfo_leader_err(info, &ffi_err), ffi_take(&ffi_err)))
        || (leader = node_to_py(leader_node)) == NULL
        || ffi_raise((replica_nodes = kafka_common_TopicPartitionInfo_replicas_err(info, &ffi_err), ffi_take(&ffi_err)))
        || (replicas = node_list_to_py(replica_nodes)) == NULL
        || ffi_raise((isr_nodes = kafka_common_TopicPartitionInfo_isr_err(info, &ffi_err), ffi_take(&ffi_err)))
        || (isr = node_list_to_py(isr_nodes)) == NULL
        || ffi_raise((elr_nodes = kafka_common_TopicPartitionInfo_elr_err(info, &ffi_err), ffi_take(&ffi_err)))
        || (elr = node_list_to_py(elr_nodes)) == NULL
        || ffi_raise((last_known_elr_nodes = kafka_common_TopicPartitionInfo_last_known_elr_err(info, &ffi_err), ffi_take(&ffi_err)))
        || (last_known_elr = node_list_to_py(last_known_elr_nodes)) == NULL) {
        Py_XDECREF(leader); Py_XDECREF(replicas); Py_XDECREF(isr); Py_XDECREF(elr); Py_XDECREF(last_known_elr);
        return NULL;
    }
    // Java's replicas/isr are never null; the FFI gives an empty list, which
    // node_list_to_py would also map to [] (only NULL becomes None).
    return Py_BuildValue("(iNNNNN)", (int)partition, leader, replicas, isr, elr, last_known_elr);
}

// (name, topic_id, is_internal, [partition_info], [acl_op] | None)
static PyObject* topic_description_to_py(const kafka_admin_TopicDescription_t* d) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* parts = NULL;  // owned
    if (ffi_raise((parts = kafka_admin_TopicDescription_partitions_err(d, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    int32_t n = 0;
    PyObject* partitions = parts && ffi_raise((n = kafka_List_size_err(parts, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n < 0 ? 0 : n);
    if (partitions == NULL) { if (parts) ffi_unraisable((kafka_List_destroy_err(parts, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* info = NULL;
        PyObject* p = ffi_raise((info = kafka_List_get_err(parts, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : topic_partition_info_to_py((const kafka_common_TopicPartitionInfo_t*)info);
        if (p == NULL) { Py_DECREF(partitions); ffi_unraisable((kafka_List_destroy_err(parts, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(partitions, i, p);
    }
    if (parts) ffi_unraisable((kafka_List_destroy_err(parts, &ffi_err), ffi_take(&ffi_err)));
    kafka_common_Uuid_t* id = NULL;
    kafka_List_t* operations = NULL;
    const char* name = NULL;
    int8_t is_internal = 0;
    PyObject* topic_id = NULL;
    PyObject* ops = NULL;
    if (ffi_raise((id = kafka_admin_TopicDescription_topic_id_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (topic_id = owned_uuid_to_py(id)) == NULL
        || ffi_raise((operations = kafka_admin_TopicDescription_authorized_operations_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (ops = acl_operations_to_py(operations)) == NULL
        || ffi_raise((name = kafka_admin_TopicDescription_name_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_internal = kafka_admin_TopicDescription_is_internal_err(d, &ffi_err), ffi_take(&ffi_err)))) {
        Py_XDECREF(topic_id); Py_XDECREF(ops); Py_DECREF(partitions); return NULL;
    }
    return Py_BuildValue("(sNiNN)", name ? name : "", topic_id, is_internal ? 1 : 0, partitions, ops);
}

// (name, topic_id, is_internal)
static PyObject* topic_listing_to_py(const kafka_admin_TopicListing_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_common_Uuid_t* id = NULL;
    if (ffi_raise((id = kafka_admin_TopicListing_topic_id_err(l, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* topic_id = owned_uuid_to_py(id);
    if (topic_id == NULL) return NULL;
    const char* name = NULL;
    int8_t is_internal = 0;
    if (ffi_raise((name = kafka_admin_TopicListing_name_err(l, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_internal = kafka_admin_TopicListing_is_internal_err(l, &ffi_err), ffi_take(&ffi_err)))) {
        Py_DECREF(topic_id);
        return NULL;
    }
    return Py_BuildValue("(sNi)", name ? name : "", topic_id, is_internal ? 1 : 0);
}

// The 5-tuple `_to_config_entry` expects (createTopics' metadata).
static PyObject* config_entry_short_to_py(const kafka_admin_ConfigEntry_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    const char* name = NULL;
    const char* value = NULL;
    int8_t is_default = 0, is_sensitive = 0, is_read_only = 0;
    if (ffi_raise((name = kafka_admin_ConfigEntry_name_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((value = kafka_admin_ConfigEntry_value_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_default = kafka_admin_ConfigEntry_is_default_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_sensitive = kafka_admin_ConfigEntry_is_sensitive_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_read_only = kafka_admin_ConfigEntry_is_read_only_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return Py_BuildValue("(sziii)", name ? name : "", value, is_default ? 1 : 0, is_sensitive ? 1 : 0,
                         is_read_only ? 1 : 0);
}

// The 9-tuple `_to_full_config_entry` expects: (name, value, is_default,
// is_sensitive, is_read_only, source_name, type_name, documentation | None,
// [(name, value, source_name)]).
static PyObject* config_entry_to_py(const kafka_admin_ConfigEntry_t* e) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* syns = NULL;  // owned
    if (ffi_raise((syns = kafka_admin_ConfigEntry_synonyms_err(e, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    int32_t n = 0;
    PyObject* synonyms = syns && ffi_raise((n = kafka_List_size_err(syns, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n < 0 ? 0 : n);
    if (synonyms == NULL) { if (syns) ffi_unraisable((kafka_List_destroy_err(syns, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* s = NULL;
        const kafka_admin_ConfigEntry_ConfigSynonym_t* synonym;
        const char* sname = NULL;
        const char* svalue = NULL;
        const kafka_admin_ConfigEntry_ConfigSource_t* ssource = NULL;
        const char* ssource_name = NULL;
        PyObject* row = NULL;
        if (!ffi_raise((s = kafka_List_get_err(syns, i, &ffi_err), ffi_take(&ffi_err)))
            && (synonym = (const kafka_admin_ConfigEntry_ConfigSynonym_t*)s, 1)
            && !ffi_raise((sname = kafka_admin_ConfigEntry_ConfigSynonym_name_err(synonym, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((svalue = kafka_admin_ConfigEntry_ConfigSynonym_value_err(synonym, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((ssource = kafka_admin_ConfigEntry_ConfigSynonym_source_err(synonym, &ffi_err), ffi_take(&ffi_err)))
            && (ssource_name = config_source_name(ssource)) != NULL) {
            row = Py_BuildValue("(szs)", sname ? sname : "", svalue, ssource_name);
        }
        if (row == NULL) { Py_DECREF(synonyms); ffi_unraisable((kafka_List_destroy_err(syns, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(synonyms, i, row);
    }
    if (syns) ffi_unraisable((kafka_List_destroy_err(syns, &ffi_err), ffi_take(&ffi_err)));
    const char* name = NULL;
    const char* value = NULL;
    const char* documentation = NULL;
    int8_t is_default = 0, is_sensitive = 0, is_read_only = 0;
    const kafka_admin_ConfigEntry_ConfigSource_t* source = NULL;
    const kafka_admin_ConfigEntry_ConfigType_t* type = NULL;
    const char* source_name = NULL;
    const char* type_name = NULL;
    if (ffi_raise((name = kafka_admin_ConfigEntry_name_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((value = kafka_admin_ConfigEntry_value_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_default = kafka_admin_ConfigEntry_is_default_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_sensitive = kafka_admin_ConfigEntry_is_sensitive_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_read_only = kafka_admin_ConfigEntry_is_read_only_err(e, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((source = kafka_admin_ConfigEntry_source_err(e, &ffi_err), ffi_take(&ffi_err)))
        || (source_name = config_source_name(source)) == NULL
        || ffi_raise((type = kafka_admin_ConfigEntry_type_err(e, &ffi_err), ffi_take(&ffi_err)))
        || (type_name = config_type_name(type)) == NULL
        || ffi_raise((documentation = kafka_admin_ConfigEntry_documentation_err(e, &ffi_err), ffi_take(&ffi_err)))) {
        Py_DECREF(synonyms);
        return NULL;
    }
    return Py_BuildValue("(sziiisszN)", name ? name : "", value, is_default ? 1 : 0, is_sensitive ? 1 : 0,
                         is_read_only ? 1 : 0, source_name, type_name, documentation, synonyms);
}

// Config_t -> [entry]; `full` picks the 9-tuple over the 5-tuple.
static PyObject* config_to_py(const kafka_admin_Config_t* c, int full) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* entries = NULL;  // owned
    if (ffi_raise((entries = kafka_admin_Config_entries_err(c, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    int32_t n = 0;
    PyObject* out = entries && ffi_raise((n = kafka_List_size_err(entries, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { if (entries) ffi_unraisable((kafka_List_destroy_err(entries, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* item = NULL;
        PyObject* row = NULL;
        if (!ffi_raise((item = kafka_List_get_err(entries, i, &ffi_err), ffi_take(&ffi_err)))) {
            const kafka_admin_ConfigEntry_t* e = (const kafka_admin_ConfigEntry_t*)item;
            row = full ? config_entry_to_py(e) : config_entry_short_to_py(e);
        }
        if (row == NULL) { Py_DECREF(out); ffi_unraisable((kafka_List_destroy_err(entries, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    if (entries) ffi_unraisable((kafka_List_destroy_err(entries, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// (error | None, total_bytes, usable_bytes, [(topic, partition, size, offset_lag, is_future)])
// -1 bytes are Java's empty OptionalLong; admin.py maps them to None.
static PyObject* log_dir_description_to_py(const kafka_admin_LogDirDescription_t* d) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_Map_t* infos = NULL;  // owned TP* -> ReplicaInfo*
    if (ffi_raise((infos = kafka_admin_LogDirDescription_replica_infos_err(d, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    int32_t n = 0;
    PyObject* replicas = infos && ffi_raise((n = kafka_Map_size_err(infos, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n < 0 ? 0 : n);
    if (replicas == NULL) { if (infos) ffi_unraisable((kafka_Map_destroy_err(infos, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* key = NULL;
        void* value = NULL;
        const char* topic = NULL;
        int32_t partition = 0;
        int64_t size = 0, offset_lag = 0;
        int8_t is_future = 0;
        PyObject* row = NULL;
        if (!ffi_raise((key = kafka_Map_key_err(infos, i, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((value = kafka_Map_value_err(infos, i, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((topic = kafka_common_TopicPartition_topic_err((const kafka_common_TopicPartition_t*)key, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((partition = kafka_common_TopicPartition_partition_err((const kafka_common_TopicPartition_t*)key, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((size = kafka_admin_ReplicaInfo_size_err((const kafka_admin_ReplicaInfo_t*)value, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((offset_lag = kafka_admin_ReplicaInfo_offset_lag_err((const kafka_admin_ReplicaInfo_t*)value, &ffi_err), ffi_take(&ffi_err)))
            && !ffi_raise((is_future = kafka_admin_ReplicaInfo_is_future_err((const kafka_admin_ReplicaInfo_t*)value, &ffi_err), ffi_take(&ffi_err)))) {
            row = Py_BuildValue("(siLLi)", topic ? topic : "", (int)partition, (long long)size,
                                (long long)offset_lag, is_future ? 1 : 0);
        }
        if (row == NULL) { Py_DECREF(replicas); ffi_unraisable((kafka_Map_destroy_err(infos, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(replicas, i, row);
    }
    if (infos) ffi_unraisable((kafka_Map_destroy_err(infos, &ffi_err), ffi_take(&ffi_err)));
    const kafka_common_Error_t* error = NULL;
    int64_t total_bytes = 0, usable_bytes = 0;
    PyObject* err = NULL;
    if (ffi_raise((error = kafka_admin_LogDirDescription_error_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (err = borrowed_error_to_py(error)) == NULL
        || ffi_raise((total_bytes = kafka_admin_LogDirDescription_total_bytes_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((usable_bytes = kafka_admin_LogDirDescription_usable_bytes_err(d, &ffi_err), ffi_take(&ffi_err)))) {
        Py_XDECREF(err);
        Py_DECREF(replicas);
        return NULL;
    }
    return Py_BuildValue("(NLLN)", err, (long long)total_bytes, (long long)usable_bytes, replicas);
}

// MemberAssignment -> [(topic, partition)]; NULL (Java's empty Optional) -> None.
static PyObject* member_assignment_to_py(const kafka_admin_MemberAssignment_t* a) {
    kafka_common_Error_t *ffi_err = NULL;
    if (a == NULL) Py_RETURN_NONE;
    kafka_List_t* partitions = NULL;
    if (ffi_raise((partitions = kafka_admin_MemberAssignment_topic_partitions_err(a, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return tp_list_to_py(partitions);
}

// (consumer_id, group_instance_id | None, rack_id | None, client_id, host,
//  assignment, target_assignment | None, member_epoch | None, upgraded | None)
static PyObject* member_description_to_py(const kafka_admin_MemberDescription_t* m) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_MemberAssignment_t* assigned = NULL;
    const kafka_admin_MemberAssignment_t* targeted = NULL;
    PyObject* assignment = NULL;
    PyObject* target = NULL;
    if (ffi_raise((assigned = kafka_admin_MemberDescription_assignment_err(m, &ffi_err), ffi_take(&ffi_err)))
        || (assignment = member_assignment_to_py(assigned)) == NULL
        || ffi_raise((targeted = kafka_admin_MemberDescription_target_assignment_err(m, &ffi_err), ffi_take(&ffi_err)))
        || (target = member_assignment_to_py(targeted)) == NULL) {
        Py_XDECREF(assignment); Py_XDECREF(target); return NULL;
    }
    int32_t epoch = 0;
    int8_t upgraded = 0;
    const char *consumer_id = NULL, *group_instance_id = NULL, *rack_id = NULL, *client_id = NULL, *host = NULL;
    if (ffi_raise((epoch = kafka_admin_MemberDescription_member_epoch_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((upgraded = kafka_admin_MemberDescription_upgraded_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((consumer_id = kafka_admin_MemberDescription_consumer_id_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((group_instance_id = kafka_admin_MemberDescription_group_instance_id_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((rack_id = kafka_admin_MemberDescription_rack_id_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((client_id = kafka_admin_MemberDescription_client_id_err(m, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((host = kafka_admin_MemberDescription_host_err(m, &ffi_err), ffi_take(&ffi_err)))) {
        Py_DECREF(assignment); Py_DECREF(target); return NULL;
    }
    PyObject* py_epoch = epoch < 0 ? (Py_INCREF(Py_None), Py_None) : PyLong_FromLong(epoch);
    PyObject* py_upgraded = upgraded < 0 ? (Py_INCREF(Py_None), Py_None) : PyBool_FromLong(upgraded);
    if (py_epoch == NULL || py_upgraded == NULL) {
        Py_XDECREF(py_epoch); Py_XDECREF(py_upgraded); Py_DECREF(assignment); Py_DECREF(target); return NULL;
    }
    return Py_BuildValue("(szzssNNNN)", consumer_id ? consumer_id : "", group_instance_id, rack_id,
                         client_id ? client_id : "", host ? host : "",
                         assignment, target, py_epoch, py_upgraded);
}

// OWNED list of MemberDescription* -> [member]; frees it.
static PyObject* member_list_to_py(kafka_List_t* members) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t n = 0;
    PyObject* out = members && ffi_raise((n = kafka_List_size_err(members, &ffi_err), ffi_take(&ffi_err))) ? NULL : PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { if (members) ffi_unraisable((kafka_List_destroy_err(members, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* member = NULL;
        PyObject* m = ffi_raise((member = kafka_List_get_err(members, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : member_description_to_py((const kafka_admin_MemberDescription_t*)member);
        if (m == NULL) { Py_DECREF(out); ffi_unraisable((kafka_List_destroy_err(members, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(out, i, m);
    }
    if (members) ffi_unraisable((kafka_List_destroy_err(members, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

static PyObject* optional_i32_to_py(int32_t v) {
    if (v < 0) Py_RETURN_NONE;
    return PyLong_FromLong(v);
}

static PyObject* optional_i64_to_py(int64_t v) {
    if (v < 0) Py_RETURN_NONE;
    return PyLong_FromLongLong((long long)v);
}

// (group_id, is_simple, [member], partition_assignor, group_type_name,
//  group_state_name, coordinator, [acl_op] | None, group_epoch | None,
//  target_assignment_epoch | None)
static PyObject* consumer_group_description_to_py(const kafka_admin_ConsumerGroupDescription_t* d) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* member_list = NULL;
    const kafka_common_GroupType_t* type = NULL;
    const kafka_common_GroupState_t* state = NULL;
    const kafka_common_Node_t* coordinator_node = NULL;
    kafka_List_t* operations = NULL;
    int32_t epoch = 0, target_assignment_epoch = 0;
    PyObject *members = NULL, *group_type = NULL, *group_state = NULL, *coordinator = NULL, *ops = NULL,
             *group_epoch = NULL, *target_epoch = NULL;
    const char* group_id = NULL;
    const char* assignor = NULL;
    int8_t is_simple = 0;
    // Each owned list goes to its converter, which frees it, as soon as it is read.
    if (ffi_raise((member_list = kafka_admin_ConsumerGroupDescription_members_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (members = member_list_to_py(member_list)) == NULL
        || ffi_raise((type = kafka_admin_ConsumerGroupDescription_type_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (group_type = group_type_to_py(type)) == NULL
        || ffi_raise((state = kafka_admin_ConsumerGroupDescription_group_state_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (group_state = group_state_to_py(state)) == NULL
        || ffi_raise((coordinator_node = kafka_admin_ConsumerGroupDescription_coordinator_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (coordinator = node_to_py(coordinator_node)) == NULL
        || ffi_raise((operations = kafka_admin_ConsumerGroupDescription_authorized_operations_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (ops = acl_operations_to_py(operations)) == NULL
        || ffi_raise((epoch = kafka_admin_ConsumerGroupDescription_group_epoch_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (group_epoch = optional_i32_to_py(epoch)) == NULL
        || ffi_raise((target_assignment_epoch = kafka_admin_ConsumerGroupDescription_target_assignment_epoch_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (target_epoch = optional_i32_to_py(target_assignment_epoch)) == NULL
        || ffi_raise((group_id = kafka_admin_ConsumerGroupDescription_group_id_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((assignor = kafka_admin_ConsumerGroupDescription_partition_assignor_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_simple = kafka_admin_ConsumerGroupDescription_is_simple_consumer_group_err(d, &ffi_err), ffi_take(&ffi_err)))) {
        Py_XDECREF(members); Py_XDECREF(group_type); Py_XDECREF(group_state); Py_XDECREF(coordinator);
        Py_XDECREF(ops); Py_XDECREF(group_epoch); Py_XDECREF(target_epoch);
        return NULL;
    }
    return Py_BuildValue("(sONsNNNNNN)", group_id ? group_id : "", is_simple ? Py_True : Py_False,
                         members, assignor ? assignor : "", group_type, group_state, coordinator, ops,
                         group_epoch, target_epoch);
}

// (group_id, protocol, protocol_data, is_simple, [member], state_name,
//  coordinator, [acl_op] | None)
static PyObject* classic_group_description_to_py(const kafka_admin_ClassicGroupDescription_t* d) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* member_list = NULL;
    const kafka_common_ClassicGroupState_t* group_state = NULL;
    const kafka_common_Node_t* coordinator_node = NULL;
    kafka_List_t* operations = NULL;
    PyObject *members = NULL, *state = NULL, *coordinator = NULL, *ops = NULL;
    const char *group_id = NULL, *protocol = NULL, *protocol_data = NULL;
    int8_t is_simple = 0;
    if (ffi_raise((member_list = kafka_admin_ClassicGroupDescription_members_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (members = member_list_to_py(member_list)) == NULL
        || ffi_raise((group_state = kafka_admin_ClassicGroupDescription_state_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (state = classic_group_state_to_py(group_state)) == NULL
        || ffi_raise((coordinator_node = kafka_admin_ClassicGroupDescription_coordinator_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (coordinator = node_to_py(coordinator_node)) == NULL
        || ffi_raise((operations = kafka_admin_ClassicGroupDescription_authorized_operations_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (ops = acl_operations_to_py(operations)) == NULL
        || ffi_raise((group_id = kafka_admin_ClassicGroupDescription_group_id_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((protocol = kafka_admin_ClassicGroupDescription_protocol_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((protocol_data = kafka_admin_ClassicGroupDescription_protocol_data_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_simple = kafka_admin_ClassicGroupDescription_is_simple_consumer_group_err(d, &ffi_err), ffi_take(&ffi_err)))) {
        Py_XDECREF(members); Py_XDECREF(state); Py_XDECREF(coordinator); Py_XDECREF(ops);
        return NULL;
    }
    return Py_BuildValue("(sssONNNN)", group_id ? group_id : "", protocol ? protocol : "",
                         protocol_data ? protocol_data : "", is_simple ? Py_True : Py_False,
                         members, state, coordinator, ops);
}

// (group_id, group_type_name | None, protocol, group_state_name | None, is_simple)
static PyObject* group_listing_to_py(const kafka_admin_GroupListing_t* g) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_common_GroupType_t* group_type = NULL;
    const kafka_common_GroupState_t* group_state = NULL;
    PyObject* type = NULL;
    PyObject* state = NULL;
    const char* group_id = NULL;
    const char* protocol = NULL;
    int8_t is_simple = 0;
    if (ffi_raise((group_type = kafka_admin_GroupListing_type_err(g, &ffi_err), ffi_take(&ffi_err)))
        || (type = group_type_to_py(group_type)) == NULL
        || ffi_raise((group_state = kafka_admin_GroupListing_group_state_err(g, &ffi_err), ffi_take(&ffi_err)))
        || (state = group_state_to_py(group_state)) == NULL
        || ffi_raise((group_id = kafka_admin_GroupListing_group_id_err(g, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((protocol = kafka_admin_GroupListing_protocol_err(g, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((is_simple = kafka_admin_GroupListing_is_simple_consumer_group_err(g, &ffi_err), ffi_take(&ffi_err)))) {
        Py_XDECREF(type); Py_XDECREF(state); return NULL;
    }
    return Py_BuildValue("(sNsNO)", group_id ? group_id : "", type, protocol ? protocol : "", state,
                         is_simple ? Py_True : Py_False);
}

// (offset, metadata, leader_epoch | None); NULL (Java null) -> None.
static PyObject* offset_and_metadata_to_py(const kafka_consumer_OffsetAndMetadata_t* om) {
    kafka_common_Error_t *ffi_err = NULL;
    if (om == NULL) Py_RETURN_NONE;
    int32_t leader_epoch = 0;
    const char* metadata = NULL;
    int64_t offset = 0;
    if (ffi_raise((leader_epoch = kafka_consumer_OffsetAndMetadata_leader_epoch_err(om, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((metadata = kafka_consumer_OffsetAndMetadata_metadata_err(om, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((offset = kafka_consumer_OffsetAndMetadata_offset_err(om, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* epoch = optional_i32_to_py(leader_epoch);
    if (epoch == NULL) return NULL;
    return Py_BuildValue("(LsN)", (long long)offset, metadata ? metadata : "", epoch);
}

// (token_id, owner, requester, [renewer], issue_ts, expiry_ts, max_ts, hmac, hmac_base64)
static PyObject* delegation_token_to_py(const kafka_common_security_token_delegation_DelegationToken_t* t) {
    kafka_common_Error_t *ffi_err = NULL;
    if (t == NULL) Py_RETURN_NONE;
    const kafka_common_security_token_delegation_TokenInformation_t* info = NULL;
    kafka_List_t* renewer_list = NULL;  // owned
    if (ffi_raise((info = kafka_common_security_token_delegation_DelegationToken_token_info_err(t, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((renewer_list = kafka_common_security_token_delegation_TokenInformation_renewers_err(info, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    int32_t n = 0;
    PyObject* renewers = renewer_list && ffi_raise((n = kafka_List_size_err(renewer_list, &ffi_err), ffi_take(&ffi_err)))
        ? NULL : PyList_New(n < 0 ? 0 : n);
    if (renewers == NULL) { if (renewer_list) ffi_unraisable((kafka_List_destroy_err(renewer_list, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* principal = NULL;
        PyObject* p = ffi_raise((principal = kafka_List_get_err(renewer_list, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : kafka_principal_to_py((const kafka_common_security_auth_KafkaPrincipal_t*)principal);
        if (p == NULL) { Py_DECREF(renewers); ffi_unraisable((kafka_List_destroy_err(renewer_list, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        PyList_SET_ITEM(renewers, i, p);
    }
    if (renewer_list) ffi_unraisable((kafka_List_destroy_err(renewer_list, &ffi_err), ffi_take(&ffi_err)));
    const kafka_common_security_auth_KafkaPrincipal_t* owner_principal = NULL;
    const kafka_common_security_auth_KafkaPrincipal_t* requester_principal = NULL;
    kafka_Bytes_t hmac = {0};  // borrowed bytes
    char* b64 = NULL;          // owned
    const char* token_id = NULL;
    int64_t issue = 0, expiry = 0, max = 0;
    PyObject *owner = NULL, *requester = NULL, *py_hmac = NULL, *py_b64 = NULL;
    if (ffi_raise((owner_principal = kafka_common_security_token_delegation_TokenInformation_owner_err(info, &ffi_err), ffi_take(&ffi_err)))
        || (owner = kafka_principal_to_py(owner_principal)) == NULL
        || ffi_raise((requester_principal = kafka_common_security_token_delegation_TokenInformation_token_requester_err(
               info, &ffi_err), ffi_take(&ffi_err)))
        || (requester = kafka_principal_to_py(requester_principal)) == NULL
        || ffi_raise((hmac = kafka_common_security_token_delegation_DelegationToken_hmac_err(t, &ffi_err), ffi_take(&ffi_err)))
        || (py_hmac = PyBytes_FromStringAndSize((const char*)hmac.data,
                                                hmac.data ? (Py_ssize_t)hmac.len : 0)) == NULL
        || ffi_raise((b64 = kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string_err(t, &ffi_err), ffi_take(&ffi_err)))
        || (py_b64 = PyUnicode_FromString(b64 ? b64 : "")) == NULL
        || ffi_raise((token_id = kafka_common_security_token_delegation_TokenInformation_token_id_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((issue = kafka_common_security_token_delegation_TokenInformation_issue_timestamp_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((expiry = kafka_common_security_token_delegation_TokenInformation_expiry_timestamp_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((max = kafka_common_security_token_delegation_TokenInformation_max_timestamp_err(info, &ffi_err), ffi_take(&ffi_err)))) {
        if (b64) ffi_unraisable((kafka_string_destroy_err(b64, &ffi_err), ffi_take(&ffi_err)));
        Py_XDECREF(owner); Py_XDECREF(requester); Py_XDECREF(py_hmac); Py_XDECREF(py_b64); Py_DECREF(renewers);
        return NULL;
    }
    if (b64) ffi_unraisable((kafka_string_destroy_err(b64, &ffi_err), ffi_take(&ffi_err)));
    return Py_BuildValue("(sNNNLLLNN)", token_id ? token_id : "", owner, requester, renewers,
                         (long long)issue, (long long)expiry, (long long)max, py_hmac, py_b64);
}

// (producer_id, producer_epoch, last_sequence, last_timestamp,
//  coordinator_epoch | None, current_transaction_start_offset | None)
static PyObject* producer_state_to_py(const kafka_admin_ProducerState_t* s) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t coordinator_epoch = 0;
    int64_t start_offset = 0, producer_id = 0, last_timestamp = 0;
    int32_t producer_epoch = 0;
    int32_t last_sequence = 0;
    if (ffi_raise((coordinator_epoch = kafka_admin_ProducerState_coordinator_epoch_err(s, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((start_offset = kafka_admin_ProducerState_current_transaction_start_offset_err(s, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((producer_id = kafka_admin_ProducerState_producer_id_err(s, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((producer_epoch = kafka_admin_ProducerState_producer_epoch_err(s, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((last_sequence = kafka_admin_ProducerState_last_sequence_err(s, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((last_timestamp = kafka_admin_ProducerState_last_timestamp_err(s, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* coord = optional_i32_to_py(coordinator_epoch);
    PyObject* start = optional_i64_to_py(start_offset);
    if (coord == NULL || start == NULL) { Py_XDECREF(coord); Py_XDECREF(start); return NULL; }
    return Py_BuildValue("(LiiLNN)", (long long)producer_id, (int)producer_epoch, (int)last_sequence,
                         (long long)last_timestamp, coord, start);
}

// (coordinator_id, state_name, producer_id, producer_epoch, timeout_ms,
//  start_time_ms | None, [(topic, partition)])
static PyObject* transaction_description_to_py(const kafka_admin_TransactionDescription_t* d) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_TransactionState_t* s = NULL;
    kafka_List_t* tps = NULL;  // owned, freed by tp_list_to_py
    int64_t start_ms = 0, producer_id = 0, timeout_ms = 0;
    int32_t coordinator_id = 0, producer_epoch = 0;
    PyObject *state = NULL, *start = NULL, *partitions = NULL;
    if (ffi_raise((s = kafka_admin_TransactionDescription_state_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (state = transaction_state_to_py(s)) == NULL
        || ffi_raise((start_ms = kafka_admin_TransactionDescription_transaction_start_time_ms_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (start = optional_i64_to_py(start_ms)) == NULL
        || ffi_raise((tps = kafka_admin_TransactionDescription_topic_partitions_err(d, &ffi_err), ffi_take(&ffi_err)))
        || (partitions = tp_list_to_py(tps)) == NULL
        || ffi_raise((coordinator_id = kafka_admin_TransactionDescription_coordinator_id_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((producer_id = kafka_admin_TransactionDescription_producer_id_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((producer_epoch = kafka_admin_TransactionDescription_producer_epoch_err(d, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((timeout_ms = kafka_admin_TransactionDescription_transaction_timeout_ms_err(d, &ffi_err), ffi_take(&ffi_err)))) {
        Py_XDECREF(state); Py_XDECREF(start); Py_XDECREF(partitions); return NULL;
    }
    return Py_BuildValue("(iNLiLNN)", (int)coordinator_id, state, (long long)producer_id, (int)producer_epoch,
                         (long long)timeout_ms, start, partitions);
}

// (transactional_id, producer_id, state_name)
static PyObject* transaction_listing_to_py(const kafka_admin_TransactionListing_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_TransactionState_t* s = NULL;
    const char* tid = NULL;
    int64_t producer_id = 0;
    PyObject* state = NULL;
    if (ffi_raise((s = kafka_admin_TransactionListing_state_err(l, &ffi_err), ffi_take(&ffi_err)))
        || (state = transaction_state_to_py(s)) == NULL
        || ffi_raise((tid = kafka_admin_TransactionListing_transactional_id_err(l, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((producer_id = kafka_admin_TransactionListing_producer_id_err(l, &ffi_err), ffi_take(&ffi_err)))) {
        Py_XDECREF(state); return NULL;
    }
    return Py_BuildValue("(sLN)", tid ? tid : "", (long long)producer_id, state);
}

// ---- borrowed-container helpers ------------------------------------------------------
//
// The element converters take the `void*` a kafka_List_t / kafka_Map_t hands
// out; each ITEM_TO_PY adapter casts it to the converter's own type, so no
// function is called through a pointer of another type.

typedef PyObject* (*item_to_py_fn)(const void* item);
typedef PyObject* (*entry_to_py_fn)(const void* key, const void* value);

#define ITEM_TO_PY(name, T, fn) \
    static PyObject* name(const void* item) { return fn((const T*)item); }

static PyObject* item_str(const void* item) { return PyUnicode_FromString(item ? (const char*)item : ""); }
ITEM_TO_PY(item_tp, kafka_common_TopicPartition_t, tp_to_py)
ITEM_TO_PY(item_error, kafka_common_Error_t, borrowed_error_to_py)
ITEM_TO_PY(item_config_resource, kafka_common_config_ConfigResource_t, config_resource_to_py)
ITEM_TO_PY(item_group_listing, kafka_admin_GroupListing_t, group_listing_to_py)
ITEM_TO_PY(item_acl_binding, kafka_common_acl_AclBinding_t, acl_binding_to_py)
ITEM_TO_PY(item_delegation_token, kafka_common_security_token_delegation_DelegationToken_t, delegation_token_to_py)
ITEM_TO_PY(item_transaction_listing, kafka_admin_TransactionListing_t, transaction_listing_to_py)
ITEM_TO_PY(item_topic_listing, kafka_admin_TopicListing_t, topic_listing_to_py)
ITEM_TO_PY(item_log_dir_description, kafka_admin_LogDirDescription_t, log_dir_description_to_py)
ITEM_TO_PY(item_offset_and_metadata, kafka_consumer_OffsetAndMetadata_t, offset_and_metadata_to_py)
ITEM_TO_PY(item_producer_state, kafka_admin_ProducerState_t, producer_state_to_py)
ITEM_TO_PY(item_quota_entity, kafka_common_quota_ClientQuotaEntity_t, client_quota_entity_to_py)

// BORROWED list -> [item_fn(item)]; NULL is empty.
static PyObject* borrowed_list_to_py(const kafka_List_t* list, item_to_py_fn item_fn) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t n = 0;
    if (list != NULL && ffi_raise((n = kafka_List_size_err(list, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    for (int32_t i = 0; out != NULL && i < n; i++) {
        void* item = NULL;
        PyObject* row = ffi_raise((item = kafka_List_get_err(list, i, &ffi_err), ffi_take(&ffi_err))) ? NULL : item_fn(item);
        if (row == NULL) { Py_CLEAR(out); break; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
}

// OWNED list -> [item_fn(item)]; frees the list.
static PyObject* owned_list_to_py(kafka_List_t* list, item_to_py_fn item_fn) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* out = borrowed_list_to_py(list, item_fn);
    if (list) ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// BORROWED map -> {key_fn(key): value_fn(value)}; NULL is empty.
static PyObject* borrowed_map_to_dict(const kafka_Map_t* map, item_to_py_fn key_fn, item_to_py_fn value_fn) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t n = 0;
    if (map != NULL && ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* d = PyDict_New();
    for (int32_t i = 0; d != NULL && i < n; i++) {
        void* k = NULL;
        void* v = NULL;
        PyObject *key = NULL, *value = NULL;
        if (ffi_raise((k = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) || (key = key_fn(k)) == NULL
            || ffi_raise((v = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err))) || (value = value_fn(v)) == NULL
            || PyDict_SetItem(d, key, value) < 0) Py_CLEAR(d);
        Py_XDECREF(key); Py_XDECREF(value);
    }
    return d;
}

// BORROWED map -> [entry_fn(key, value)] in map order; NULL is empty.
static PyObject* borrowed_map_to_list(const kafka_Map_t* map, entry_to_py_fn entry_fn) {
    kafka_common_Error_t *ffi_err = NULL;
    int32_t n = 0;
    if (map != NULL && ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    for (int32_t i = 0; out != NULL && i < n; i++) {
        void* k = NULL;
        void* v = NULL;
        PyObject* row = ffi_raise((k = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) || ffi_raise((v = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : entry_fn(k, v);
        if (row == NULL) { Py_CLEAR(out); break; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
}

// OWNED map -> [entry_fn(key, value)]; frees the map.
static PyObject* owned_map_to_list(kafka_Map_t* map, entry_to_py_fn entry_fn) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* out = borrowed_map_to_list(map, entry_fn);
    if (map) ffi_unraisable((kafka_Map_destroy_err(map, &ffi_err), ffi_take(&ffi_err)));
    return out;
}

// ---- slot converters ---------------------------------------------------------------
//
// `admin_conv_fn` implementations: the void* is the value a future delivered.

static PyObject* conv_str(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return optional_str_to_py((const char*)v);
}

static PyObject* conv_uuid(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return uuid_to_py((const kafka_common_Uuid_t*)v);
}

static PyObject* conv_i32_ptr(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) return PyLong_FromLong(-1);
    return PyLong_FromLong(*(const int32_t*)v);
}

static PyObject* conv_i16_ptr(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) return PyLong_FromLong(-1);
    return PyLong_FromLong(*(const int16_t*)v);
}

static PyObject* conv_i64_ptr(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) return PyLong_FromLong(-1);
    return PyLong_FromLongLong((long long)*(const int64_t*)v);
}

static PyObject* conv_node(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return node_to_py((const kafka_common_Node_t*)v);
}

static PyObject* conv_node_list(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    PyObject* out = node_list_to_py_borrowed((const kafka_List_t*)v);
    // Java's `nodes()` is never null; keep the list shape.
    if (out == Py_None) { Py_DECREF(out); return PyList_New(0); }
    return out;
}

static PyObject* conv_acl_operations(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return acl_operations_to_py_borrowed((const kafka_List_t*)v);
}

static PyObject* conv_str_list(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_list_to_py((const kafka_List_t*)v, item_str);
}

static PyObject* conv_config_short(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) return PyList_New(0);
    return config_to_py((const kafka_admin_Config_t*)v, 0);
}

static PyObject* conv_config_full(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) return PyList_New(0);
    return config_to_py((const kafka_admin_Config_t*)v, 1);
}

static PyObject* conv_topic_description(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) Py_RETURN_NONE;
    return topic_description_to_py((const kafka_admin_TopicDescription_t*)v);
}

// {name: (name, topic_id, is_internal)}
static PyObject* conv_topic_listings(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_map_to_dict((const kafka_Map_t*)v, item_str, item_topic_listing);
}

static PyObject* conv_deleted_records(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)job; (void)slot;
    if (v == NULL) return PyLong_FromLong(-1);
    int64_t low_watermark = 0;
    if (ffi_raise((low_watermark = kafka_admin_DeletedRecords_low_watermark_err((const kafka_admin_DeletedRecords_t*)v, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return PyLong_FromLongLong((long long)low_watermark);
}

// [(type_id, name)]
static PyObject* conv_config_resources(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_list_to_py((const kafka_List_t*)v, item_config_resource);
}

// {log_dir: (error, total_bytes, usable_bytes, [replica])}
static PyObject* conv_log_dir_descriptions(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_map_to_dict((const kafka_Map_t*)v, item_str, item_log_dir_description);
}

// (current_dir | None, current_lag, future_dir | None, future_lag)
static PyObject* conv_replica_log_dir_info(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)job; (void)slot;
    const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t* info =
        (const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t*)v;
    if (info == NULL) Py_RETURN_NONE;
    const char *current_dir = NULL, *future_dir = NULL;
    int64_t current_lag = 0, future_lag = 0;
    if (ffi_raise((current_dir = kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((current_lag = kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_offset_lag_err(
               info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((future_dir = kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((future_lag = kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_offset_lag_err(
               info, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return Py_BuildValue("(zLzL)", current_dir, (long long)current_lag, future_dir, (long long)future_lag);
}

// ([replicas], [adding], [removing]) of one PartitionReassignment.
static PyObject* partition_reassignment_to_py(const void* item) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_PartitionReassignment_t* r = (const kafka_admin_PartitionReassignment_t*)item;
    kafka_List_t *replicas_list = NULL, *adding_list = NULL, *removing_list = NULL;  // owned
    PyObject *replicas = NULL, *adding = NULL, *removing = NULL;
    if (ffi_raise((replicas_list = kafka_admin_PartitionReassignment_replicas_err(r, &ffi_err), ffi_take(&ffi_err)))
        || (replicas = int32_list_to_py(replicas_list)) == NULL
        || ffi_raise((adding_list = kafka_admin_PartitionReassignment_adding_replicas_err(r, &ffi_err), ffi_take(&ffi_err)))
        || (adding = int32_list_to_py(adding_list)) == NULL
        || ffi_raise((removing_list = kafka_admin_PartitionReassignment_removing_replicas_err(r, &ffi_err), ffi_take(&ffi_err)))
        || (removing = int32_list_to_py(removing_list)) == NULL) {
        Py_XDECREF(replicas); Py_XDECREF(adding); return NULL;
    }
    return Py_BuildValue("(NNN)", replicas, adding, removing);
}

// {(topic, partition): ([replicas], [adding], [removing])}
static PyObject* conv_partition_reassignments(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_map_to_dict((const kafka_Map_t*)v, item_tp, partition_reassignment_to_py);
}

// {(topic, partition): error4 | None}
static PyObject* conv_elect_leaders(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_map_to_dict((const kafka_Map_t*)v, item_tp, item_error);
}

// (offset, timestamp, leader_epoch | None)
static PyObject* conv_list_offsets_info(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)job; (void)slot;
    const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t* info =
        (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t*)v;
    if (info == NULL) Py_RETURN_NONE;
    int32_t leader_epoch = 0;
    int64_t offset = 0, timestamp = 0;
    if (ffi_raise((leader_epoch = kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((offset = kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((timestamp = kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp_err(info, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    PyObject* epoch = optional_i32_to_py(leader_epoch);
    if (epoch == NULL) return NULL;
    return Py_BuildValue("(LLN)", (long long)offset, (long long)timestamp, epoch);
}

// [(group_id, type, protocol, state, is_simple)]
static PyObject* conv_group_listings(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_list_to_py((const kafka_List_t*)v, item_group_listing);
}

// [error4]
static PyObject* conv_error_list(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_list_to_py((const kafka_List_t*)v, item_error);
}

static PyObject* conv_consumer_group_description(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) Py_RETURN_NONE;
    return consumer_group_description_to_py((const kafka_admin_ConsumerGroupDescription_t*)v);
}

static PyObject* conv_classic_group_description(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) Py_RETURN_NONE;
    return classic_group_description_to_py((const kafka_admin_ClassicGroupDescription_t*)v);
}

// {(topic, partition): (offset, metadata, leader_epoch | None) | None}
static PyObject* conv_group_offsets(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_map_to_dict((const kafka_Map_t*)v, item_tp, item_offset_and_metadata);
}

// [binding7]
static PyObject* conv_acl_bindings(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_list_to_py((const kafka_List_t*)v, item_acl_binding);
}

// (error4 | None, binding7 | None) of one FilterResult.
static PyObject* filter_result_to_py(const void* item) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_DeleteAclsResult_FilterResult_t* r = (const kafka_admin_DeleteAclsResult_FilterResult_t*)item;
    const kafka_common_Error_t* error = NULL;
    const kafka_common_acl_AclBinding_t* binding = NULL;
    if (ffi_raise((error = kafka_admin_DeleteAclsResult_FilterResult_error_err(r, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((binding = kafka_admin_DeleteAclsResult_FilterResult_binding_err(r, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return error_value_pair(borrowed_error_to_py(error), acl_binding_to_py(binding));
}

// FilterResults -> [(error4 | None, binding7 | None)]
static PyObject* conv_filter_results(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)job; (void)slot;
    const kafka_admin_DeleteAclsResult_FilterResults_t* results = (const kafka_admin_DeleteAclsResult_FilterResults_t*)v;
    if (results == NULL) return PyList_New(0);
    kafka_List_t* values = NULL;  // owned
    if (ffi_raise((values = kafka_admin_DeleteAclsResult_FilterResults_values_err(results, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return owned_list_to_py(values, filter_result_to_py);
}

// (quota_key, value) of one quota.
static PyObject* quota_row_to_py(const void* key, const void* value) {
    const double* qv = (const double*)value;
    return Py_BuildValue("(sd)", key ? (const char*)key : "", qv ? *qv : 0.0);
}

static PyObject* quota_rows_to_py(const void* item) {
    return borrowed_map_to_list((const kafka_Map_t*)item, quota_row_to_py);
}

// {entity_pairs: [(quota_key, value)]}
static PyObject* conv_quota_entities(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_map_to_dict((const kafka_Map_t*)v, item_quota_entity, quota_rows_to_py);
}

static PyObject* conv_delegation_token(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return delegation_token_to_py((const kafka_common_security_token_delegation_DelegationToken_t*)v);
}

static PyObject* conv_delegation_tokens(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_list_to_py((const kafka_List_t*)v, item_delegation_token);
}

// (mechanism_type, iterations) of one ScramCredentialInfo.
static PyObject* scram_credential_info_to_py(const void* item) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_ScramCredentialInfo_t* info = (const kafka_admin_ScramCredentialInfo_t*)item;
    const kafka_admin_ScramMechanism_t* mechanism = NULL;
    int8_t type = 0;
    int32_t iterations = 0;
    if (ffi_raise((mechanism = kafka_admin_ScramCredentialInfo_mechanism_err(info, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((type = kafka_admin_ScramMechanism_type_err(mechanism, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((iterations = kafka_admin_ScramCredentialInfo_iterations_err(info, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return Py_BuildValue("(ii)", (int)type, (int)iterations);
}

// UserScramCredentialsDescription -> [(mechanism_type, iterations)]
static PyObject* conv_scram_description(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)job; (void)slot;
    const kafka_admin_UserScramCredentialsDescription_t* d = (const kafka_admin_UserScramCredentialsDescription_t*)v;
    if (d == NULL) return PyList_New(0);
    kafka_List_t* infos = NULL;  // owned
    if (ffi_raise((infos = kafka_admin_UserScramCredentialsDescription_credential_infos_err(d, &ffi_err), ffi_take(&ffi_err)))) return NULL;
    return owned_list_to_py(infos, scram_credential_info_to_py);
}

// (feature, min, max) of one finalized feature.
static PyObject* finalized_feature_to_py(const void* key, const void* value) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_FinalizedVersionRange_t* r = (const kafka_admin_FinalizedVersionRange_t*)value;
    int16_t min = 0, max = 0;
    if (ffi_raise((min = kafka_admin_FinalizedVersionRange_min_version_level_err(r, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((max = kafka_admin_FinalizedVersionRange_max_version_level_err(r, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return Py_BuildValue("(sii)", key ? (const char*)key : "", (int)min, (int)max);
}

// (feature, min, max) of one supported feature.
static PyObject* supported_feature_to_py(const void* key, const void* value) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_admin_SupportedVersionRange_t* r = (const kafka_admin_SupportedVersionRange_t*)value;
    int16_t min = 0, max = 0;
    if (ffi_raise((min = kafka_admin_SupportedVersionRange_min_version_err(r, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((max = kafka_admin_SupportedVersionRange_max_version_err(r, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return Py_BuildValue("(sii)", key ? (const char*)key : "", (int)min, (int)max);
}

// ([(feature, min, max)], epoch | None, [(feature, min, max)])
static PyObject* conv_feature_metadata(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)job; (void)slot;
    const kafka_admin_FeatureMetadata_t* m = (const kafka_admin_FeatureMetadata_t*)v;
    if (m == NULL) Py_RETURN_NONE;
    kafka_Map_t *finalized = NULL, *supported = NULL;  // owned, freed by owned_map_to_list
    int64_t finalized_epoch = 0;
    PyObject *py_finalized = NULL, *py_supported = NULL, *epoch = NULL;
    if (ffi_raise((finalized = kafka_admin_FeatureMetadata_finalized_features_err(m, &ffi_err), ffi_take(&ffi_err)))
        || (py_finalized = owned_map_to_list(finalized, finalized_feature_to_py)) == NULL
        || ffi_raise((supported = kafka_admin_FeatureMetadata_supported_features_err(m, &ffi_err), ffi_take(&ffi_err)))
        || (py_supported = owned_map_to_list(supported, supported_feature_to_py)) == NULL
        || ffi_raise((finalized_epoch = kafka_admin_FeatureMetadata_finalized_features_epoch_err(m, &ffi_err), ffi_take(&ffi_err)))
        || (epoch = optional_i64_to_py(finalized_epoch)) == NULL) {
        Py_XDECREF(py_finalized); Py_XDECREF(py_supported); return NULL;
    }
    return Py_BuildValue("(NNN)", py_finalized, epoch, py_supported);
}

// PartitionProducerState -> [producer_state]
static PyObject* conv_partition_producer_state(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)job; (void)slot;
    const kafka_admin_DescribeProducersResult_PartitionProducerState_t* s =
        (const kafka_admin_DescribeProducersResult_PartitionProducerState_t*)v;
    if (s == NULL) return PyList_New(0);
    kafka_List_t* producers = NULL;  // owned
    if (ffi_raise((producers = kafka_admin_DescribeProducersResult_PartitionProducerState_active_producers_err(s, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    return owned_list_to_py(producers, item_producer_state);
}

static PyObject* conv_transaction_description(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) Py_RETURN_NONE;
    return transaction_description_to_py((const kafka_admin_TransactionDescription_t*)v);
}

// [(transactional_id, producer_id, state)]
static PyObject* conv_transaction_listings(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return borrowed_list_to_py((const kafka_List_t*)v, item_transaction_listing);
}

// ---- keyed-job helpers --------------------------------------------------------

// Converts the BORROWED key of a `*_values()` map into the Python key the
// `_to_*` converters expect.
typedef PyObject* (*admin_key_fn)(const void* key);

static PyObject* key_str(const void* k) { return PyUnicode_FromString(k ? (const char*)k : ""); }
static PyObject* key_i32(const void* k) { return PyLong_FromLong(*(const int32_t*)k); }
static PyObject* key_uuid(const void* k) { return uuid_to_py((const kafka_common_Uuid_t*)k); }
static PyObject* key_tp(const void* k) { return tp_to_py((const kafka_common_TopicPartition_t*)k); }
static PyObject* key_tpr(const void* k) { return tp_replica_to_py((const kafka_common_TopicPartitionReplica_t*)k); }
static PyObject* key_config_resource(const void* k) {
    return config_resource_to_py((const kafka_common_config_ConfigResource_t*)k);
}
static PyObject* key_acl_binding(const void* k) { return acl_binding_to_py((const kafka_common_acl_AclBinding_t*)k); }
static PyObject* key_acl_binding_filter(const void* k) {
    return acl_binding_filter_to_py((const kafka_common_acl_AclBindingFilter_t*)k);
}
static PyObject* key_quota_entity(const void* k) {
    return client_quota_entity_to_py((const kafka_common_quota_ClientQuotaEntity_t*)k);
}

// Adds one slot per entry of an OWNED `*_values()`-style map (owned key ->
// owned future) and registers the map for destruction at job end, which
// frees the futures: the slots therefore do not own them. 0 / -1.
static int job_add_map(AdminJob* job, kafka_Map_t* map, admin_key_fn key_fn, admin_conv_fn conv, int tag) {
    kafka_common_Error_t *ffi_err = NULL;
    if (job_own_map(job, map) < 0) return -1;
    if (map == NULL) return 0;
    int32_t n = 0;
    if (ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) return -1;
    for (int32_t i = 0; i < n; i++) {
        void* k = NULL;
        void* f = NULL;
        PyObject* key = ffi_raise((k = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) ? NULL : key_fn(k);
        if (key == NULL) return -1;
        if (ffi_raise((f = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(key); return -1; }
        if (job_add_slot(job, (kafka_common_KafkaFuture_t*)f, 0, key, conv, NULL, tag) == NULL) return -1;
    }
    return 0;
}

// Adds a slot for an owned (`owned` = 1) or borrowed (`owned` = 0) whole-call
// future with no key. 0 / -1.
static int job_add_single(AdminJob* job, kafka_common_KafkaFuture_t* f, int owned, admin_conv_fn conv, int tag) {
    return job_add_slot(job, f, owned, NULL, conv, NULL, tag) ? 0 : -1;
}

// The (job, 0) return of a successfully built entry point, or NULL with the
// exception the failing step set after freeing the job.
#define JOB_TRY(job, expr) do { if ((expr) < 0) { job_free(job); return NULL; } } while (0)
#define JOB_RESULT_DESTROY(Rpc) ((void (*)(void*, kafka_common_Error_t**))kafka_admin_##Rpc##Result_destroy_err)

// ---- owned-handle containers --------------------------------------------------
//
// A C-built kafka_List_t / kafka_Map_t holds BORROWED elements, so the typed
// handles built from Python rows are tracked beside the container and
// destroyed with it after the RPC (which copies what it keeps).

// A `_destroy_err` twin, or a local release with the same shape (NULL).
typedef void (*handle_destroy_fn)(void*, kafka_common_Error_t**);

// Frees `h` with `destroy`, a `_destroy_err` twin, returning the panic it
// caught (NULL if none).
static kafka_common_Error_t* handle_destroy(handle_destroy_fn destroy, void* h) {
    kafka_common_Error_t* e = NULL;
    destroy(h, &e);
    return e;
}

typedef struct {
    kafka_List_t* list;
    void** items;
    Py_ssize_t n, cap;
    handle_destroy_fn destroy;   // NULL: elements are borrowed
} obj_list_t;

static int obj_list_init(obj_list_t* l, Py_ssize_t cap, handle_destroy_fn destroy) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(l, 0, sizeof(*l));
    l->destroy = destroy;
    if (ffi_raise((l->list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) return -1;
    l->items = cap > 0 ? (void**)PyMem_Calloc((size_t)cap, sizeof(void*)) : NULL;
    if (cap > 0 && l->items == NULL) {
        ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err))); l->list = NULL; PyErr_NoMemory(); return -1;
    }
    l->cap = cap;
    return 0;
}

// Tracks `item` for obj_list_free whether or not adding it succeeds. 0 / -1.
static int obj_list_add(obj_list_t* l, void* item) {
    kafka_common_Error_t *ffi_err = NULL;
    l->items[l->n++] = item;
    return ffi_raise((kafka_List_add_err(l->list, item, &ffi_err), ffi_take(&ffi_err))) ? -1 : 0;
}

static void obj_list_free(obj_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->destroy && l->items[i]) ffi_unraisable(handle_destroy(l->destroy, l->items[i]));
    }
    PyMem_Free(l->items);
    if (l->list) ffi_unraisable((kafka_List_destroy_err(l->list, &ffi_err), ffi_take(&ffi_err)));
    memset(l, 0, sizeof(*l));
}

typedef struct {
    kafka_Map_t* map;
    void** keys;
    void** vals;
    Py_ssize_t n, cap;
    handle_destroy_fn key_destroy;   // NULL: keys are borrowed
    handle_destroy_fn val_destroy;   // NULL: values are borrowed
} obj_map_t;

static int obj_map_init(obj_map_t* m, Py_ssize_t cap, handle_destroy_fn key_destroy, handle_destroy_fn val_destroy) {
    kafka_common_Error_t *ffi_err = NULL;
    memset(m, 0, sizeof(*m));
    m->key_destroy = key_destroy;
    m->val_destroy = val_destroy;
    if (ffi_raise((m->map = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) return -1;
    m->keys = cap > 0 ? (void**)PyMem_Calloc((size_t)cap, sizeof(void*)) : NULL;
    m->vals = cap > 0 ? (void**)PyMem_Calloc((size_t)cap, sizeof(void*)) : NULL;
    if (cap > 0 && (m->keys == NULL || m->vals == NULL)) {
        PyMem_Free(m->keys); PyMem_Free(m->vals);
        ffi_unraisable((kafka_Map_destroy_err(m->map, &ffi_err), ffi_take(&ffi_err))); m->map = NULL; PyErr_NoMemory(); return -1;
    }
    m->cap = cap;
    return 0;
}

// `value` may be NULL where the RPC reads NULL as Java's null. Tracks both
// for obj_map_free whether or not the put succeeds. 0 / -1.
static int obj_map_put(obj_map_t* m, void* key, void* value) {
    kafka_common_Error_t *ffi_err = NULL;
    m->keys[m->n] = key;
    m->vals[m->n] = value;
    m->n++;
    return ffi_raise((kafka_Map_put_err(m->map, key, value, &ffi_err), ffi_take(&ffi_err))) ? -1 : 0;
}

static void obj_map_free(obj_map_t* m) {
    kafka_common_Error_t *ffi_err = NULL;
    for (Py_ssize_t i = 0; i < m->n; i++) {
        if (m->key_destroy && m->keys[i]) ffi_unraisable(handle_destroy(m->key_destroy, m->keys[i]));
        if (m->val_destroy && m->vals[i]) ffi_unraisable(handle_destroy(m->val_destroy, m->vals[i]));
    }
    PyMem_Free(m->keys); PyMem_Free(m->vals);
    if (m->map) ffi_unraisable((kafka_Map_destroy_err(m->map, &ffi_err), ffi_take(&ffi_err)));
    memset(m, 0, sizeof(*m));
}

// Boxes an int32 for the `const int32_t *` scalars the containers carry.
static int32_t* box_i32(int32_t v) {
    int32_t* p = (int32_t*)PyMem_Malloc(sizeof(*p));
    if (p == NULL) { PyErr_NoMemory(); return NULL; }
    *p = v;
    return p;
}

static void pymem_free(void* p, kafka_common_Error_t** out_err) { (void)out_err; PyMem_Free(p); }

// list[int] -> an owned kafka_List_t of boxed int32_t*, as the nested broker
// lists of NewTopic / NewPartitions / NewPartitionReassignment need. NULL
// with an exception on failure.
static kafka_List_t* i32_list_boxed(PyObject* seq, const char* what) {
    PyObject* fast = PySequence_Fast(seq, what);
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, pymem_free) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        long v = PyLong_AsLong(PySequence_Fast_GET_ITEM(fast, i));
        if (v == -1 && PyErr_Occurred()) { Py_DECREF(fast); obj_list_free(&l); return NULL; }
        int32_t* b = box_i32((int32_t)v);
        if (b == NULL || obj_list_add(&l, b) < 0) { Py_DECREF(fast); obj_list_free(&l); return NULL; }
    }
    Py_DECREF(fast);
    // Hand the list out; i32_list_boxed_free frees the boxed elements with it.
    kafka_List_t* out = l.list;
    PyMem_Free(l.items);
    return out;
}

// Frees a list built by i32_list_boxed (elements then container). A failed
// read of the list is reported and leaks the boxes it could not reach.
static void i32_list_boxed_free(void* l, kafka_common_Error_t** out_err) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* list = (kafka_List_t*)l;
    if (list == NULL) return;
    int32_t n = 0;
    kafka_common_Error_t* err = (n = kafka_List_size_err(list, &ffi_err), ffi_take(&ffi_err));
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* item = NULL;
        err = (item = kafka_List_get_err(list, i, &ffi_err), ffi_take(&ffi_err));
        if (err == NULL) PyMem_Free(item);
    }
    ffi_unraisable(err);
    kafka_List_destroy_err(list, out_err);
}

// Formats the "<what> at index <i>" prefixes the marshaling errors carry.
static kafka_common_Error_t* indexed_error(const char* what, Py_ssize_t index, kafka_common_Error_t* err) {
    char prefix[160];
    snprintf(prefix, sizeof(prefix), "%s at index %zd", what, index);
    return prefixed_error(prefix, err);
}

// ---- entry-point boilerplate ----------------------------------------------------
//
// Every `py_Admin_<rpc>(handle, inputs..., timeout_ms, flags...)` builds the
// borrowed C inputs, the options, calls the `_with_options` RPC, frees the
// inputs, and returns `(job, 0)` -- or `(0, error)` when a typed constructor
// rejected a row (the Python wrapper raises KafkaError) -- or NULL with a
// Python exception for a malformed argument.

// Declares the options `var` and `var##_err`, the error of the steps that
// build the options and call the RPC: `if (var) <destroy>` and
// `if (ffi_raise(var##_err)) return NULL` after the inputs are freed.
#define ADMIN_OPTIONS(Rpc, var, timeout_ms) \
    kafka_admin_##Rpc##Options_t* var = NULL; \
    kafka_common_Error_t* var##_err = NULL; \
    var = kafka_admin_##Rpc##Options_new_err(&var##_err); \
    ADMIN_SET_TIMEOUT(Rpc, var, timeout_ms, var##_err)

// ---- topics ------------------------------------------------------------------------

// Admin_create_topics(h, [(name, num_partitions, replication_factor,
//                          [(key, value)], [(partition, [broker])])],
//                     timeout_ms, validate_only, retry_on_quota_violation)
// -> (job, err). Payload: {name: (error, (topic_id, num_partitions,
//    replication_factor, [entry5], None))}; the metadata tuple's error
//    slot is always None here (a failed topic reports at the pair level).
enum { CT_VALUES = 0, CT_CONFIG, CT_TOPIC_ID, CT_NUM_PARTITIONS, CT_REPLICATION_FACTOR, CT_SLOTS };

static PyObject* assemble_create_topics(AdminJob* job) {
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    for (int i = 0; i + CT_SLOTS <= job->n; i += CT_SLOTS) {
        AdminSlot* values = job->slots[i + CT_VALUES];
        PyObject* pair;
        if (values->error) {
            pair = Py_BuildValue("(OO)", values->error, Py_None);
        } else {
            // The refinement futures fail only when the topic's own future
            // failed (Java's ensureSuccess), which the branch above covers;
            // a refinement that fails on its own reports at the pair level too.
            PyObject* err = Py_None;
            for (int k = 1; k < CT_SLOTS; k++) {
                if (job->slots[i + k]->error) { err = job->slots[i + k]->error; break; }
            }
            if (err != Py_None) {
                pair = Py_BuildValue("(OO)", err, Py_None);
            } else {
                pair = Py_BuildValue("(O(OOOOO))", Py_None,
                                     job->slots[i + CT_TOPIC_ID]->value,
                                     job->slots[i + CT_NUM_PARTITIONS]->value,
                                     job->slots[i + CT_REPLICATION_FACTOR]->value,
                                     job->slots[i + CT_CONFIG]->value, Py_None);
            }
        }
        if (pair == NULL || PyDict_SetItem(d, values->key, pair) < 0) { Py_XDECREF(pair); Py_DECREF(d); return NULL; }
        Py_DECREF(pair);
    }
    return d;
}

static PyObject* py_Admin_create_topics(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only, retry;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &spec, &timeout_ms, &validate_only, &retry)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "new_topics must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t topics;
    if (obj_list_init(&topics, n, (handle_destroy_fn)kafka_admin_NewTopic_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* name; int np, rf; PyObject *configs, *assignments;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siiOO", &name, &np, &rf, &configs, &assignments)) goto fail;
        kafka_admin_NewTopic_t* t = NULL;
        Py_ssize_t n_assign = PySequence_Size(assignments);
        if (n_assign < 0) goto fail;
        if (n_assign > 0) {
            // NewTopic(name, Map<Integer, List<Integer>>): borrowed int32_t* ->
            // kafka_List_t of int32_t*, copied by the constructor.
            PyObject* afast = PySequence_Fast(assignments, "replicas_assignments must be a sequence");
            if (afast == NULL) goto fail;
            obj_map_t m;
            if (obj_map_init(&m, n_assign, pymem_free, i32_list_boxed_free) < 0) { Py_DECREF(afast); goto fail; }
            int ok = 1;
            for (Py_ssize_t j = 0; j < n_assign && ok; j++) {
                int partition; PyObject* brokers;
                if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(afast, j), "iO", &partition, &brokers)) { ok = 0; break; }
                int32_t* key = box_i32((int32_t)partition);
                if (key == NULL) { ok = 0; break; }
                kafka_List_t* blist = i32_list_boxed(brokers, "broker ids must be a sequence of int");
                if (blist == NULL) { PyMem_Free(key); ok = 0; break; }
                if (obj_map_put(&m, key, blist) < 0) { ok = 0; break; }
            }
            Py_DECREF(afast);
            if (ok) ok = !ffi_raise((t = kafka_admin_NewTopic_with_replicas_assignments_err(name, m.map, &ffi_err), ffi_take(&ffi_err)));
            obj_map_free(&m);
            if (!ok) goto fail;
        } else if (ffi_raise((t = kafka_admin_NewTopic_with_num_partitions_replication_factor_err(
                       name, (int32_t)np, (int16_t)rf, &ffi_err), ffi_take(&ffi_err)))) {
            goto fail;
        }
        if (obj_list_add(&topics, t) < 0) goto fail;
        Py_ssize_t n_conf = PySequence_Size(configs);
        if (n_conf < 0) goto fail;
        if (n_conf > 0) {
            PyObject* cfast = PySequence_Fast(configs, "configs must be a sequence of (key, value)");
            if (cfast == NULL) goto fail;
            kafka_Map_t* cmap = NULL;   // borrowed char* -> char* (owned by cfast's str objects)
            if (ffi_raise((cmap = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(cfast); goto fail; }
            int ok = 1;
            for (Py_ssize_t j = 0; j < n_conf; j++) {
                const char *k, *v;
                if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(cfast, j), "ss", &k, &v)
                    || ffi_raise((kafka_Map_put_err(cmap, (void*)k, (void*)v, &ffi_err), ffi_take(&ffi_err)))) { ok = 0; break; }
            }
            if (ok) ok = !ffi_raise((kafka_admin_NewTopic_set_configs_err(t, cmap, &ffi_err), ffi_take(&ffi_err)));
            ffi_unraisable((kafka_Map_destroy_err(cmap, &ffi_err), ffi_take(&ffi_err)));
            Py_DECREF(cfast);
            if (!ok) goto fail;
        }
    }
    {
        ADMIN_OPTIONS(CreateTopics, o, timeout_ms);
        FFI_STEP(o_err, (kafka_admin_CreateTopicsOptions_set_validate_only_err(o, (int8_t)validate_only, &ffi_err), ffi_take(&ffi_err)));
        FFI_STEP(o_err, (kafka_admin_CreateTopicsOptions_set_retry_on_quota_violation_err(o, (int8_t)retry, &ffi_err), ffi_take(&ffi_err)));
        kafka_admin_CreateTopicsResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_create_topics_with_options_err(a->admin, topics.list, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_CreateTopicsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        obj_list_free(&topics);
        Py_DECREF(fast);
        if (ffi_raise(o_err)) return NULL;
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreateTopics), assemble_create_topics);
        if (job == NULL) { ffi_unraisable((kafka_admin_CreateTopicsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        // Five slots per topic, in CT_* order: the Void future from values()
        // and the four refinements (owned, client-less, so they fire inline).
        kafka_Map_t* values = NULL;
        JOB_TRY(job, ffi_raise((values = kafka_admin_CreateTopicsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_own_map(job, values));
        int32_t nv = 0;
        JOB_TRY(job, values && ffi_raise((nv = kafka_Map_size_err(values, &ffi_err), ffi_take(&ffi_err))) ? -1 : 0);
        for (int32_t i = 0; i < nv; i++) {
            void* k = NULL;
            void* f = NULL;
            JOB_TRY(job, ffi_raise((k = kafka_Map_key_err(values, i, &ffi_err), ffi_take(&ffi_err))) || ffi_raise((f = kafka_Map_value_err(values, i, &ffi_err), ffi_take(&ffi_err)))
                ? -1 : 0);
            const char* name = (const char*)k;
            PyObject* key = key_str(name);
            if (key == NULL) { job_free(job); return NULL; }
            if (job_add_slot(job, (kafka_common_KafkaFuture_t*)f, 0, key, NULL, NULL, CT_VALUES) == NULL) {
                job_free(job); return NULL;
            }
            { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_CreateTopicsResult_config_err(r, name, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_config_short, CT_CONFIG)); }
            { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_CreateTopicsResult_topic_id_err(r, name, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_uuid, CT_TOPIC_ID)); }
            { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_CreateTopicsResult_num_partitions_err(r, name, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_i32_ptr, CT_NUM_PARTITIONS)); }
            { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_CreateTopicsResult_replication_factor_err(r, name, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_i32_ptr, CT_REPLICATION_FACTOR)); }
        }
        return job_or_error(job, NULL);
    }
fail:
    obj_list_free(&topics);
    Py_DECREF(fast);
    return NULL;
}

// Builds the TopicCollection for the by-name / by-id topic RPCs. Returns 0
// with `*out` set, or -1 with either a Python exception or `*err_out` (a
// malformed topic id, reported as KafkaError "invalid topic id: ...").
static int topic_collection_build(PyObject* names, int by_ids, kafka_common_TopicCollection_t** out,
                                  kafka_common_Error_t** err_out) {
    kafka_common_Error_t *ffi_err = NULL;
    *out = NULL; *err_out = NULL;
    if (by_ids) {
        uuid_list_t ids;
        if (uuid_list_build(names, &ids, err_out) < 0) return -1;
        int failed = ffi_raise((*(out) = kafka_common_TopicCollection_of_topic_ids_err(ids.list, &ffi_err), ffi_take(&ffi_err)));
        uuid_list_free(&ids);
        if (failed) return -1;
    } else {
        str_list_t l;
        if (str_list_build(names, &l) < 0) return -1;
        int failed = ffi_raise((*(out) = kafka_common_TopicCollection_of_topic_names_err(l.list, &ffi_err), ffi_take(&ffi_err)));
        str_list_free(&l);
        if (failed) return -1;
    }
    return 0;
}

// Admin_delete_topics(h, [name|id], timeout_ms, retry_on_quota_violation, by_ids)
// -> (job, err). Payload: {name|id: error}.
static PyObject* py_Admin_delete_topics(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* names; long long timeout_ms; int retry, by_ids;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &names, &timeout_ms, &retry, &by_ids)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_TopicCollection_t* topics; kafka_common_Error_t* err;
    if (topic_collection_build(names, by_ids, &topics, &err) < 0) return err ? job_or_error(NULL, err) : NULL;
    ADMIN_OPTIONS(DeleteTopics, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_DeleteTopicsOptions_set_retry_on_quota_violation_err(o, (int8_t)retry, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DeleteTopicsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_delete_topics_with_options_err(a->admin, topics, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DeleteTopicsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_common_TopicCollection_destroy_err(topics, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteTopics), assemble_keyed_errors);
    if (job == NULL) { ffi_unraisable((kafka_admin_DeleteTopicsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    if (by_ids) { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DeleteTopicsResult_topic_id_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_uuid, NULL, 0)); }
    else { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DeleteTopicsResult_topic_name_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, NULL, 0)); }
    return job_or_error(job, NULL);
}

// Admin_list_topics(h, timeout_ms, list_internal) -> (job, err).
// Payload: (error, {name: (name, topic_id, is_internal)}).
static PyObject* py_Admin_list_topics(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long timeout_ms; int list_internal;
    if (!PyArg_ParseTuple(args, "KLp", &h, &timeout_ms, &list_internal)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(ListTopics, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_ListTopicsOptions_set_list_internal_err(o, (int8_t)list_internal, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_ListTopicsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_list_topics_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_ListTopicsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListTopics), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_ListTopicsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_ListTopicsResult_names_to_listings_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_topic_listings, 0)); }
    return job_or_error(job, NULL);
}

// Admin_describe_topics(h, [name|id], timeout_ms, include_authorized_operations,
//                       partition_size_limit (-1: unset), by_ids) -> (job, err).
// Payload: {name|id: (error, description5)}.
static PyObject* py_Admin_describe_topics(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* names; long long timeout_ms; int include_ops, limit, by_ids;
    if (!PyArg_ParseTuple(args, "KOLpip", &h, &names, &timeout_ms, &include_ops, &limit, &by_ids)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_TopicCollection_t* topics; kafka_common_Error_t* err;
    if (topic_collection_build(names, by_ids, &topics, &err) < 0) return err ? job_or_error(NULL, err) : NULL;
    ADMIN_OPTIONS(DescribeTopics, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_DescribeTopicsOptions_set_include_authorized_operations_err(o, (int8_t)include_ops, &ffi_err), ffi_take(&ffi_err)));
    if (limit >= 0) FFI_STEP(o_err, (kafka_admin_DescribeTopicsOptions_set_partition_size_limit_per_response_err(o, (int32_t)limit, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DescribeTopicsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_topics_with_topics_options_err(a->admin, topics, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeTopicsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_common_TopicCollection_destroy_err(topics, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeTopics), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeTopicsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    if (by_ids) { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeTopicsResult_topic_id_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_uuid, conv_topic_description, 0)); }
    else { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeTopicsResult_topic_name_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, conv_topic_description, 0)); }
    return job_or_error(job, NULL);
}

// Admin_create_partitions(h, [(topic, total_count, [[broker]] | None)],
//                         timeout_ms, validate_only, retry_on_quota_violation)
// -> (job, err). Payload: {topic: error}. A None assignment is
// increaseTo(int); a list -- even an empty one -- is increaseTo(int, List).
static PyObject* py_Admin_create_partitions(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only, retry;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &spec, &timeout_ms, &validate_only, &retry)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "new_partitions must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // borrowed char* (owned by fast) -> owned NewPartitions*
    if (obj_map_init(&m, n, NULL, (handle_destroy_fn)kafka_admin_NewPartitions_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* topic; int total; PyObject* assignments;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siO", &topic, &total, &assignments)) goto fail;
        kafka_admin_NewPartitions_t* np = NULL;
        if (assignments == Py_None) {
            if (ffi_raise((np = kafka_admin_NewPartitions_increase_to_err((int32_t)total, &ffi_err), ffi_take(&ffi_err)))) goto fail;
        } else {
            PyObject* afast = PySequence_Fast(assignments, "new_assignments must be a sequence of broker lists");
            if (afast == NULL) goto fail;
            Py_ssize_t na = PySequence_Fast_GET_SIZE(afast);
            obj_list_t outer;
            if (obj_list_init(&outer, na, i32_list_boxed_free) < 0) { Py_DECREF(afast); goto fail; }
            int ok = 1;
            for (Py_ssize_t j = 0; j < na; j++) {
                kafka_List_t* inner = i32_list_boxed(PySequence_Fast_GET_ITEM(afast, j), "broker ids must be a sequence of int");
                if (inner == NULL || obj_list_add(&outer, inner) < 0) { ok = 0; break; }
            }
            Py_DECREF(afast);
            if (ok) ok = !ffi_raise((np = kafka_admin_NewPartitions_increase_to_with_new_assignments_err(
                (int32_t)total, outer.list, &ffi_err), ffi_take(&ffi_err)));
            obj_list_free(&outer);
            if (!ok) goto fail;
        }
        if (obj_map_put(&m, (void*)topic, np) < 0) goto fail;
    }
    {
        ADMIN_OPTIONS(CreatePartitions, o, timeout_ms);
        FFI_STEP(o_err, (kafka_admin_CreatePartitionsOptions_set_validate_only_err(o, (int8_t)validate_only, &ffi_err), ffi_take(&ffi_err)));
        FFI_STEP(o_err, (kafka_admin_CreatePartitionsOptions_set_retry_on_quota_violation_err(o, (int8_t)retry, &ffi_err), ffi_take(&ffi_err)));
        kafka_admin_CreatePartitionsResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_create_partitions_with_options_err(a->admin, m.map, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_CreatePartitionsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        obj_map_free(&m);
        Py_DECREF(fast);
        if (ffi_raise(o_err)) return NULL;
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreatePartitions), assemble_keyed_errors);
        if (job == NULL) { ffi_unraisable((kafka_admin_CreatePartitionsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_CreatePartitionsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, NULL, 0)); }
        return job_or_error(job, NULL);
    }
fail:
    obj_map_free(&m);
    Py_DECREF(fast);
    return NULL;
}

// Admin_delete_records(h, [(topic, partition, before_offset)], timeout_ms)
// -> (job, err). Payload: {(topic, partition): (error, low_watermark)}.
static PyObject* py_Admin_delete_records(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "records_to_delete must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy_err,
                     (handle_destroy_fn)kafka_admin_RecordsToDelete_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* t; int p; long long before;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siL", &t, &p, &before)) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
        kafka_common_TopicPartition_t* tp = NULL;
        kafka_admin_RecordsToDelete_t* rd = NULL;
        if (ffi_raise((tp = kafka_common_TopicPartition_new_err(t, (int32_t)p, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((rd = kafka_admin_RecordsToDelete_with_offset_err((int64_t)before, &ffi_err), ffi_take(&ffi_err)))) {
            if (tp) ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err)));
            obj_map_free(&m); Py_DECREF(fast); return NULL;
        }
        if (obj_map_put(&m, tp, rd) < 0) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(DeleteRecords, o, timeout_ms);
    kafka_admin_DeleteRecordsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_delete_records_with_options_err(a->admin, m.map, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DeleteRecordsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    obj_map_free(&m);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteRecords), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DeleteRecordsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DeleteRecordsResult_low_watermarks_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_tp, conv_deleted_records, 0)); }
    return job_or_error(job, NULL);
}

// ---- cluster and configs --------------------------------------------------------------

// Admin_describe_cluster(h, timeout_ms, include_authorized_operations,
//                        include_fenced_brokers) -> (job, err).
// Payload: (error, (cluster_id, [node], controller, [op] | None)) -- Java's
// four independent futures; the first failure wins.
static PyObject* py_Admin_describe_cluster(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; long long timeout_ms; int include_ops, include_fenced;
    if (!PyArg_ParseTuple(args, "KLpp", &h, &timeout_ms, &include_ops, &include_fenced)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(DescribeCluster, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_DescribeClusterOptions_set_include_authorized_operations_err(o, (int8_t)include_ops, &ffi_err), ffi_take(&ffi_err)));
    FFI_STEP(o_err, (kafka_admin_DescribeClusterOptions_set_include_fenced_brokers_err(o, (int8_t)include_fenced, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DescribeClusterResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_cluster_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeClusterOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeCluster), assemble_multi);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeClusterResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeClusterResult_cluster_id_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_str, 0)); }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeClusterResult_nodes_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_node_list, 1)); }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeClusterResult_controller_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_node, 2)); }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeClusterResult_authorized_operations_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_acl_operations, 3)); }
    return job_or_error(job, NULL);
}

// (type_id, name) rows -> owned ConfigResource handles. 0 / -1.
static int config_resource_list_build(PyObject* seq, obj_list_t* l) {
    kafka_common_Error_t *ffi_err = NULL;
    PyObject* fast = PySequence_Fast(seq, "resources must be a sequence of (type, name)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (obj_list_init(l, n, (handle_destroy_fn)kafka_common_config_ConfigResource_destroy_err) < 0) { Py_DECREF(fast); return -1; }
    for (Py_ssize_t i = 0; i < n; i++) {
        int type; const char* name;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "is", &type, &name)) { Py_DECREF(fast); obj_list_free(l); return -1; }
        const kafka_common_config_ConfigResource_Type_t* rt = NULL;
        if (ffi_raise((rt = kafka_common_config_ConfigResource_Type_for_id_err((int8_t)type, &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(fast); obj_list_free(l); return -1;
        }
        if (rt == NULL) {
            PyErr_Format(PyExc_ValueError, "unknown ConfigResource type id %d", type);
            Py_DECREF(fast); obj_list_free(l); return -1;
        }
        kafka_common_config_ConfigResource_t* resource = NULL;
        if (ffi_raise((resource = kafka_common_config_ConfigResource_new_err(rt, name, &ffi_err), ffi_take(&ffi_err))) || obj_list_add(l, resource) < 0) {
            Py_DECREF(fast); obj_list_free(l); return -1;
        }
    }
    Py_DECREF(fast);
    return 0;
}

// Admin_describe_configs(h, [(type, name)], timeout_ms, include_synonyms,
//                        include_documentation) -> (job, err).
// Payload: {(type, name): (error, [entry9])}.
static PyObject* py_Admin_describe_configs(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int synonyms, documentation;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &spec, &timeout_ms, &synonyms, &documentation)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    obj_list_t resources;
    if (config_resource_list_build(spec, &resources) < 0) return NULL;
    ADMIN_OPTIONS(DescribeConfigs, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_DescribeConfigsOptions_set_include_synonyms_err(o, (int8_t)synonyms, &ffi_err), ffi_take(&ffi_err)));
    FFI_STEP(o_err, (kafka_admin_DescribeConfigsOptions_set_include_documentation_err(o, (int8_t)documentation, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DescribeConfigsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_configs_with_options_err(a->admin, resources.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeConfigsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    obj_list_free(&resources);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeConfigs), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeConfigsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeConfigsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_config_resource, conv_config_full, 0)); }
    return job_or_error(job, NULL);
}

static void alter_config_op_list_free(void* l, kafka_common_Error_t** out_err) {
    kafka_common_Error_t *ffi_err = NULL;
    kafka_List_t* list = (kafka_List_t*)l;
    int32_t n = 0;
    kafka_common_Error_t* err = (n = kafka_List_size_err(list, &ffi_err), ffi_take(&ffi_err));
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* op = NULL;
        err = (op = kafka_List_get_err(list, i, &ffi_err), ffi_take(&ffi_err));
        if (err == NULL) ffi_unraisable((kafka_admin_AlterConfigOp_destroy_err((kafka_admin_AlterConfigOp_t*)op, &ffi_err), ffi_take(&ffi_err)));
    }
    ffi_unraisable(err);
    kafka_List_destroy_err(list, out_err);
}

// Admin_incremental_alter_configs(h, [(type, name, entry_name, value | None, op_type)],
//                                 timeout_ms, validate_only) -> (job, err).
// Payload: {(type, name): error}. Rows are one per operation, grouped by
// resource (consecutive rows with the same (type, name) share one
// ConfigResource key, as the Python side emits them).
static PyObject* py_Admin_incremental_alter_configs(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)self;
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &validate_only)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "configs must be a sequence of rows");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // owned ConfigResource* -> owned kafka_List_t of owned AlterConfigOp*
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_config_ConfigResource_destroy_err, alter_config_op_list_free) < 0) {
        Py_DECREF(fast); return NULL;
    }
    int last_type = -1; const char* last_name = NULL; kafka_List_t* ops = NULL;
    for (Py_ssize_t i = 0; i < n; i++) {
        int type; const char *name, *entry_name, *value; int op;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "isszi", &type, &name, &entry_name, &value, &op)) goto fail;
        if (ops == NULL || type != last_type || strcmp(name, last_name) != 0) {
            const kafka_common_config_ConfigResource_Type_t* rt = NULL;
            if (ffi_raise((rt = kafka_common_config_ConfigResource_Type_for_id_err((int8_t)type, &ffi_err), ffi_take(&ffi_err)))) goto fail;
            if (rt == NULL) { PyErr_Format(PyExc_ValueError, "unknown ConfigResource type id %d", type); goto fail; }
            kafka_common_config_ConfigResource_t* resource = NULL;
            ops = NULL;
            if (ffi_raise((ops = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) goto fail;
            if (ffi_raise((resource = kafka_common_config_ConfigResource_new_err(rt, name, &ffi_err), ffi_take(&ffi_err)))) {
                ffi_unraisable((kafka_List_destroy_err(ops, &ffi_err), ffi_take(&ffi_err))); goto fail;
            }
            if (obj_map_put(&m, resource, ops) < 0) goto fail;
            last_type = type; last_name = name;
        }
        const kafka_admin_AlterConfigOp_OpType_t* op_type = NULL;
        if (ffi_raise((op_type = kafka_admin_AlterConfigOp_OpType_for_id_err((int8_t)op, &ffi_err), ffi_take(&ffi_err)))) goto fail;
        if (op_type == NULL) {
            // Reported through the error channel, as the call's KafkaError:
            // Java's typed OpType cannot carry an unknown id, so this is the
            // marshaling failure admin.py documents (the RPC is not submitted).
            char msg[64];
            snprintf(msg, sizeof msg, "unknown AlterConfigOp type id %d", op);
            obj_map_free(&m);
            Py_DECREF(fast);
            return job_or_error(NULL, ffi_built(kafka_common_Error_local_illegal_argument_err(msg, &ffi_err), &ffi_err));
        }
        kafka_admin_ConfigEntry_t* entry = NULL;
        kafka_admin_AlterConfigOp_t* alter_op = NULL;
        int failed = ffi_raise((entry = kafka_admin_ConfigEntry_new_err(entry_name, value, &ffi_err), ffi_take(&ffi_err)))
            || ffi_raise((alter_op = kafka_admin_AlterConfigOp_new_err(entry, op_type, &ffi_err), ffi_take(&ffi_err)));
        if (entry) ffi_unraisable((kafka_admin_ConfigEntry_destroy_err(entry, &ffi_err), ffi_take(&ffi_err)));   // copied by AlterConfigOp_new
        if (failed) goto fail;
        if (ffi_raise((kafka_List_add_err(ops, alter_op, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_admin_AlterConfigOp_destroy_err(alter_op, &ffi_err), ffi_take(&ffi_err))); goto fail;
        }
    }
    {
        ADMIN_OPTIONS(AlterConfigs, o, timeout_ms);
        FFI_STEP(o_err, (kafka_admin_AlterConfigsOptions_set_validate_only_err(o, (int8_t)validate_only, &ffi_err), ffi_take(&ffi_err)));
        kafka_admin_AlterConfigsResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_incremental_alter_configs_with_options_err(a->admin, m.map, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_AlterConfigsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        obj_map_free(&m);
        Py_DECREF(fast);
        if (ffi_raise(o_err)) return NULL;
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterConfigs), assemble_keyed_errors);
        if (job == NULL) { ffi_unraisable((kafka_admin_AlterConfigsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_AlterConfigsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_config_resource, NULL, 0)); }
        return job_or_error(job, NULL);
    }
fail:
    obj_map_free(&m);
    Py_DECREF(fast);
    return NULL;
}

// Admin_list_config_resources(h, [type_id], timeout_ms) -> (job, err).
// Payload: (error, [(type, name)]). An empty type list means every type.
static PyObject* py_Admin_list_config_resources(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* types; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &types, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(types, "resource_types must be a sequence of int");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    kafka_List_t* list = NULL;   // borrowed Type singletons
    if (ffi_raise((list = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        long v = PyLong_AsLong(PySequence_Fast_GET_ITEM(fast, i));
        if (v == -1 && PyErr_Occurred()) { ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err))); Py_DECREF(fast); return NULL; }
        const kafka_common_config_ConfigResource_Type_t* rt = NULL;
        if (ffi_raise((rt = kafka_common_config_ConfigResource_Type_for_id_err((int8_t)v, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err))); Py_DECREF(fast); return NULL;
        }
        if (rt == NULL) {
            PyErr_Format(PyExc_ValueError, "unknown ConfigResource type id %ld", v);
            ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err))); Py_DECREF(fast); return NULL;
        }
        if (ffi_raise((kafka_List_add_err(list, (void*)rt, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err))); Py_DECREF(fast); return NULL;
        }
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(ListConfigResources, o, timeout_ms);
    kafka_admin_ListConfigResourcesResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_list_config_resources_with_options_err(a->admin, list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_ListConfigResourcesOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_List_destroy_err(list, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListConfigResources), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_ListConfigResourcesResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_ListConfigResourcesResult_all_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_config_resources, 0)); }
    return job_or_error(job, NULL);
}

// ---- log dirs ------------------------------------------------------------------------

// Admin_describe_log_dirs(h, [broker_id], timeout_ms) -> (job, err).
// Payload: {broker_id: (error, {log_dir: description4})}.
static PyObject* py_Admin_describe_log_dirs(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* brokers; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &brokers, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    i32_list_t ids;
    if (i32_list_build(brokers, &ids, "brokers must be a sequence of int") < 0) return NULL;
    ADMIN_OPTIONS(DescribeLogDirs, o, timeout_ms);
    kafka_admin_DescribeLogDirsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_log_dirs_with_options_err(a->admin, ids.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeLogDirsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    i32_list_free(&ids);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeLogDirs), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeLogDirsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeLogDirsResult_descriptions_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_i32, conv_log_dir_descriptions, 0)); }
    return job_or_error(job, NULL);
}

// Admin_alter_replica_log_dirs(h, [(topic, partition, broker_id, log_dir)], timeout_ms)
// -> (job, err). Payload: {(topic, partition, broker_id): error}.
static PyObject* py_Admin_alter_replica_log_dirs(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tpr_list_t replicas; PyObject* fast = NULL; const char** dirs = NULL;
    if (tpr_list_build(spec, &replicas, 1, &fast, &dirs) < 0) return NULL;
    kafka_Map_t* m = NULL;   // borrowed TPReplica* -> borrowed char* (owned by fast)
    kafka_common_Error_t* m_err = (m = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err));
    for (Py_ssize_t i = 0; m_err == NULL && i < replicas.n; i++) {
        m_err = (kafka_Map_put_err(m, replicas.replicas[i], (void*)dirs[i], &ffi_err), ffi_take(&ffi_err));
    }
    ADMIN_OPTIONS(AlterReplicaLogDirs, o, timeout_ms);
    // The map's error comes first and is the one raised.
    if (m_err) { ffi_unraisable(o_err); o_err = m_err; }
    kafka_admin_AlterReplicaLogDirsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_alter_replica_log_dirs_with_options_err(a->admin, m, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_AlterReplicaLogDirsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (m) ffi_unraisable((kafka_Map_destroy_err(m, &ffi_err), ffi_take(&ffi_err)));
    tpr_list_free(&replicas);
    PyMem_Free(dirs);
    Py_XDECREF(fast);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterReplicaLogDirs), assemble_keyed_errors);
    if (job == NULL) { ffi_unraisable((kafka_admin_AlterReplicaLogDirsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_AlterReplicaLogDirsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_tpr, NULL, 0)); }
    return job_or_error(job, NULL);
}

// Admin_describe_replica_log_dirs(h, [(topic, partition, broker_id)], timeout_ms)
// -> (job, err). Payload: {(topic, partition, broker_id): (error, info4)}.
static PyObject* py_Admin_describe_replica_log_dirs(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tpr_list_t replicas;
    if (tpr_list_build(spec, &replicas, 0, NULL, NULL) < 0) return NULL;
    ADMIN_OPTIONS(DescribeReplicaLogDirs, o, timeout_ms);
    kafka_admin_DescribeReplicaLogDirsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_replica_log_dirs_with_options_err(a->admin, replicas.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeReplicaLogDirsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    tpr_list_free(&replicas);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeReplicaLogDirs), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeReplicaLogDirsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeReplicaLogDirsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_tpr, conv_replica_log_dir_info, 0)); }
    return job_or_error(job, NULL);
}

// ---- elections, reassignments, offsets --------------------------------------------------

// Admin_elect_leaders(h, election_type, all_partitions, [(topic, partition)], timeout_ms)
// -> (job, err). Payload: (error, {(topic, partition): error | None}).
// `all_partitions` is Java's null Set (every partition in the cluster).
static PyObject* py_Admin_elect_leaders(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; int election_type, all_partitions; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KipOL", &h, &election_type, &all_partitions, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    const kafka_common_ElectionType_t* et = NULL;
    kafka_common_Error_t* err = (et = kafka_common_ElectionType_value_of((int8_t)election_type, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) return job_or_error(NULL, err);
    tp_list_t tps;
    memset(&tps, 0, sizeof(tps));
    if (!all_partitions && tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(ElectLeaders, o, timeout_ms);
    kafka_admin_ElectLeadersResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_elect_leaders_with_options_err(
        a->admin, et, all_partitions ? NULL : tps.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_ElectLeadersOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    tp_list_free(&tps);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ElectLeaders), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_ElectLeadersResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_ElectLeadersResult_partitions_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_elect_leaders, 0)); }
    return job_or_error(job, NULL);
}

// Admin_alter_partition_reassignments(h, [(topic, partition, is_none, [replica])],
//                                     timeout_ms, allow_replication_factor_change)
// -> (job, err). Payload: {(topic, partition): error}. `is_none` is Java's
// empty Optional (cancel the reassignment), which crosses as a NULL value.
static PyObject* py_Admin_alter_partition_reassignments(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int allow_rf;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &allow_rf)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "reassignments must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy_err,
                     (handle_destroy_fn)kafka_admin_NewPartitionReassignment_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* t; int p, is_none; PyObject* replicas;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "sipO", &t, &p, &is_none, &replicas)) goto fail;
        kafka_admin_NewPartitionReassignment_t* npr = NULL;
        if (!is_none) {
            kafka_List_t* list = i32_list_boxed(replicas, "target_replicas must be a sequence of int");
            if (list == NULL) goto fail;
            kafka_common_Error_t* err = (npr = kafka_admin_NewPartitionReassignment_new(list, &ffi_err), ffi_take(&ffi_err));
            ffi_unraisable(handle_destroy(i32_list_boxed_free, list));
            if (err != NULL) {
                char prefix[192];
                snprintf(prefix, sizeof(prefix), "reassignment for %s-%d at index %zd", t, p, i);
                err = prefixed_error(prefix, err);
                obj_map_free(&m); Py_DECREF(fast);
                return job_or_error(NULL, err);
            }
        }
        kafka_common_TopicPartition_t* tp = NULL;
        if (ffi_raise((tp = kafka_common_TopicPartition_new_err(t, (int32_t)p, &ffi_err), ffi_take(&ffi_err)))) {
            if (npr) ffi_unraisable((kafka_admin_NewPartitionReassignment_destroy_err(npr, &ffi_err), ffi_take(&ffi_err)));
            goto fail;
        }
        if (obj_map_put(&m, tp, npr) < 0) goto fail;
    }
    {
        ADMIN_OPTIONS(AlterPartitionReassignments, o, timeout_ms);
        FFI_STEP(o_err, (kafka_admin_AlterPartitionReassignmentsOptions_set_allow_replication_factor_change_err(o, (int8_t)allow_rf, &ffi_err), ffi_take(&ffi_err)));
        kafka_admin_AlterPartitionReassignmentsResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_alter_partition_reassignments_with_options_err(a->admin, m.map, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_AlterPartitionReassignmentsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        obj_map_free(&m);
        Py_DECREF(fast);
        if (ffi_raise(o_err)) return NULL;
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterPartitionReassignments), assemble_keyed_errors);
        if (job == NULL) { ffi_unraisable((kafka_admin_AlterPartitionReassignmentsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_AlterPartitionReassignmentsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_tp, NULL, 0)); }
        return job_or_error(job, NULL);
    }
fail:
    obj_map_free(&m);
    Py_DECREF(fast);
    return NULL;
}

// Admin_list_partition_reassignments(h, all_partitions, [(topic, partition)], timeout_ms)
// -> (job, err). Payload: (error, {(topic, partition): (replicas, adding, removing)}).
static PyObject* py_Admin_list_partition_reassignments(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; int all_partitions; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KpOL", &h, &all_partitions, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tp_list_t tps;
    memset(&tps, 0, sizeof(tps));
    if (!all_partitions && tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(ListPartitionReassignments, o, timeout_ms);
    kafka_admin_ListPartitionReassignmentsResult_t* r = NULL;
    if (all_partitions) {
        FFI_STEP(o_err, (r = kafka_admin_Admin_list_partition_reassignments_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    } else {
        FFI_STEP(o_err, (r = kafka_admin_Admin_list_partition_reassignments_with_partitions_options_err(
            a->admin, tps.list, o, &ffi_err), ffi_take(&ffi_err)));
    }
    if (o) ffi_unraisable((kafka_admin_ListPartitionReassignmentsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    tp_list_free(&tps);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListPartitionReassignments), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_ListPartitionReassignmentsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_ListPartitionReassignmentsResult_reassignments_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_partition_reassignments, 0)); }
    return job_or_error(job, NULL);
}

// The Python OffsetSpec sentinels (admin.OffsetSpec._LATEST ...), carried in
// the `value` column of a non-timestamp row.
enum {
    OFFSET_SPEC_LATEST = -1, OFFSET_SPEC_EARLIEST = -2, OFFSET_SPEC_MAX_TIMESTAMP = -3,
    OFFSET_SPEC_EARLIEST_LOCAL = -4, OFFSET_SPEC_LATEST_TIERED = -5, OFFSET_SPEC_EARLIEST_PENDING_UPLOAD = -6,
};

static void offset_spec_destroy_if_owned(void* spec, kafka_common_Error_t** out_err) {
    // Only the timestamp spec is an owned handle; the others are singletons
    // the map stores as borrowed pointers, which must not be freed. The owned
    // ones are tracked separately (see py_Admin_list_offsets).
    (void)spec;
    (void)out_err;
}

// Admin_list_offsets(h, [(topic, partition, is_timestamp, value)], timeout_ms,
//                    isolation_level) -> (job, err).
// Payload: {(topic, partition): (error, (offset, timestamp, leader_epoch))}.
static PyObject* py_Admin_list_offsets(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int isolation_level;
    if (!PyArg_ParseTuple(args, "KOLi", &h, &spec, &timeout_ms, &isolation_level)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    const kafka_common_IsolationLevel_t* il = NULL;
    kafka_common_Error_t* err = (il = kafka_common_IsolationLevel_for_id((int8_t)isolation_level, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) return job_or_error(NULL, err);
    PyObject* fast = fast_rows(spec, "topic_partition_offsets must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // owned TP* -> OffsetSpec* (singleton, or owned timestamp spec tracked in `owned`)
    obj_list_t owned;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy_err, offset_spec_destroy_if_owned) < 0) {
        Py_DECREF(fast); return NULL;
    }
    if (obj_list_init(&owned, n, (handle_destroy_fn)kafka_admin_OffsetSpec_destroy_err) < 0) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* t; int p, is_timestamp; long long value;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "sipL", &t, &p, &is_timestamp, &value)) goto fail;
        const kafka_admin_OffsetSpec_t* os = NULL;
        kafka_common_Error_t* spec_err = NULL;
        if (is_timestamp) {
            kafka_admin_OffsetSpec_t* ts = NULL;
            spec_err = (ts = kafka_admin_OffsetSpec_timestamp_err((int64_t)value, &ffi_err), ffi_take(&ffi_err));
            if (spec_err == NULL && obj_list_add(&owned, ts) < 0) goto fail;
            os = ts;
        } else {
            switch (value) {
                case OFFSET_SPEC_LATEST: spec_err = (os = kafka_admin_OffsetSpec_latest_err(&ffi_err), ffi_take(&ffi_err)); break;
                case OFFSET_SPEC_EARLIEST: spec_err = (os = kafka_admin_OffsetSpec_earliest_err(&ffi_err), ffi_take(&ffi_err)); break;
                case OFFSET_SPEC_MAX_TIMESTAMP: spec_err = (os = kafka_admin_OffsetSpec_max_timestamp_err(&ffi_err), ffi_take(&ffi_err)); break;
                case OFFSET_SPEC_EARLIEST_LOCAL: spec_err = (os = kafka_admin_OffsetSpec_earliest_local_err(&ffi_err), ffi_take(&ffi_err)); break;
                case OFFSET_SPEC_LATEST_TIERED: spec_err = (os = kafka_admin_OffsetSpec_latest_tiered_err(&ffi_err), ffi_take(&ffi_err)); break;
                case OFFSET_SPEC_EARLIEST_PENDING_UPLOAD:
                    spec_err = (os = kafka_admin_OffsetSpec_earliest_pending_upload_err(&ffi_err), ffi_take(&ffi_err)); break;
                default:
                    PyErr_Format(PyExc_ValueError, "unknown OffsetSpec sentinel %lld", value);
                    goto fail;
            }
        }
        if (ffi_raise(spec_err)) goto fail;
        kafka_common_TopicPartition_t* tp = NULL;
        if (ffi_raise((tp = kafka_common_TopicPartition_new_err(t, (int32_t)p, &ffi_err), ffi_take(&ffi_err))) || obj_map_put(&m, tp, (void*)os) < 0)
            goto fail;
    }
    {
        // ListOffsetsOptions(IsolationLevel): the owned constructor form.
        kafka_admin_ListOffsetsOptions_t* o = NULL;
        kafka_common_Error_t* o_err = (o = kafka_admin_ListOffsetsOptions_with_isolation_level_err(il, &ffi_err), ffi_take(&ffi_err));
        ADMIN_SET_TIMEOUT(ListOffsets, o, timeout_ms, o_err);
        kafka_admin_ListOffsetsResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_list_offsets_with_options_err(a->admin, m.map, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_ListOffsetsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        obj_list_free(&owned);
        Py_DECREF(fast);
        if (ffi_raise(o_err)) { obj_map_free(&m); return NULL; }
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListOffsets), assemble_keyed_pairs);
        if (job == NULL) { obj_map_free(&m); ffi_unraisable((kafka_admin_ListOffsetsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        // One owned per-partition future per requested partition, keyed by it.
        for (Py_ssize_t i = 0; i < m.n; i++) {
            const kafka_common_TopicPartition_t* tp = (const kafka_common_TopicPartition_t*)m.keys[i];
            kafka_common_KafkaFuture_t* f = NULL;
            kafka_common_Error_t* perr = (f = kafka_admin_ListOffsetsResult_partition_result(r, tp, &ffi_err), ffi_take(&ffi_err));
            if (perr != NULL) { obj_map_free(&m); job_free(job); return job_or_error(NULL, perr); }
            PyObject* key = tp_to_py(tp);
            if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); obj_map_free(&m); job_free(job); return NULL; }
            if (job_add_slot(job, f, 1, key, conv_list_offsets_info, NULL, 0) == NULL) { obj_map_free(&m); job_free(job); return NULL; }
        }
        obj_map_free(&m);
        return job_or_error(job, NULL);
    }
fail:
    obj_list_free(&owned);
    obj_map_free(&m);
    Py_DECREF(fast);
    return NULL;
}

// ---- groups ----------------------------------------------------------------------------

// Admin_list_groups(h, [state_name], [protocol_type], [type_name], timeout_ms)
// -> (job, err). Payload: (error, ([listing5], [error4])) -- Java's valid()
// and errors() futures. Names are the Java enums' toString() forms; an empty
// list leaves that filter unset.
static PyObject* py_Admin_list_groups(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject *states, *protocols, *types; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOOOL", &h, &states, &protocols, &types, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t s, pr, ty;
    if (str_list_build_msg(states, &s, "group_states must be a sequence of str") < 0) return NULL;
    if (str_list_build_msg(protocols, &pr, "protocol_types must be a sequence of str") < 0) { str_list_free(&s); return NULL; }
    if (str_list_build_msg(types, &ty, "types must be a sequence of str") < 0) { str_list_free(&s); str_list_free(&pr); return NULL; }
    kafka_admin_ListGroupsOptions_t* o = NULL;
    kafka_common_Error_t* o_err = (o = kafka_admin_ListGroupsOptions_new_err(&ffi_err), ffi_take(&ffi_err));
    ADMIN_SET_TIMEOUT(ListGroups, o, timeout_ms, o_err);
    int32_t n = 0;
    FFI_STEP(o_err, (n = kafka_List_size_err(s.list, &ffi_err), ffi_take(&ffi_err)));
    if (o_err == NULL && n > 0) {
        kafka_List_t* singletons = NULL;
        FFI_STEP(o_err, (singletons = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)));
        for (int32_t i = 0; o_err == NULL && i < n; i++) {
            void* name = NULL;
            const kafka_common_GroupState_t* state = NULL;
            FFI_STEP(o_err, (name = kafka_List_get_err(s.list, i, &ffi_err), ffi_take(&ffi_err)));
            FFI_STEP(o_err, (state = kafka_common_GroupState_parse_err((const char*)name, &ffi_err), ffi_take(&ffi_err)));
            FFI_STEP(o_err, (kafka_List_add_err(singletons, (void*)state, &ffi_err), ffi_take(&ffi_err)));
        }
        FFI_STEP(o_err, (kafka_admin_ListGroupsOptions_in_group_states_err(o, singletons, &ffi_err), ffi_take(&ffi_err)));
        if (singletons) ffi_unraisable((kafka_List_destroy_err(singletons, &ffi_err), ffi_take(&ffi_err)));
    }
    n = 0;
    FFI_STEP(o_err, (n = kafka_List_size_err(pr.list, &ffi_err), ffi_take(&ffi_err)));
    if (n > 0) FFI_STEP(o_err, (kafka_admin_ListGroupsOptions_with_protocol_types_err(o, pr.list, &ffi_err), ffi_take(&ffi_err)));
    n = 0;
    FFI_STEP(o_err, (n = kafka_List_size_err(ty.list, &ffi_err), ffi_take(&ffi_err)));
    if (o_err == NULL && n > 0) {
        kafka_List_t* singletons = NULL;
        FFI_STEP(o_err, (singletons = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err)));
        for (int32_t i = 0; o_err == NULL && i < n; i++) {
            void* name = NULL;
            const kafka_common_GroupType_t* type = NULL;
            FFI_STEP(o_err, (name = kafka_List_get_err(ty.list, i, &ffi_err), ffi_take(&ffi_err)));
            FFI_STEP(o_err, (type = kafka_common_GroupType_parse_err((const char*)name, &ffi_err), ffi_take(&ffi_err)));
            FFI_STEP(o_err, (kafka_List_add_err(singletons, (void*)type, &ffi_err), ffi_take(&ffi_err)));
        }
        FFI_STEP(o_err, (kafka_admin_ListGroupsOptions_with_types_err(o, singletons, &ffi_err), ffi_take(&ffi_err)));
        if (singletons) ffi_unraisable((kafka_List_destroy_err(singletons, &ffi_err), ffi_take(&ffi_err)));
    }
    kafka_admin_ListGroupsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_list_groups_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_ListGroupsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    str_list_free(&s); str_list_free(&pr); str_list_free(&ty);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListGroups), assemble_multi);
    if (job == NULL) { ffi_unraisable((kafka_admin_ListGroupsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_ListGroupsResult_valid_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_group_listings, 0)); }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_ListGroupsResult_errors_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_error_list, 1)); }
    return job_or_error(job, NULL);
}

// Admin_describe_consumer_groups(h, [group_id], timeout_ms, include_authorized_operations)
// -> (job, err). Payload: {group_id: (error, description10)}.
static PyObject* py_Admin_describe_consumer_groups(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* ids; long long timeout_ms; int include_ops;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &ids, &timeout_ms, &include_ops)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "group_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeConsumerGroups, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_DescribeConsumerGroupsOptions_set_include_authorized_operations_err(o, (int8_t)include_ops, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DescribeConsumerGroupsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_consumer_groups_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeConsumerGroupsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    str_list_free(&l);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeConsumerGroups), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeConsumerGroupsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeConsumerGroupsResult_described_groups_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, conv_consumer_group_description, 0)); }
    return job_or_error(job, NULL);
}

// Admin_describe_classic_groups(h, [group_id], timeout_ms, include_authorized_operations)
// -> (job, err). Payload: {group_id: (error, description8)}.
static PyObject* py_Admin_describe_classic_groups(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* ids; long long timeout_ms; int include_ops;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &ids, &timeout_ms, &include_ops)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "group_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeClassicGroups, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_DescribeClassicGroupsOptions_set_include_authorized_operations_err(o, (int8_t)include_ops, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DescribeClassicGroupsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_classic_groups_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeClassicGroupsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    str_list_free(&l);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeClassicGroups), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeClassicGroupsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeClassicGroupsResult_described_groups_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, conv_classic_group_description, 0)); }
    return job_or_error(job, NULL);
}

// Admin_list_consumer_group_offsets(h, [(group_id, all_partitions, [(topic, partition)])],
//                                   timeout_ms, require_stable) -> (job, err).
// Payload: {group_id: (error, {(topic, partition): (offset, metadata, epoch) | None})}.
// `all_partitions` is Java's unset collection: every partition the group has
// committed offsets for.
static PyObject* py_Admin_list_consumer_group_offsets(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int require_stable;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &require_stable)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "group_specs must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // borrowed char* (owned by fast) -> owned ListConsumerGroupOffsetsSpec*
    if (obj_map_init(&m, n, NULL, (handle_destroy_fn)kafka_admin_ListConsumerGroupOffsetsSpec_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* gid; int all_partitions; PyObject* rows;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "spO", &gid, &all_partitions, &rows)) goto fail;
        kafka_admin_ListConsumerGroupOffsetsSpec_t* s = NULL;
        if (ffi_raise((s = kafka_admin_ListConsumerGroupOffsetsSpec_new_err(&ffi_err), ffi_take(&ffi_err))) || obj_map_put(&m, (void*)gid, s) < 0) goto fail;
        if (!all_partitions) {
            tp_list_t tps;
            if (tp_list_build(rows, &tps) < 0) goto fail;
            int failed = ffi_raise((kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions_err(s, tps.list, &ffi_err), ffi_take(&ffi_err)));   // copied
            tp_list_free(&tps);
            if (failed) goto fail;
        }
    }
    {
        ADMIN_OPTIONS(ListConsumerGroupOffsets, o, timeout_ms);
        FFI_STEP(o_err, (kafka_admin_ListConsumerGroupOffsetsOptions_set_require_stable_err(o, (int8_t)require_stable, &ffi_err), ffi_take(&ffi_err)));
        kafka_admin_ListConsumerGroupOffsetsResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options_err(a->admin, m.map, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_ListConsumerGroupOffsetsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        if (ffi_raise(o_err)) goto fail;
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListConsumerGroupOffsets), assemble_keyed_pairs);
        if (job == NULL) { obj_map_free(&m); Py_DECREF(fast); ffi_unraisable((kafka_admin_ListConsumerGroupOffsetsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        // One owned future per requested group id, keyed by it.
        for (Py_ssize_t i = 0; i < m.n; i++) {
            const char* gid = (const char*)m.keys[i];
            kafka_common_KafkaFuture_t* f = NULL;
            kafka_common_Error_t* perr =
                (f = kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(r, gid, &ffi_err), ffi_take(&ffi_err));
            if (perr != NULL) { obj_map_free(&m); Py_DECREF(fast); job_free(job); return job_or_error(NULL, perr); }
            PyObject* key = key_str(gid);
            if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); obj_map_free(&m); Py_DECREF(fast); job_free(job); return NULL; }
            if (job_add_slot(job, f, 1, key, conv_group_offsets, NULL, 0) == NULL) { obj_map_free(&m); Py_DECREF(fast); job_free(job); return NULL; }
        }
        obj_map_free(&m);
        Py_DECREF(fast);
        return job_or_error(job, NULL);
    }
fail:
    obj_map_free(&m);
    Py_DECREF(fast);
    return NULL;
}

// Admin_alter_consumer_group_offsets(h, group_id,
//     [(topic, partition, offset, metadata | None, has_leader_epoch, leader_epoch)],
//     timeout_ms) -> (job, err). Payload: {(topic, partition): error}, or for
//     an empty offsets map (error | None, None) from `all()`.
static PyObject* py_Admin_alter_consumer_group_offsets(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)self;
    unsigned long long h; const char* gid; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KsOL", &h, &gid, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "offsets must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy_err,
                     (handle_destroy_fn)kafka_consumer_OffsetAndMetadata_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char *t, *metadata; int p, has_epoch, epoch; long long offset;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siLzpi", &t, &p, &offset, &metadata, &has_epoch, &epoch)) {
            obj_map_free(&m); Py_DECREF(fast); return NULL;
        }
        kafka_consumer_OffsetAndMetadata_t* om = NULL;
        kafka_common_Error_t* err = has_epoch
            ? (om = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata((int64_t)offset, (int32_t)epoch, metadata, &ffi_err), ffi_take(&ffi_err))
            : (om = kafka_consumer_OffsetAndMetadata_with_metadata((int64_t)offset, metadata, &ffi_err), ffi_take(&ffi_err));
        if (err != NULL) {
            obj_map_free(&m); Py_DECREF(fast);
            return job_or_error(NULL, indexed_error("offset", i, err));
        }
        kafka_common_TopicPartition_t* tp = NULL;
        if (ffi_raise((tp = kafka_common_TopicPartition_new_err(t, (int32_t)p, &ffi_err), ffi_take(&ffi_err)))) {
            ffi_unraisable((kafka_consumer_OffsetAndMetadata_destroy_err(om, &ffi_err), ffi_take(&ffi_err)));
            obj_map_free(&m); Py_DECREF(fast); return NULL;
        }
        if (obj_map_put(&m, tp, om) < 0) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(AlterConsumerGroupOffsets, o, timeout_ms);
    kafka_admin_AlterConsumerGroupOffsetsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_alter_consumer_group_offsets_with_options_err(a->admin, gid, m.map, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_AlterConsumerGroupOffsetsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) { obj_map_free(&m); return NULL; }
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterConsumerGroupOffsets), assemble_keyed_errors);
    if (job == NULL) { obj_map_free(&m); ffi_unraisable((kafka_admin_AlterConsumerGroupOffsetsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    if (m.n == 0) {
        // No partition, so no per-key slot to report the outcome in: the job
        // resolves Java's `all()` instead, payload (error | None, None), which
        // admin.py raises (or returns {} for).
        obj_map_free(&m);
        job->assemble = assemble_single;
        { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_AlterConsumerGroupOffsetsResult_all_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, NULL, 0)); }
        return job_or_error(job, NULL);
    }
    for (Py_ssize_t i = 0; i < m.n; i++) {
        const kafka_common_TopicPartition_t* tp = (const kafka_common_TopicPartition_t*)m.keys[i];
        kafka_common_KafkaFuture_t* f = NULL;
        if (ffi_raise((f = kafka_admin_AlterConsumerGroupOffsetsResult_partition_result_err(r, tp, &ffi_err), ffi_take(&ffi_err)))) {
            obj_map_free(&m); job_free(job); return NULL;
        }
        PyObject* key = tp_to_py(tp);
        if (key == NULL) { if (f) ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); obj_map_free(&m); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, NULL, NULL, 0) == NULL) { obj_map_free(&m); job_free(job); return NULL; }
    }
    obj_map_free(&m);
    return job_or_error(job, NULL);
}

// Admin_delete_consumer_group_offsets(h, group_id, [(topic, partition)], timeout_ms)
// -> (job, err). Payload: {(topic, partition): error}.
static PyObject* py_Admin_delete_consumer_group_offsets(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* gid; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KsOL", &h, &gid, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tp_list_t tps;
    if (tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(DeleteConsumerGroupOffsets, o, timeout_ms);
    kafka_admin_DeleteConsumerGroupOffsetsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_delete_consumer_group_offsets_with_options_err(a->admin, gid, tps.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DeleteConsumerGroupOffsetsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) { tp_list_free(&tps); return NULL; }
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteConsumerGroupOffsets), assemble_keyed_errors);
    if (job == NULL) { tp_list_free(&tps); ffi_unraisable((kafka_admin_DeleteConsumerGroupOffsetsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (Py_ssize_t i = 0; i < tps.n; i++) {
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = (f = kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result(r, tps.tps[i], &ffi_err), ffi_take(&ffi_err));
        if (perr != NULL) { tp_list_free(&tps); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = tp_to_py(tps.tps[i]);
        if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); tp_list_free(&tps); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, NULL, NULL, 0) == NULL) { tp_list_free(&tps); job_free(job); return NULL; }
    }
    tp_list_free(&tps);
    return job_or_error(job, NULL);
}

// Admin_delete_consumer_groups(h, [group_id], timeout_ms) -> (job, err).
// Payload: {group_id: error}.
static PyObject* py_Admin_delete_consumer_groups(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &ids, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "group_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DeleteConsumerGroups, o, timeout_ms);
    kafka_admin_DeleteConsumerGroupsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_delete_consumer_groups_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DeleteConsumerGroupsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    str_list_free(&l);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteConsumerGroups), assemble_keyed_errors);
    if (job == NULL) { ffi_unraisable((kafka_admin_DeleteConsumerGroupsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DeleteConsumerGroupsResult_deleted_groups_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, NULL, 0)); }
    return job_or_error(job, NULL);
}

// Admin_remove_members_from_consumer_group(h, group_id, remove_all, [group_instance_id],
//                                          reason | None, timeout_ms) -> (job, err).
// Payload: {group_instance_id: error} for an explicit member list, or
// (error, None) -- Java's all() -- in remove-all mode (Java's no-argument
// options constructor), where memberResult() is not applicable.
static PyObject* py_Admin_remove_members_from_consumer_group(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char *gid, *reason; int remove_all; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KspOzL", &h, &gid, &remove_all, &ids, &reason, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    obj_list_t members;
    if (remove_all) {
        if (obj_list_init(&members, 0, NULL) < 0) return NULL;
    } else {
        str_list_t l;
        if (str_list_build_msg(ids, &l, "members must be a sequence of group instance ids") < 0) return NULL;
        int32_t n = 0;
        if (ffi_raise((n = kafka_List_size_err(l.list, &ffi_err), ffi_take(&ffi_err)))) { str_list_free(&l); return NULL; }
        if (obj_list_init(&members, n, (handle_destroy_fn)kafka_admin_MemberToRemove_destroy_err) < 0) { str_list_free(&l); return NULL; }
        for (int32_t i = 0; i < n; i++) {
            void* id = NULL;
            kafka_admin_MemberToRemove_t* member = NULL;
            if (ffi_raise((id = kafka_List_get_err(l.list, i, &ffi_err), ffi_take(&ffi_err)))
                || ffi_raise((member = kafka_admin_MemberToRemove_new_err((const char*)id, &ffi_err), ffi_take(&ffi_err)))
                || obj_list_add(&members, member) < 0) {
                str_list_free(&l); obj_list_free(&members); return NULL;
            }
        }
        str_list_free(&l);
    }
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t* o = NULL;
    kafka_common_Error_t* o_err = NULL;
    if (remove_all) {
        o_err = (o = kafka_admin_RemoveMembersFromConsumerGroupOptions_new_err(&ffi_err), ffi_take(&ffi_err));
    } else {
        kafka_common_Error_t* err = (o = kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(members.list, &ffi_err), ffi_take(&ffi_err));
        if (err != NULL) { obj_list_free(&members); return job_or_error(NULL, err); }
    }
    ADMIN_SET_TIMEOUT(RemoveMembersFromConsumerGroup, o, timeout_ms, o_err);
    if (reason != NULL) FFI_STEP(o_err, (kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason_err(o, reason, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_RemoveMembersFromConsumerGroupResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_remove_members_from_consumer_group_with_options_err(a->admin, gid, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) { obj_list_free(&members); return NULL; }
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(RemoveMembersFromConsumerGroup),
                            remove_all ? assemble_single : assemble_keyed_errors);
    if (job == NULL) { obj_list_free(&members); ffi_unraisable((kafka_admin_RemoveMembersFromConsumerGroupResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    if (remove_all) {
        obj_list_free(&members);
        { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_RemoveMembersFromConsumerGroupResult_all_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, NULL, 0)); }
        return job_or_error(job, NULL);
    }
    for (Py_ssize_t i = 0; i < members.n; i++) {
        const kafka_admin_MemberToRemove_t* member = (const kafka_admin_MemberToRemove_t*)members.items[i];
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = (f = kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(r, member, &ffi_err), ffi_take(&ffi_err));
        if (perr != NULL) { obj_list_free(&members); job_free(job); return job_or_error(NULL, perr); }
        const char* instance_id = NULL;
        PyObject* key = ffi_raise((instance_id = kafka_admin_MemberToRemove_group_instance_id_err(member, &ffi_err), ffi_take(&ffi_err)))
            ? NULL : key_str(instance_id);
        if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); obj_list_free(&members); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, NULL, NULL, 0) == NULL) { obj_list_free(&members); job_free(job); return NULL; }
    }
    obj_list_free(&members);
    return job_or_error(job, NULL);
}

// ---- ACLs --------------------------------------------------------------------------

// Builds one AclBinding from a (resource_type, name, pattern_type, principal,
// host, operation, permission) row. NULL with `*err_out` set (the constructor
// rejected the row, "acl at index <i>: ...") or with a Python exception.
static kafka_common_acl_AclBinding_t* acl_binding_build(PyObject* row, Py_ssize_t index,
                                                        kafka_common_Error_t** err_out) {
    kafka_common_Error_t *ffi_err = NULL;
    const char *name, *principal, *host; int rtype, ptype, op, perm;
    *err_out = NULL;
    if (!PyArg_ParseTuple(row, "isissii", &rtype, &name, &ptype, &principal, &host, &op, &perm)) return NULL;
    const kafka_common_resource_ResourceType_t* resource_type = NULL;
    const kafka_common_resource_PatternType_t* pattern_type = NULL;
    const kafka_common_acl_AclOperation_t* operation = NULL;
    const kafka_common_acl_AclPermissionType_t* permission = NULL;
    if (ffi_raise((resource_type = kafka_common_resource_ResourceType_from_code_err((int8_t)rtype, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((pattern_type = kafka_common_resource_PatternType_from_code_err((int8_t)ptype, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((operation = kafka_common_acl_AclOperation_from_code_err((int8_t)op, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((permission = kafka_common_acl_AclPermissionType_from_code_err((int8_t)perm, &ffi_err), ffi_take(&ffi_err))))
        return NULL;
    kafka_common_resource_ResourcePattern_t* pattern = NULL;
    kafka_common_Error_t* err = (pattern = kafka_common_resource_ResourcePattern_new(resource_type, name, pattern_type, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) { *err_out = indexed_error("acl", index, err); return NULL; }
    kafka_common_acl_AccessControlEntry_t* entry = NULL;
    err = (entry = kafka_common_acl_AccessControlEntry_new(principal, host, operation, permission, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        ffi_unraisable((kafka_common_resource_ResourcePattern_destroy_err(pattern, &ffi_err), ffi_take(&ffi_err)));
        *err_out = indexed_error("acl", index, err);
        return NULL;
    }
    kafka_common_acl_AclBinding_t* binding = NULL;
    int failed = ffi_raise((binding = kafka_common_acl_AclBinding_new_err(pattern, entry, &ffi_err), ffi_take(&ffi_err)));  // copies both
    ffi_unraisable((kafka_common_resource_ResourcePattern_destroy_err(pattern, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_common_acl_AccessControlEntry_destroy_err(entry, &ffi_err), ffi_take(&ffi_err)));
    return failed ? NULL : binding;
}

// Builds one AclBindingFilter from a (resource_type, name | None, pattern_type,
// principal | None, host | None, operation, permission) row.
static kafka_common_acl_AclBindingFilter_t* acl_binding_filter_build(PyObject* row) {
    kafka_common_Error_t *ffi_err = NULL;
    const char *name, *principal, *host; int rtype, ptype, op, perm;
    if (!PyArg_ParseTuple(row, "izizzii", &rtype, &name, &ptype, &principal, &host, &op, &perm)) return NULL;
    const kafka_common_resource_ResourceType_t* resource_type = NULL;
    const kafka_common_resource_PatternType_t* pattern_type = NULL;
    const kafka_common_acl_AclOperation_t* operation = NULL;
    const kafka_common_acl_AclPermissionType_t* permission = NULL;
    kafka_common_resource_ResourcePatternFilter_t* pattern = NULL;
    kafka_common_acl_AccessControlEntryFilter_t* entry = NULL;
    kafka_common_acl_AclBindingFilter_t* filter = NULL;
    int failed = ffi_raise((resource_type = kafka_common_resource_ResourceType_from_code_err((int8_t)rtype, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((pattern_type = kafka_common_resource_PatternType_from_code_err((int8_t)ptype, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((operation = kafka_common_acl_AclOperation_from_code_err((int8_t)op, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((permission = kafka_common_acl_AclPermissionType_from_code_err((int8_t)perm, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((pattern = kafka_common_resource_ResourcePatternFilter_new_err(resource_type, name, pattern_type, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((entry = kafka_common_acl_AccessControlEntryFilter_new_err(principal, host, operation, permission, &ffi_err), ffi_take(&ffi_err)))
        || ffi_raise((filter = kafka_common_acl_AclBindingFilter_new_err(pattern, entry, &ffi_err), ffi_take(&ffi_err)));  // copies both
    if (pattern) ffi_unraisable((kafka_common_resource_ResourcePatternFilter_destroy_err(pattern, &ffi_err), ffi_take(&ffi_err)));
    if (entry) ffi_unraisable((kafka_common_acl_AccessControlEntryFilter_destroy_err(entry, &ffi_err), ffi_take(&ffi_err)));
    return failed ? NULL : filter;
}

// Admin_create_acls(h, [binding7], timeout_ms) -> (job, err).
// Payload: {binding7: error}.
static PyObject* py_Admin_create_acls(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "acls must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_acl_AclBinding_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        kafka_common_Error_t* err = NULL;
        kafka_common_acl_AclBinding_t* b = acl_binding_build(PySequence_Fast_GET_ITEM(fast, i), i, &err);
        if (b == NULL) {
            obj_list_free(&l); Py_DECREF(fast);
            return err ? job_or_error(NULL, err) : NULL;
        }
        if (obj_list_add(&l, b) < 0) { obj_list_free(&l); Py_DECREF(fast); return NULL; }
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(CreateAcls, o, timeout_ms);
    kafka_admin_CreateAclsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_create_acls_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_CreateAclsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    obj_list_free(&l);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreateAcls), assemble_keyed_errors);
    if (job == NULL) { ffi_unraisable((kafka_admin_CreateAclsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_CreateAclsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_acl_binding, NULL, 0)); }
    return job_or_error(job, NULL);
}

// Admin_describe_acls(h, filter7, timeout_ms) -> (job, err).
// Payload: (error, [binding7]).
static PyObject* py_Admin_describe_acls(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* row; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &row, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_acl_AclBindingFilter_t* filter = acl_binding_filter_build(row);
    if (filter == NULL) return NULL;
    ADMIN_OPTIONS(DescribeAcls, o, timeout_ms);
    kafka_admin_DescribeAclsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_acls_with_options_err(a->admin, filter, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeAclsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_common_acl_AclBindingFilter_destroy_err(filter, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeAcls), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeAclsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    // `values()` is BORROWED from the result: destroyed with it, never alone.
    const kafka_common_KafkaFuture_t* values = NULL;
    JOB_TRY(job, ffi_raise((values = kafka_admin_DescribeAclsResult_values_err(r, &ffi_err), ffi_take(&ffi_err)))
        ? -1 : job_add_single(job, (kafka_common_KafkaFuture_t*)values, 0, conv_acl_bindings, 0));
    return job_or_error(job, NULL);
}

// Admin_delete_acls(h, [filter7], timeout_ms) -> (job, err).
// Payload: {filter7: (error, [(error, binding7)])}.
static PyObject* py_Admin_delete_acls(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "filters must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_acl_AclBindingFilter_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        kafka_common_acl_AclBindingFilter_t* f = acl_binding_filter_build(PySequence_Fast_GET_ITEM(fast, i));
        if (f == NULL || obj_list_add(&l, f) < 0) { obj_list_free(&l); Py_DECREF(fast); return NULL; }
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(DeleteAcls, o, timeout_ms);
    kafka_admin_DeleteAclsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_delete_acls_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DeleteAclsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    obj_list_free(&l);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteAcls), assemble_keyed_pairs);
    if (job == NULL) { ffi_unraisable((kafka_admin_DeleteAclsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DeleteAclsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_acl_binding_filter, conv_filter_results, 0)); }
    return job_or_error(job, NULL);
}

// ---- client quotas -------------------------------------------------------------------

// `DescribeClientQuotasRequest.MATCH_TYPE_*`: the match-type column of a
// filter-component row.
enum { QUOTA_MATCH_EXACT = 0, QUOTA_MATCH_DEFAULT = 1, QUOTA_MATCH_ANY = 2 };

// Admin_describe_client_quotas(h, [(entity_type, match_type, name | None)],
//                              strict, timeout_ms) -> (job, err).
// Payload: (error, {entity_pairs: [(quota_key, value)]}).
static PyObject* py_Admin_describe_client_quotas(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; int strict; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOpL", &h, &spec, &strict, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "components must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_quota_ClientQuotaFilterComponent_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char *type, *name; int match;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siz", &type, &match, &name)) { obj_list_free(&l); Py_DECREF(fast); return NULL; }
        kafka_common_quota_ClientQuotaFilterComponent_t* c = NULL;
        kafka_common_Error_t* cerr = NULL;
        switch (match) {
            case QUOTA_MATCH_EXACT:
                if (name == NULL) {
                    PyErr_SetString(PyExc_ValueError, "an exact-match quota filter component needs a name");
                    obj_list_free(&l); Py_DECREF(fast); return NULL;
                }
                cerr = (c = kafka_common_quota_ClientQuotaFilterComponent_of_entity_err(type, name, &ffi_err), ffi_take(&ffi_err));
                break;
            case QUOTA_MATCH_DEFAULT: cerr = (c = kafka_common_quota_ClientQuotaFilterComponent_of_default_entity_err(type, &ffi_err), ffi_take(&ffi_err)); break;
            case QUOTA_MATCH_ANY: cerr = (c = kafka_common_quota_ClientQuotaFilterComponent_of_entity_type_err(type, &ffi_err), ffi_take(&ffi_err)); break;
            default:
                PyErr_Format(PyExc_ValueError, "unknown quota filter match type %d", match);
                obj_list_free(&l); Py_DECREF(fast); return NULL;
        }
        if (ffi_raise(cerr) || obj_list_add(&l, c) < 0) { obj_list_free(&l); Py_DECREF(fast); return NULL; }
    }
    Py_DECREF(fast);
    // ClientQuotaFilter.contains / containsOnly copy the components; `all()`
    // is containsOnly of nothing, so the explicit forms cover it.
    kafka_common_quota_ClientQuotaFilter_t* filter = NULL;
    kafka_common_Error_t* ferr = strict
        ? (filter = kafka_common_quota_ClientQuotaFilter_contains_only_err(l.list, &ffi_err), ffi_take(&ffi_err))
        : (filter = kafka_common_quota_ClientQuotaFilter_contains_err(l.list, &ffi_err), ffi_take(&ffi_err));
    obj_list_free(&l);
    if (ffi_raise(ferr)) return NULL;
    ADMIN_OPTIONS(DescribeClientQuotas, o, timeout_ms);
    kafka_admin_DescribeClientQuotasResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_client_quotas_with_options_err(a->admin, filter, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeClientQuotasOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_common_quota_ClientQuotaFilter_destroy_err(filter, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeClientQuotas), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeClientQuotasResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    // `entities()` is BORROWED from the result.
    const kafka_common_KafkaFuture_t* entities = NULL;
    JOB_TRY(job, ffi_raise((entities = kafka_admin_DescribeClientQuotasResult_entities_err(r, &ffi_err), ffi_take(&ffi_err)))
        ? -1 : job_add_single(job, (kafka_common_KafkaFuture_t*)entities, 0, conv_quota_entities, 0));
    return job_or_error(job, NULL);
}

// Admin_alter_client_quotas(h, [([(entity_type, name | None)], [(key, value | None)])],
//                           timeout_ms, validate_only) -> (job, err).
// Payload: {entity_pairs: error}. A None op value is a removal (Java's null
// Double), which crosses as NaN.
static PyObject* py_Admin_alter_client_quotas(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &validate_only)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "entries must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_quota_ClientQuotaAlteration_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *pairs, *ops;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "OO", &pairs, &ops)) goto fail;
        PyObject* fpairs = fast_rows(pairs, "entity entries must be a sequence");
        if (fpairs == NULL) goto fail;
        // Borrowed char* -> char* | NULL, owned by `fpairs` for the call.
        kafka_Map_t* entries = NULL;
        if (ffi_raise((entries = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(fpairs); goto fail; }
        Py_ssize_t np = PySequence_Fast_GET_SIZE(fpairs);
        for (Py_ssize_t j = 0; j < np; j++) {
            const char *type, *name;
            if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fpairs, j), "sz", &type, &name)
                || ffi_raise((kafka_Map_put_err(entries, (void*)type, (void*)name, &ffi_err), ffi_take(&ffi_err)))) {
                ffi_unraisable((kafka_Map_destroy_err(entries, &ffi_err), ffi_take(&ffi_err))); Py_DECREF(fpairs); goto fail;
            }
        }
        kafka_common_quota_ClientQuotaEntity_t* entity = NULL;
        int entity_failed = ffi_raise((entity = kafka_common_quota_ClientQuotaEntity_new_err(entries, &ffi_err), ffi_take(&ffi_err)));  // copies
        ffi_unraisable((kafka_Map_destroy_err(entries, &ffi_err), ffi_take(&ffi_err)));
        Py_DECREF(fpairs);
        if (entity_failed) goto fail;
        PyObject* fops = fast_rows(ops, "ops must be a sequence");
        if (fops == NULL) { ffi_unraisable((kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err), ffi_take(&ffi_err))); goto fail; }
        Py_ssize_t no = PySequence_Fast_GET_SIZE(fops);
        obj_list_t opl;
        if (obj_list_init(&opl, no, (handle_destroy_fn)kafka_common_quota_ClientQuotaAlteration_Op_destroy_err) < 0) {
            Py_DECREF(fops); ffi_unraisable((kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err), ffi_take(&ffi_err))); goto fail;
        }
        for (Py_ssize_t j = 0; j < no; j++) {
            const char* key; PyObject* value;
            if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fops, j), "sO", &key, &value)) {
                obj_list_free(&opl); Py_DECREF(fops); ffi_unraisable((kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err), ffi_take(&ffi_err))); goto fail;
            }
            double d = NAN;
            if (value != Py_None) {
                d = PyFloat_AsDouble(value);
                if (d == -1.0 && PyErr_Occurred()) {
                    obj_list_free(&opl); Py_DECREF(fops); ffi_unraisable((kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err), ffi_take(&ffi_err))); goto fail;
                }
            }
            kafka_common_quota_ClientQuotaAlteration_Op_t* op = NULL;
            if (ffi_raise((op = kafka_common_quota_ClientQuotaAlteration_Op_new_err(key, d, &ffi_err), ffi_take(&ffi_err))) || obj_list_add(&opl, op) < 0) {
                obj_list_free(&opl); Py_DECREF(fops); ffi_unraisable((kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err), ffi_take(&ffi_err))); goto fail;
            }
        }
        Py_DECREF(fops);
        kafka_common_quota_ClientQuotaAlteration_t* alteration = NULL;
        int alteration_failed = ffi_raise(
            (alteration = kafka_common_quota_ClientQuotaAlteration_new_err(entity, opl.list, &ffi_err), ffi_take(&ffi_err)));  // copies entity and ops
        obj_list_free(&opl);
        ffi_unraisable((kafka_common_quota_ClientQuotaEntity_destroy_err(entity, &ffi_err), ffi_take(&ffi_err)));
        if (alteration_failed || obj_list_add(&l, alteration) < 0) goto fail;
    }
    Py_DECREF(fast);
    {
        ADMIN_OPTIONS(AlterClientQuotas, o, timeout_ms);
        FFI_STEP(o_err, (kafka_admin_AlterClientQuotasOptions_set_validate_only_err(o, (int8_t)validate_only, &ffi_err), ffi_take(&ffi_err)));
        kafka_admin_AlterClientQuotasResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_alter_client_quotas_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_AlterClientQuotasOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        obj_list_free(&l);
        if (ffi_raise(o_err)) return NULL;
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterClientQuotas), assemble_keyed_errors);
        if (job == NULL) { ffi_unraisable((kafka_admin_AlterClientQuotasResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_AlterClientQuotasResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_quota_entity, NULL, 0)); }
        return job_or_error(job, NULL);
    }
fail:
    obj_list_free(&l);
    Py_DECREF(fast);
    return NULL;
}

// ---- SCRAM --------------------------------------------------------------------------

// Java's three views of describeUserScramCredentials, composed into the
// per-user shape the way the Rust test backend (rust/tests/common/
// admin_backend.rs) and the C gRPC server (c/grpc_server/server.cc) compose
// them, so every backend answers with the same key set and the same errors:
//
//   - all() (tag 0) succeeds only when every user's error is NONE or
//     RESOURCE_NOT_FOUND, and then its keys are the complete user set, a user
//     the broker reports as having no credential described with an empty list
//     rather than as an error;
//   - when it fails, users() (tag 2) still lists every user whose error is not
//     RESOURCE_NOT_FOUND -- necessarily including the one that failed all() --
//     and description(user) (tag 1) yields that user's own outcome;
//   - if that leaves no row at all, all()'s error is the whole-call error
//     rather than an empty, successful-looking result.
//
// description(user) alone is not enough: for a user with no credential it
// fails with RESOURCE_NOT_FOUND, which all() does not treat as an error.
enum { SCRAM_ALL = 0, SCRAM_DESCRIPTION = 1, SCRAM_USERS = 2 };

// all()'s Map<String, UserScramCredentialsDescription> ->
// {user: (None, [(mechanism, iterations)])}, in the map's (sorted) order.
static PyObject* conv_scram_all(AdminJob* job, AdminSlot* slot, void* v) {
    kafka_common_Error_t *ffi_err = NULL;
    const kafka_Map_t* m = (const kafka_Map_t*)v;
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int32_t n = 0;
    if (m != NULL && ffi_raise((n = kafka_Map_size_err(m, &ffi_err), ffi_take(&ffi_err)))) { Py_DECREF(d); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        void* k = NULL; void* value = NULL;
        if (ffi_raise((k = kafka_Map_key_err(m, i, &ffi_err), ffi_take(&ffi_err))) || ffi_raise((value = kafka_Map_value_err(m, i, &ffi_err), ffi_take(&ffi_err)))) {
            Py_DECREF(d); return NULL;
        }
        PyObject* user = PyUnicode_FromString((const char*)k);
        if (user == NULL) { Py_DECREF(d); return NULL; }
        PyObject* infos = conv_scram_description(job, slot, value);
        PyObject* pair = infos ? Py_BuildValue("(ON)", Py_None, infos) : NULL;
        if (pair == NULL || PyDict_SetItem(d, user, pair) < 0) {
            Py_XDECREF(pair); Py_DECREF(user); Py_DECREF(d); return NULL;
        }
        Py_DECREF(pair); Py_DECREF(user);
    }
    return d;
}

// Third stage, after users() delivered: one owned `description(user)` future
// per listed user, keyed by it.
static int cont_scram_users(AdminJob* job, AdminSlot* slot) {
    kafka_common_Error_t *ffi_err = NULL;
    if (slot->error != NULL) return 0;  // users() failed: nothing to describe
    PyObject* users = slot->value;      // [str] from conv_str_list
    if (!PyList_Check(users)) return 0;
    const kafka_admin_DescribeUserScramCredentialsResult_t* r =
        (const kafka_admin_DescribeUserScramCredentialsResult_t*)job->result;
    Py_ssize_t n = PyList_GET_SIZE(users);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* user = PyList_GET_ITEM(users, i);
        const char* name = PyUnicode_AsUTF8(user);
        if (name == NULL) return -1;
        kafka_common_KafkaFuture_t* f = NULL;
        if (ffi_raise((f = kafka_admin_DescribeUserScramCredentialsResult_description_err(r, name, &ffi_err), ffi_take(&ffi_err)))) return -1;
        Py_INCREF(user);
        if (job_add_slot(job, f, 1, user, conv_scram_description, NULL, SCRAM_DESCRIPTION) == NULL) return -1;
    }
    return 0;
}

// Second stage, after all() delivered: nothing more when it succeeded;
// otherwise the users() fallback.
static int cont_scram_all(AdminJob* job, AdminSlot* slot) {
    kafka_common_Error_t *ffi_err = NULL;
    if (slot->error == NULL) return 0;
    const kafka_admin_DescribeUserScramCredentialsResult_t* r =
        (const kafka_admin_DescribeUserScramCredentialsResult_t*)job->result;
    kafka_common_KafkaFuture_t* f = NULL;
    if (ffi_raise((f = kafka_admin_DescribeUserScramCredentialsResult_users_err(r, &ffi_err), ffi_take(&ffi_err)))) return -1;
    if (job_add_slot(job, f, 1, NULL, conv_str_list, cont_scram_users, SCRAM_USERS) == NULL) return -1;
    return 0;
}

// (error | None, {user: (error | None, [(mechanism, iterations)])}), composed
// as described above.
static PyObject* assemble_scram(AdminJob* job) {
    AdminSlot* all = job->slots[0];
    if (all->error == NULL) return Py_BuildValue("(OO)", Py_None, all->value);
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    PyObject* err = NULL;
    for (int i = 1; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        if (s->tag == SCRAM_USERS) { if (s->error) err = s->error; continue; }
        PyObject* pair = Py_BuildValue("(OO)", s->error ? s->error : Py_None, s->value);
        if (pair == NULL || PyDict_SetItem(d, s->key, pair) < 0) { Py_XDECREF(pair); Py_DECREF(d); return NULL; }
        Py_DECREF(pair);
    }
    if (err == NULL && PyDict_Size(d) == 0) err = all->error;
    return Py_BuildValue("(ON)", err ? err : Py_None, d);
}

// Admin_describe_user_scram_credentials(h, [user], timeout_ms) -> (job, err).
// An empty list is Java's describe-all. Payload: see assemble_scram.
static PyObject* py_Admin_describe_user_scram_credentials(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* users; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &users, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(users, &l, "users must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeUserScramCredentials, o, timeout_ms);
    kafka_admin_DescribeUserScramCredentialsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_user_scram_credentials_with_users_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeUserScramCredentialsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    str_list_free(&l);
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeUserScramCredentials), assemble_scram);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeUserScramCredentialsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    kafka_common_KafkaFuture_t* all = NULL;
    if (ffi_raise((all = kafka_admin_DescribeUserScramCredentialsResult_all_err(r, &ffi_err), ffi_take(&ffi_err)))
        || job_add_slot(job, all, 1, NULL, conv_scram_all, cont_scram_all, SCRAM_ALL) == NULL) {
        job_free(job); return NULL;
    }
    return job_or_error(job, NULL);
}

// Admin_alter_user_scram_credentials(h,
//     [(user, is_deletion, mechanism_type, iterations, password | None, salt | None)],
//     timeout_ms) -> (job, err). Payload: {user: error}.
static PyObject* py_Admin_alter_user_scram_credentials(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)self;
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "alterations must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_admin_UserScramCredentialAlteration_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* user; int is_deletion, mech, iterations; PyObject *password, *salt;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "spiiOO", &user, &is_deletion, &mech, &iterations, &password, &salt)) goto fail;
        const kafka_admin_ScramMechanism_t* mechanism = NULL;
        if (ffi_raise((mechanism = kafka_admin_ScramMechanism_from_type_err((int8_t)mech, &ffi_err), ffi_take(&ffi_err)))) goto fail;
        if (mechanism == NULL) {
            PyErr_Format(PyExc_ValueError, "unknown SCRAM mechanism type %d", mech);
            goto fail;
        }
        kafka_admin_UserScramCredentialAlteration_t* alteration = NULL;
        kafka_common_Error_t* e = NULL;
        if (is_deletion) {
            kafka_admin_UserScramCredentialDeletion_t* del = NULL;
            e = (del = kafka_admin_UserScramCredentialDeletion_new_err(user, mechanism, &ffi_err), ffi_take(&ffi_err));
            FFI_STEP(e, (alteration = kafka_admin_UserScramCredentialAlteration_deletion_err(del, &ffi_err), ffi_take(&ffi_err)));  // copies
            if (del) ffi_unraisable((kafka_admin_UserScramCredentialDeletion_destroy_err(del, &ffi_err), ffi_take(&ffi_err)));
        } else {
            Py_buffer pw, sa;
            if (PyObject_GetBuffer(password, &pw, PyBUF_SIMPLE) < 0) goto fail;
            int32_t pw_len;
            if (checked_len_to_int32(pw.len, "password", &pw_len) < 0) { PyBuffer_Release(&pw); goto fail; }
            kafka_Bytes_t pw_bytes = { (const uint8_t*)pw.buf, pw_len };
            kafka_admin_ScramCredentialInfo_t* info = NULL;
            e = (info = kafka_admin_ScramCredentialInfo_new_err(mechanism, (int32_t)iterations, &ffi_err), ffi_take(&ffi_err));
            kafka_admin_UserScramCredentialUpsertion_t* up = NULL;
            if (salt == Py_None) {
                // Java's three-argument constructor: a salt is generated.
                FFI_STEP(e, (up = kafka_admin_UserScramCredentialUpsertion_with_bytes_err(user, info, pw_bytes, &ffi_err), ffi_take(&ffi_err)));
            } else {
                if (PyObject_GetBuffer(salt, &sa, PyBUF_SIMPLE) < 0) {
                    PyBuffer_Release(&pw);
                    if (info) ffi_unraisable((kafka_admin_ScramCredentialInfo_destroy_err(info, &ffi_err), ffi_take(&ffi_err)));
                    ffi_unraisable(e); goto fail;
                }
                int32_t salt_len;
                if (checked_len_to_int32(sa.len, "salt", &salt_len) < 0) {
                    PyBuffer_Release(&sa); PyBuffer_Release(&pw);
                    if (info) ffi_unraisable((kafka_admin_ScramCredentialInfo_destroy_err(info, &ffi_err), ffi_take(&ffi_err)));
                    ffi_unraisable(e); goto fail;
                }
                kafka_Bytes_t salt_bytes = { (const uint8_t*)sa.buf, salt_len };
                FFI_STEP(e, (up = kafka_admin_UserScramCredentialUpsertion_with_salt_err(user, info, pw_bytes, salt_bytes, &ffi_err), ffi_take(&ffi_err)));
                PyBuffer_Release(&sa);
            }
            PyBuffer_Release(&pw);
            if (info) ffi_unraisable((kafka_admin_ScramCredentialInfo_destroy_err(info, &ffi_err), ffi_take(&ffi_err)));
            FFI_STEP(e, (alteration = kafka_admin_UserScramCredentialAlteration_upsertion_err(up, &ffi_err), ffi_take(&ffi_err)));  // copies
            if (up) ffi_unraisable((kafka_admin_UserScramCredentialUpsertion_destroy_err(up, &ffi_err), ffi_take(&ffi_err)));
        }
        if (ffi_raise(e)) {
            if (alteration) ffi_unraisable((kafka_admin_UserScramCredentialAlteration_destroy_err(alteration, &ffi_err), ffi_take(&ffi_err)));
            goto fail;
        }
        if (obj_list_add(&l, alteration) < 0) goto fail;
    }
    Py_DECREF(fast);
    {
        ADMIN_OPTIONS(AlterUserScramCredentials, o, timeout_ms);
        kafka_admin_AlterUserScramCredentialsResult_t* r = NULL;
        FFI_STEP(o_err, (r = kafka_admin_Admin_alter_user_scram_credentials_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
        if (o) ffi_unraisable((kafka_admin_AlterUserScramCredentialsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
        obj_list_free(&l);
        if (ffi_raise(o_err)) return NULL;
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterUserScramCredentials), assemble_keyed_errors);
        if (job == NULL) { ffi_unraisable((kafka_admin_AlterUserScramCredentialsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
        { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_AlterUserScramCredentialsResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, NULL, 0)); }
        return job_or_error(job, NULL);
    }
fail:
    obj_list_free(&l);
    Py_DECREF(fast);
    return NULL;
}

// ---- delegation tokens ----------------------------------------------------------------

// Admin_create_delegation_token(h, [(type, name)], owner_type | None, owner_name | None,
//                               max_lifetime_ms, timeout_ms) -> (job, err).
// Payload: (error, token9).
static PyObject* py_Admin_create_delegation_token(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* renewers; const char *owner_type, *owner_name; long long max_lifetime_ms, timeout_ms;
    if (!PyArg_ParseTuple(args, "KOzzLL", &h, &renewers, &owner_type, &owner_name, &max_lifetime_ms, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    principal_list_t l;
    if (principal_list_build(renewers, &l) < 0) return NULL;
    ADMIN_OPTIONS(CreateDelegationToken, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_CreateDelegationTokenOptions_set_renewers_err(o, l.list, &ffi_err), ffi_take(&ffi_err)));  // copied
    principal_list_free(&l);
    if (owner_type != NULL && owner_name != NULL) {
        kafka_common_security_auth_KafkaPrincipal_t* owner = NULL;
        FFI_STEP(o_err, (owner = kafka_common_security_auth_KafkaPrincipal_new_err(owner_type, owner_name, &ffi_err), ffi_take(&ffi_err)));
        FFI_STEP(o_err, (kafka_admin_CreateDelegationTokenOptions_set_owner_err(o, owner, &ffi_err), ffi_take(&ffi_err)));  // copied
        if (owner) ffi_unraisable((kafka_common_security_auth_KafkaPrincipal_destroy_err(owner, &ffi_err), ffi_take(&ffi_err)));
    }
    FFI_STEP(o_err, (kafka_admin_CreateDelegationTokenOptions_set_max_lifetime_ms_err(o, (int64_t)max_lifetime_ms, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_CreateDelegationTokenResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_create_delegation_token_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_CreateDelegationTokenOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreateDelegationToken), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_CreateDelegationTokenResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    // `delegation_token()` is BORROWED from the result.
    { const kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_CreateDelegationTokenResult_delegation_token_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, (kafka_common_KafkaFuture_t*)v_, 0, conv_delegation_token, 0)); }
    return job_or_error(job, NULL);
}

// Admin_renew_delegation_token(h, hmac, renew_time_period_ms, timeout_ms) -> (job, err).
// Payload: (error, expiry_timestamp).
static PyObject* py_Admin_renew_delegation_token(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)self;
    unsigned long long h; const uint8_t* hmac; Py_ssize_t hmac_len; long long period_ms, timeout_ms;
    if (!PyArg_ParseTuple(args, "Ky#LL", &h, &hmac, &hmac_len, &period_ms, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    int32_t hmac_len32;
    if (checked_len_to_int32(hmac_len, "hmac", &hmac_len32) < 0) return NULL;
    kafka_Bytes_t bytes = { hmac, hmac_len32 };
    ADMIN_OPTIONS(RenewDelegationToken, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_RenewDelegationTokenOptions_set_renew_time_period_ms_err(o, (int64_t)period_ms, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_RenewDelegationTokenResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_renew_delegation_token_with_options_err(a->admin, bytes, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_RenewDelegationTokenOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(RenewDelegationToken), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_RenewDelegationTokenResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { const kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_RenewDelegationTokenResult_expiry_timestamp_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, (kafka_common_KafkaFuture_t*)v_, 0, conv_i64_ptr, 0)); }
    return job_or_error(job, NULL);
}

// Admin_expire_delegation_token(h, hmac, expiry_time_period_ms, timeout_ms) -> (job, err).
// Payload: (error, expiry_timestamp).
static PyObject* py_Admin_expire_delegation_token(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    (void)self;
    unsigned long long h; const uint8_t* hmac; Py_ssize_t hmac_len; long long period_ms, timeout_ms;
    if (!PyArg_ParseTuple(args, "Ky#LL", &h, &hmac, &hmac_len, &period_ms, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    int32_t hmac_len32;
    if (checked_len_to_int32(hmac_len, "hmac", &hmac_len32) < 0) return NULL;
    kafka_Bytes_t bytes = { hmac, hmac_len32 };
    ADMIN_OPTIONS(ExpireDelegationToken, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_ExpireDelegationTokenOptions_set_expiry_time_period_ms_err(o, (int64_t)period_ms, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_ExpireDelegationTokenResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_expire_delegation_token_with_options_err(a->admin, bytes, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_ExpireDelegationTokenOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ExpireDelegationToken), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_ExpireDelegationTokenResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { const kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_ExpireDelegationTokenResult_expiry_timestamp_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, (kafka_common_KafkaFuture_t*)v_, 0, conv_i64_ptr, 0)); }
    return job_or_error(job, NULL);
}

// Admin_describe_delegation_token(h, has_owners, [(type, name)], timeout_ms) -> (job, err).
// `has_owners` false is Java's null owners (every token). Payload: (error, [token9]).
static PyObject* py_Admin_describe_delegation_token(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; int has_owners; PyObject* owners; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KpOL", &h, &has_owners, &owners, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(DescribeDelegationToken, o, timeout_ms);
    if (has_owners) {
        principal_list_t l;
        if (principal_list_build(owners, &l) < 0) {
            if (o) ffi_unraisable((kafka_admin_DescribeDelegationTokenOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
            ffi_unraisable(o_err);
            return NULL;
        }
        FFI_STEP(o_err, (kafka_admin_DescribeDelegationTokenOptions_set_owners_err(o, l.list, &ffi_err), ffi_take(&ffi_err)));  // copied
        principal_list_free(&l);
    }
    kafka_admin_DescribeDelegationTokenResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_delegation_token_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeDelegationTokenOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeDelegationToken), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeDelegationTokenResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { const kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeDelegationTokenResult_delegation_tokens_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, (kafka_common_KafkaFuture_t*)v_, 0, conv_delegation_tokens, 0)); }
    return job_or_error(job, NULL);
}

// ---- features -------------------------------------------------------------------------

// Admin_describe_features(h, has_node_id, node_id, timeout_ms) -> (job, err).
// Payload: (error, ([(feature, min, max)], epoch | None, [(feature, min, max)])).
static PyObject* py_Admin_describe_features(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; int has_node_id, node_id; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KpiL", &h, &has_node_id, &node_id, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(DescribeFeatures, o, timeout_ms);
    if (has_node_id) FFI_STEP(o_err, (kafka_admin_DescribeFeaturesOptions_set_node_id_err(o, (int32_t)node_id, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DescribeFeaturesResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_features_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeFeaturesOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeFeatures), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_DescribeFeaturesResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_DescribeFeaturesResult_feature_metadata_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, conv_feature_metadata, 0)); }
    return job_or_error(job, NULL);
}

// Admin_update_features(h, [(feature, max_version_level, upgrade_type_code)],
//                       timeout_ms, validate_only) -> (job, err).
// Payload: {feature: error}. Java's updateFeatures validates its argument up
// front (empty map, empty feature name), which surfaces as the call error.
static PyObject* py_Admin_update_features(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &validate_only)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "feature_updates must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;  // borrowed char* (owned by fast) -> owned FeatureUpdate*
    if (obj_map_init(&m, n, NULL, (handle_destroy_fn)kafka_admin_FeatureUpdate_destroy_err) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* feature; int level, upgrade;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "sii", &feature, &level, &upgrade)) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
        const kafka_admin_FeatureUpdate_UpgradeType_t* upgrade_type = NULL;
        if (ffi_raise((upgrade_type = kafka_admin_FeatureUpdate_UpgradeType_from_code_err((int32_t)upgrade, &ffi_err), ffi_take(&ffi_err)))) {
            obj_map_free(&m); Py_DECREF(fast); return NULL;
        }
        kafka_admin_FeatureUpdate_t* fu = NULL;
        kafka_common_Error_t* err = (fu = kafka_admin_FeatureUpdate_new((int16_t)level, upgrade_type, &ffi_err), ffi_take(&ffi_err));
        if (err != NULL) { obj_map_free(&m); Py_DECREF(fast); return job_or_error(NULL, indexed_error("feature update", i, err)); }
        if (obj_map_put(&m, (void*)feature, fu) < 0) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
    }
    ADMIN_OPTIONS(UpdateFeatures, o, timeout_ms);
    FFI_STEP(o_err, (kafka_admin_UpdateFeaturesOptions_set_validate_only_err(o, (int8_t)validate_only, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_UpdateFeaturesResult_t* r = NULL;
    // The RPC's own error is the call error; it runs only when the options
    // steps succeeded.
    kafka_common_Error_t* err = NULL;
    if (o_err == NULL) err = (r = kafka_admin_Admin_update_features_with_options(a->admin, m.map, o, &ffi_err), ffi_take(&ffi_err));
    if (o) ffi_unraisable((kafka_admin_UpdateFeaturesOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    obj_map_free(&m);
    Py_DECREF(fast);
    if (ffi_raise(o_err)) return NULL;
    if (err != NULL) return job_or_error(NULL, err);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(UpdateFeatures), assemble_keyed_errors);
    if (job == NULL) { ffi_unraisable((kafka_admin_UpdateFeaturesResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_UpdateFeaturesResult_values_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, NULL, 0)); }
    return job_or_error(job, NULL);
}

// ---- producers and transactions -----------------------------------------------------------

// Admin_describe_producers(h, [(topic, partition)], has_broker_id, broker_id, timeout_ms)
// -> (job, err). Payload: {(topic, partition): (error, [state6])}.
static PyObject* py_Admin_describe_producers(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec; int has_broker_id, broker_id; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOpiL", &h, &spec, &has_broker_id, &broker_id, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tp_list_t tps;
    if (tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(DescribeProducers, o, timeout_ms);
    if (has_broker_id) FFI_STEP(o_err, (kafka_admin_DescribeProducersOptions_set_broker_id_err(o, (int32_t)broker_id, &ffi_err), ffi_take(&ffi_err)));
    kafka_admin_DescribeProducersResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_producers_with_options_err(a->admin, tps.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeProducersOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) { tp_list_free(&tps); return NULL; }
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeProducers), assemble_keyed_pairs);
    if (job == NULL) { tp_list_free(&tps); ffi_unraisable((kafka_admin_DescribeProducersResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    for (Py_ssize_t i = 0; i < tps.n; i++) {
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = (f = kafka_admin_DescribeProducersResult_partition_result(r, tps.tps[i], &ffi_err), ffi_take(&ffi_err));
        if (perr != NULL) { tp_list_free(&tps); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = tp_to_py(tps.tps[i]);
        if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); tp_list_free(&tps); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, conv_partition_producer_state, NULL, 0) == NULL) { tp_list_free(&tps); job_free(job); return NULL; }
    }
    tp_list_free(&tps);
    return job_or_error(job, NULL);
}

// Admin_describe_transactions(h, [transactional_id], timeout_ms) -> (job, err).
// Payload: {transactional_id: (error, description7)}.
static PyObject* py_Admin_describe_transactions(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &ids, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "transactional_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeTransactions, o, timeout_ms);
    kafka_admin_DescribeTransactionsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_describe_transactions_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_DescribeTransactionsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) { str_list_free(&l); return NULL; }
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeTransactions), assemble_keyed_pairs);
    if (job == NULL) { str_list_free(&l); ffi_unraisable((kafka_admin_DescribeTransactionsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    // The ids of `l.list`, read back from the str objects that own them.
    Py_ssize_t n = PySequence_Fast_GET_SIZE(l.seq);
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* tid = PyUnicode_AsUTF8(PySequence_Fast_GET_ITEM(l.seq, i));
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = (f = kafka_admin_DescribeTransactionsResult_description(r, tid, &ffi_err), ffi_take(&ffi_err));
        if (perr != NULL) { str_list_free(&l); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = key_str(tid);
        if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); str_list_free(&l); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, conv_transaction_description, NULL, 0) == NULL) { str_list_free(&l); job_free(job); return NULL; }
    }
    str_list_free(&l);
    return job_or_error(job, NULL);
}

// fenceProducers: per transactional id, the void `fencedProducers()` future
// (tag 0) and the `producerId` / `epochId` refinements (tags 1 / 2), all keyed
// by the id. Payload: {transactional_id: (error, (producer_id | -1, epoch | -1))}.
enum { FP_FENCED = 0, FP_PRODUCER_ID = 1, FP_EPOCH = 2 };

static PyObject* assemble_fence_producers(AdminJob* job) {
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    for (int i = 0; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        if (s->tag != FP_FENCED) continue;
        PyObject* pid = NULL; PyObject* epoch = NULL;
        for (int j = 0; j < job->n; j++) {
            AdminSlot* t = job->slots[j];
            if (t->key == NULL || PyObject_RichCompareBool(t->key, s->key, Py_EQ) != 1) continue;
            if (t->tag == FP_PRODUCER_ID) pid = t->error ? NULL : t->value;
            else if (t->tag == FP_EPOCH) epoch = t->error ? NULL : t->value;
        }
        // A failed refinement reports -1, Java's RecordBatch.NO_PRODUCER_ID /
        // NO_PRODUCER_EPOCH sentinels, beside the error.
        PyObject* pair = (pid && epoch)
            ? Py_BuildValue("(O(OO))", s->error ? s->error : Py_None, pid, epoch)
            : Py_BuildValue("(O(ii))", s->error ? s->error : Py_None, -1, -1);
        if (pair == NULL || PyDict_SetItem(d, s->key, pair) < 0) { Py_XDECREF(pair); Py_DECREF(d); return NULL; }
        Py_DECREF(pair);
    }
    return d;
}

// Admin_fence_producers(h, [transactional_id], timeout_ms) -> (job, err).
static PyObject* py_Admin_fence_producers(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &ids, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "transactional_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(FenceProducers, o, timeout_ms);
    kafka_admin_FenceProducersResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_fence_producers_with_options_err(a->admin, l.list, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_FenceProducersOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) { str_list_free(&l); return NULL; }
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(FenceProducers), assemble_fence_producers);
    if (job == NULL) { str_list_free(&l); ffi_unraisable((kafka_admin_FenceProducersResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_Map_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_FenceProducersResult_fenced_producers_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_map(job, v_, key_str, NULL, FP_FENCED)); }
    // The ids of `l.list`, read back from the str objects that own them.
    Py_ssize_t n = PySequence_Fast_GET_SIZE(l.seq);
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* tid = PyUnicode_AsUTF8(PySequence_Fast_GET_ITEM(l.seq, i));
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = (f = kafka_admin_FenceProducersResult_producer_id(r, tid, &ffi_err), ffi_take(&ffi_err));
        if (perr != NULL) { str_list_free(&l); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = key_str(tid);
        if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); str_list_free(&l); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, conv_i64_ptr, NULL, FP_PRODUCER_ID) == NULL) { str_list_free(&l); job_free(job); return NULL; }
        f = NULL;
        perr = (f = kafka_admin_FenceProducersResult_epoch_id(r, tid, &ffi_err), ffi_take(&ffi_err));
        if (perr != NULL) { str_list_free(&l); job_free(job); return job_or_error(NULL, perr); }
        key = key_str(tid);
        if (key == NULL) { ffi_unraisable((kafka_common_KafkaFuture_destroy_err(f, &ffi_err), ffi_take(&ffi_err))); str_list_free(&l); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, conv_i16_ptr, NULL, FP_EPOCH) == NULL) { str_list_free(&l); job_free(job); return NULL; }
    }
    str_list_free(&l);
    return job_or_error(job, NULL);
}

// listTransactions: `byBrokerId()` (tag 0) delivers a map of broker id ->
// KafkaFuture<List<TransactionListing>>, BORROWED from the outer future (and
// destroyed with it, which the job does last); the continuation re-reads the
// completed outer future and adds one borrowed slot per broker (tag 1).
// Slots are added from the continuation, not the converter, so the async path
// dispatches them (slot_resolve only dispatches what `cont` added).
static int cont_list_transactions(AdminJob* job, AdminSlot* slot) {
    kafka_common_Error_t *ffi_err = NULL;
    if (slot->error != NULL) return 0;  // byBrokerId() failed: no per-broker futures
    void* value = NULL;
    kafka_common_Error_t* err = (value = kafka_common_KafkaFuture_get(slot->future, &ffi_err), ffi_take(&ffi_err));  // completed: immediate
    if (err != NULL) { ffi_unraisable((kafka_common_Error_destroy_err(err, &ffi_err), ffi_take(&ffi_err))); return 0; }
    const kafka_Map_t* map = (const kafka_Map_t*)value;
    int32_t n = 0;
    if (map != NULL && ffi_raise((n = kafka_Map_size_err(map, &ffi_err), ffi_take(&ffi_err)))) return -1;
    for (int32_t i = 0; i < n; i++) {
        void* k = NULL; void* f = NULL;
        if (ffi_raise((k = kafka_Map_key_err(map, i, &ffi_err), ffi_take(&ffi_err))) || ffi_raise((f = kafka_Map_value_err(map, i, &ffi_err), ffi_take(&ffi_err)))) return -1;
        PyObject* key = key_i32(k);
        if (key == NULL) return -1;
        if (job_add_slot(job, (kafka_common_KafkaFuture_t*)f, 0, key, conv_transaction_listings, NULL, 1) == NULL) return -1;
    }
    return 0;
}

// (by_broker_id_error | None, {broker_id: (error, [listing3])})
static PyObject* assemble_list_transactions(AdminJob* job) {
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    PyObject* err = Py_None;
    for (int i = 0; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        if (s->tag == 0) { if (s->error) err = s->error; continue; }
        PyObject* pair = Py_BuildValue("(OO)", s->error ? s->error : Py_None, s->value);
        if (pair == NULL || PyDict_SetItem(d, s->key, pair) < 0) { Py_XDECREF(pair); Py_DECREF(d); return NULL; }
        Py_DECREF(pair);
    }
    return Py_BuildValue("(ON)", err, d);
}

// Admin_list_transactions(h, [state_name], [producer_id], duration_ms,
//                         pattern | None, timeout_ms) -> (job, err).
static PyObject* py_Admin_list_transactions(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject *states, *pids; long long duration_ms, timeout_ms; const char* pattern;
    if (!PyArg_ParseTuple(args, "KOOLzL", &h, &states, &pids, &duration_ms, &pattern, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t s;
    if (str_list_build_msg(states, &s, "states must be a sequence of str") < 0) return NULL;
    i64_list_t p;
    if (i64_list_build(pids, &p, "producer_ids must be a sequence of int") < 0) { str_list_free(&s); return NULL; }
    ADMIN_OPTIONS(ListTransactions, o, timeout_ms);
    // The state names of `s.list`, read back from the str objects that own them.
    Py_ssize_t n = PySequence_Fast_GET_SIZE(s.seq);
    if (n > 0 && o_err == NULL) {
        kafka_List_t* singletons = NULL;
        o_err = (singletons = kafka_List_new_err(&ffi_err), ffi_take(&ffi_err));
        for (Py_ssize_t i = 0; i < n && o_err == NULL; i++) {
            const char* name = PyUnicode_AsUTF8(PySequence_Fast_GET_ITEM(s.seq, i));
            const kafka_admin_TransactionState_t* st = NULL;
            o_err = (st = kafka_admin_TransactionState_parse_err(name, &ffi_err), ffi_take(&ffi_err));
            if (o_err == NULL && st == NULL) {
                PyErr_Format(PyExc_ValueError, "unknown transaction state %s", name);
                if (singletons) ffi_unraisable((kafka_List_destroy_err(singletons, &ffi_err), ffi_take(&ffi_err)));
                if (o) ffi_unraisable((kafka_admin_ListTransactionsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
                str_list_free(&s); i64_list_free(&p); return NULL;
            }
            FFI_STEP(o_err, (kafka_List_add_err(singletons, (void*)st, &ffi_err), ffi_take(&ffi_err)));
        }
        FFI_STEP(o_err, (kafka_admin_ListTransactionsOptions_filter_states_err(o, singletons, &ffi_err), ffi_take(&ffi_err)));
        if (singletons) ffi_unraisable((kafka_List_destroy_err(singletons, &ffi_err), ffi_take(&ffi_err)));
    }
    if (p.n > 0) FFI_STEP(o_err, (kafka_admin_ListTransactionsOptions_filter_producer_ids_err(o, p.list, &ffi_err), ffi_take(&ffi_err)));
    FFI_STEP(o_err, (kafka_admin_ListTransactionsOptions_filter_on_duration_err(o, (int64_t)duration_ms, &ffi_err), ffi_take(&ffi_err)));
    if (pattern != NULL) FFI_STEP(o_err, (kafka_admin_ListTransactionsOptions_filter_on_transactional_id_pattern_err(o, pattern, &ffi_err), ffi_take(&ffi_err)));
    str_list_free(&s); i64_list_free(&p);
    kafka_admin_ListTransactionsResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_list_transactions_with_options_err(a->admin, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_ListTransactionsOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListTransactions), assemble_list_transactions);
    if (job == NULL) { ffi_unraisable((kafka_admin_ListTransactionsResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    kafka_common_KafkaFuture_t* by_broker_id = NULL;
    if (ffi_raise((by_broker_id = kafka_admin_ListTransactionsResult_by_broker_id_err(r, &ffi_err), ffi_take(&ffi_err)))
        || job_add_slot(job, by_broker_id, 1, NULL, NULL, cont_list_transactions, 0) == NULL) {
        job_free(job); return NULL;
    }
    return job_or_error(job, NULL);
}

// Admin_abort_transaction(h, topic, partition, producer_id, producer_epoch,
//                         coordinator_epoch, timeout_ms) -> (job, err).
// Payload: (error, None).
static PyObject* py_Admin_abort_transaction(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* topic; int partition, coordinator_epoch, producer_epoch; long long producer_id, timeout_ms;
    if (!PyArg_ParseTuple(args, "KsiLiiL", &h, &topic, &partition, &producer_id, &producer_epoch, &coordinator_epoch, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_TopicPartition_t* tp = NULL;
    kafka_admin_AbortTransactionSpec_t* spec = NULL;
    kafka_common_Error_t* spec_err = (tp = kafka_common_TopicPartition_new_err(topic, (int32_t)partition, &ffi_err), ffi_take(&ffi_err));
    FFI_STEP(spec_err, (spec = kafka_admin_AbortTransactionSpec_new_err(
        tp, (int64_t)producer_id, (int16_t)producer_epoch, (int32_t)coordinator_epoch, &ffi_err), ffi_take(&ffi_err)));  // copies tp
    if (tp) ffi_unraisable((kafka_common_TopicPartition_destroy_err(tp, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(spec_err)) return NULL;
    ADMIN_OPTIONS(AbortTransaction, o, timeout_ms);
    kafka_admin_AbortTransactionResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_abort_transaction_with_options_err(a->admin, spec, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_AbortTransactionOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    ffi_unraisable((kafka_admin_AbortTransactionSpec_destroy_err(spec, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AbortTransaction), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_AbortTransactionResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_AbortTransactionResult_all_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, NULL, 0)); }
    return job_or_error(job, NULL);
}

// Admin_unregister_controller(h, controller_id, timeout_ms) -> (job, err).
// Payload: (error, None).
static PyObject* py_Admin_unregister_controller(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; int controller_id; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KiL", &h, &controller_id, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(UnregisterController, o, timeout_ms);
    kafka_admin_UnregisterControllerResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_unregister_controller_with_options_err(a->admin, (int32_t)controller_id, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_UnregisterControllerOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(UnregisterController), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_UnregisterControllerResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_UnregisterControllerResult_all_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, NULL, 0)); }
    return job_or_error(job, NULL);
}

// Admin_force_terminate_transaction(h, transactional_id, timeout_ms) -> (job, err).
// Payload: (error, None).
static PyObject* py_Admin_force_terminate_transaction(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; const char* tid; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KsL", &h, &tid, &timeout_ms)) return NULL;
    if (!admin_timeout_fits(timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(TerminateTransaction, o, timeout_ms);
    kafka_admin_TerminateTransactionResult_t* r = NULL;
    FFI_STEP(o_err, (r = kafka_admin_Admin_force_terminate_transaction_with_options_err(a->admin, tid, o, &ffi_err), ffi_take(&ffi_err)));
    if (o) ffi_unraisable((kafka_admin_TerminateTransactionOptions_destroy_err(o, &ffi_err), ffi_take(&ffi_err)));
    if (ffi_raise(o_err)) return NULL;
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(TerminateTransaction), assemble_single);
    if (job == NULL) { ffi_unraisable((kafka_admin_TerminateTransactionResult_destroy_err(r, &ffi_err), ffi_take(&ffi_err))); return NULL; }
    { kafka_common_KafkaFuture_t * v_ = NULL; JOB_TRY(job, ffi_raise((v_ = kafka_admin_TerminateTransactionResult_result_err(r, &ffi_err), ffi_take(&ffi_err))) ? -1 : job_add_single(job, v_, 1, NULL, 0)); }
    return job_or_error(job, NULL);
}

// ---- MockAdminClient drivers ---------------------------------------------------------------
//
// Each returns an owned kafka_common_Error_t* as an int (0 = ok), the setter's
// own `_err` result included; a handle that is not a mock gets an
// illegal-argument error, as the setters only exist on the concrete
// MockAdminClient.

// The owned error `e` (NULL = ok) as the int the drivers return.
static PyObject* owned_error_int(kafka_common_Error_t* e) {
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* mock_only_error(void) {
    kafka_common_Error_t *ffi_err = NULL;
    return owned_error_int(
        ffi_built(kafka_common_Error_local_illegal_argument_err("this operation is only supported on a MockAdminClient", &ffi_err), &ffi_err));
}

// MockAdminClient_timeout_next_request(h, n) -> error
static PyObject* py_MockAdminClient_timeout_next_request(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; int n;
    if (!PyArg_ParseTuple(args, "Ki", &h, &n)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    if (a->mock == NULL) return mock_only_error();
    return owned_error_int((kafka_admin_MockAdminClient_timeout_next_request_err(a->mock, (int32_t)n, &ffi_err), ffi_take(&ffi_err)));
}

// MockAdminClient_set_using_raft_controller(h, using_raft_controller) -> error.
// Java's `MockAdminClient.Builder.usingRaftController(boolean)`, a setter on
// the constructed mock here.
static PyObject* py_MockAdminClient_set_using_raft_controller(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; int using_raft_controller;
    if (!PyArg_ParseTuple(args, "Kp", &h, &using_raft_controller)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    if (a->mock == NULL) return mock_only_error();
    return owned_error_int((kafka_admin_MockAdminClient_set_using_raft_controller_err(a->mock, (int8_t)(using_raft_controller != 0), &ffi_err), ffi_take(&ffi_err)));
}

typedef void (*mock_offsets_fn)(const kafka_admin_MockAdminClient_t*, const kafka_Map_t*, kafka_common_Error_t**);

static PyObject* mock_update_offsets(PyObject* args, mock_offsets_fn fn) {
    unsigned long long h; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    if (a->mock == NULL) return mock_only_error();
    tp_i64_map_t m;
    if (tp_i64_map_build(spec, &m) < 0) return NULL;
    kafka_common_Error_t* err = NULL;
    fn(a->mock, m.map, &err);  // copied
    tp_i64_map_free(&m);
    return owned_error_int(err);
}

// MockAdminClient_update_beginning_offsets(h, [(topic, partition, offset)]) -> error
static PyObject* py_MockAdminClient_update_beginning_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_beginning_offsets_err);
}

// MockAdminClient_update_end_offsets(h, [(topic, partition, offset)]) -> error
static PyObject* py_MockAdminClient_update_end_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_end_offsets_err);
}

// MockAdminClient_update_consumer_group_offsets(h, [(topic, partition, offset)]) -> error
static PyObject* py_MockAdminClient_update_consumer_group_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_consumer_group_offsets_err);
}

// MockAdminClient_set_feature_levels(h, [(feature, level, min, max)]) -> error.
// Three borrowed char* -> int16_t* maps, replacing what was seeded before.
static PyObject* py_MockAdminClient_set_feature_levels(PyObject* self, PyObject* args) {
    kafka_common_Error_t *ffi_err = NULL;
    unsigned long long h; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    if (a->mock == NULL) return mock_only_error();
    PyObject* fast = fast_rows(spec, "feature levels must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    int16_t* vals = n > 0 ? PyMem_Calloc((size_t)n * 3, sizeof(int16_t)) : NULL;
    if (n > 0 && vals == NULL) { Py_DECREF(fast); PyErr_NoMemory(); return NULL; }
    kafka_Map_t* levels = NULL;
    kafka_Map_t* mins = NULL;
    kafka_Map_t* maxs = NULL;
    kafka_common_Error_t* err = (levels = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err));
    FFI_STEP(err, (mins = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)));
    FFI_STEP(err, (maxs = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err)));
    for (Py_ssize_t i = 0; i < n && err == NULL; i++) {
        const char* feature; int level, lo, hi;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siii", &feature, &level, &lo, &hi)) {
            if (levels) ffi_unraisable((kafka_Map_destroy_err(levels, &ffi_err), ffi_take(&ffi_err)));
            if (mins) ffi_unraisable((kafka_Map_destroy_err(mins, &ffi_err), ffi_take(&ffi_err)));
            if (maxs) ffi_unraisable((kafka_Map_destroy_err(maxs, &ffi_err), ffi_take(&ffi_err)));
            PyMem_Free(vals); Py_DECREF(fast); return NULL;
        }
        vals[3 * i] = (int16_t)level; vals[3 * i + 1] = (int16_t)lo; vals[3 * i + 2] = (int16_t)hi;
        FFI_STEP(err, (kafka_Map_put_err(levels, (void*)feature, &vals[3 * i], &ffi_err), ffi_take(&ffi_err)));
        FFI_STEP(err, (kafka_Map_put_err(mins, (void*)feature, &vals[3 * i + 1], &ffi_err), ffi_take(&ffi_err)));
        FFI_STEP(err, (kafka_Map_put_err(maxs, (void*)feature, &vals[3 * i + 2], &ffi_err), ffi_take(&ffi_err)));
    }
    FFI_STEP(err, (kafka_admin_MockAdminClient_set_feature_levels_err(a->mock, levels, mins, maxs, &ffi_err), ffi_take(&ffi_err)));  // copied
    if (levels) ffi_unraisable((kafka_Map_destroy_err(levels, &ffi_err), ffi_take(&ffi_err)));
    if (mins) ffi_unraisable((kafka_Map_destroy_err(mins, &ffi_err), ffi_take(&ffi_err)));
    if (maxs) ffi_unraisable((kafka_Map_destroy_err(maxs, &ffi_err), ffi_take(&ffi_err)));
    PyMem_Free(vals);
    Py_DECREF(fast);
    return owned_error_int(err);
}

static PyMethodDef ProducerNativeMethods[] = {
    {"MockProducer_new", py_MockProducer_new, METH_VARARGS,
     "MockProducer_new(auto_complete, notify_cb=None) -> handle"},
    {"KafkaProducer_new", py_KafkaProducer_new, METH_VARARGS,
     "KafkaProducer_new(config_dict, notify_cb) -> (handle, 0) or (0, owned_error_int)"},
    {"Producer_send", py_Producer_send, METH_VARARGS,
     "Blocking send(handle, record, on_completion) -> (KafkaFuture | None, owned_error_int); on_completion(meta | None, borrowed_error_int, callback_id) -> None | True"},
    {"Producer_send_cb", py_Producer_send_cb, METH_VARARGS,
     "Queued send(handle, record, on_completion, resolve) -> owned_error_int; resolve(owned_error_int) once handed to the producer"},
    {"Producer_poll", py_Producer_poll, METH_VARARGS,
     "poll(timeout_ms): run queued callbacks on this thread, waiting up to timeout_ms; returns count"},
    {"Producer__set_callback_result", py_Producer__set_callback_result, METH_VARARGS,
     "Report a deferred delivery-callback result: (h, callback_id, exception | None)"},
    {"Producer__execute_callbacks", py_Producer__execute_callbacks, METH_VARARGS,
     "Run the queued callbacks on this thread; returns count"},
    {"Producer_flush", py_Producer_flush, METH_VARARGS, "Blocking flush -> owned_error_int"},
    {"Producer_flush_cb", py_Producer_flush_cb, METH_VARARGS, "Queued flush; cb(owned_error_int)"},
    {"Producer_partitions_for", py_Producer_partitions_for, METH_VARARGS,
     "Blocking partitions_for(topic) -> (list | None, owned_error_int)"},
    {"Producer_partitions_for_cb", py_Producer_partitions_for_cb, METH_VARARGS,
     "Queued partitions_for(topic, cb); cb(list | None, owned_error_int)"},
    {"Producer_metrics", py_Producer_metrics, METH_VARARGS,
     "Point-in-time metrics snapshot; returns list[dict]"},
    {"Producer_init_transactions", py_Producer_init_transactions, METH_VARARGS,
     "Blocking initTransactions -> owned_error_int"},
    {"Producer_init_transactions_cb", py_Producer_init_transactions_cb, METH_VARARGS,
     "Queued initTransactions; cb(owned_error_int)"},
    {"Producer_begin_transaction", py_Producer_begin_transaction, METH_VARARGS,
     "beginTransaction -> owned_error_int"},
    {"Producer_begin_transaction_cb", py_Producer_begin_transaction_cb, METH_VARARGS,
     "Queued beginTransaction; cb(owned_error_int)"},
    {"Producer_send_offsets_to_transaction", py_Producer_send_offsets_to_transaction, METH_VARARGS,
     "Blocking sendOffsetsToTransaction(spec, group_metadata) -> owned_error_int"},
    {"Producer_send_offsets_to_transaction_cb", py_Producer_send_offsets_to_transaction_cb, METH_VARARGS,
     "Queued sendOffsetsToTransaction(spec, group_metadata, cb); cb(owned_error_int)"},
    {"Producer_commit_transaction", py_Producer_commit_transaction, METH_VARARGS,
     "Blocking commitTransaction -> owned_error_int"},
    {"Producer_commit_transaction_cb", py_Producer_commit_transaction_cb, METH_VARARGS,
     "Queued commitTransaction; cb(owned_error_int)"},
    {"Producer_abort_transaction", py_Producer_abort_transaction, METH_VARARGS,
     "Blocking abortTransaction -> owned_error_int"},
    {"Producer_abort_transaction_cb", py_Producer_abort_transaction_cb, METH_VARARGS,
     "Queued abortTransaction; cb(owned_error_int)"},
    {"Producer_close", py_Producer_close, METH_VARARGS, "Blocking close -> owned_error_int"},
    {"Producer_close_cb", py_Producer_close_cb, METH_VARARGS, "Queued close; cb(owned_error_int)"},
    {"Producer_close_with_timeout_cb", py_Producer_close_with_timeout_cb, METH_VARARGS,
     "Queued close(Duration)(timeout_ms, cb); cb(owned_error_int)"},
    {"Producer_destroy", py_Producer_destroy, METH_VARARGS,
     "Free the class handle (runs still-pending callbacks) and the C struct"},
    {"MockProducer_complete_next", py_MockProducer_complete_next, METH_VARARGS,
     "Complete the next pending send successfully"},
    {"MockProducer_error_next", py_MockProducer_error_next, METH_VARARGS,
     "Complete the next pending send with the error class mapped from (code, message)"},
    {"MockProducer_history_count", py_MockProducer_history_count, METH_VARARGS,
     "Return the number of records in the sent history"},
    {"MockProducer_clear", py_MockProducer_clear, METH_VARARGS,
     "Clear the sent history and pending completions"},
    {"MockProducer_set_commit_transaction_error", py_MockProducer_set_commit_transaction_error,
     METH_VARARGS, "Test hook: install (code, message) / clear (None) the mock's commitTransaction error"},
    {"MockProducer_sent_offsets", py_MockProducer_sent_offsets, METH_VARARGS,
     "Test hook: whether offsets were staged in the current transaction"},
    {"MockProducer_committed_offset", py_MockProducer_committed_offset, METH_VARARGS,
     "Test hook: (offset, leader_epoch | None, metadata) for (group_id, topic, partition) or None"},
    {"KafkaError_code", py_KafkaError_code, METH_VARARGS,
     "Get error code from KafkaError pointer"},
    {"KafkaError_message", py_KafkaError_message, METH_VARARGS,
     "Get error message from KafkaError pointer"},
    {"KafkaError_is_retriable", py_KafkaError_is_retriable, METH_VARARGS,
     "Check if KafkaError is retriable"},
    {"KafkaError_is_fatal", py_KafkaError_is_fatal, METH_VARARGS,
     "Check if KafkaError is fatal"},
    {"KafkaError_txn_requires_abort", py_KafkaError_txn_requires_abort, METH_VARARGS,
     "Check if KafkaError requires the transaction to be aborted"},
    {"KafkaError_destroy", py_KafkaError_destroy, METH_VARARGS,
     "Destroy KafkaError handle"},
    {"KafkaError_source", py_KafkaError_source, METH_VARARGS,
     "Java getCause(): the BORROWED source handle, 0 when none (never destroy it)"},
    {"KafkaError_is_api_error", py_KafkaError_is_api_error, METH_VARARGS,
     "Java instanceof ApiException"},
    {"KafkaError_callback_exception", py_KafkaError_callback_exception, METH_VARARGS,
     "The exception object of the first foreign callback error in the cause chain, or None"},
    {"KafkaError_new", py_KafkaError_new, METH_VARARGS,
     "KafkaError_new(code, message | None) -> owned error handle (-1 = bare KafkaException)"},
    {"KafkaError_payload", py_KafkaError_payload, METH_VARARGS,
     "KafkaError_payload(handle) -> dict of the Java payload getters, or None"},
    // ---- Consumer ----
    //
    // Every blocking operation `X(h, ...)` has a `X_cb(h, ..., cb)` twin that
    // queues its completion for Consumer__execute_callbacks; the callable then
    // receives the same payload the blocking form returns.
    {"Consumer_MockConsumer_new", py_Consumer_MockConsumer_new, METH_VARARGS, "Create a MockConsumer; returns a handle"},
    {"Consumer_KafkaConsumer_new", py_Consumer_KafkaConsumer_new, METH_VARARGS, "Create a KafkaConsumer from a config dict; returns (handle, error)"},
    {"Consumer_destroy", py_Consumer_destroy, METH_VARARGS, "Destroy a consumer handle (runs the still pending callbacks)"},
    {"Consumer_wakeup", py_Consumer_wakeup, METH_VARARGS, "Wake up a blocked operation"},
    {"Consumer__execute_callbacks", py_Consumer__execute_callbacks, METH_VARARGS, "Run the queued callbacks on this thread; returns how many ran"},
    {"Consumer__set_callbacks_notify", py_Consumer__set_callbacks_notify, METH_VARARGS, "Register the callable fired when callbacks become pending"},
    {"Consumer__set_callback_result", py_Consumer__set_callback_result, METH_VARARGS, "Report a deferred listener result: (h, callback_id, exception | message | None)"},
    {"Consumer_assignment", py_Consumer_assignment, METH_VARARGS, "(list[(topic, partition)] | None, err | None)"},
    {"Consumer_subscription", py_Consumer_subscription, METH_VARARGS, "(list[str] | None, err | None)"},
    {"Consumer_paused", py_Consumer_paused, METH_VARARGS, "(list[(topic, partition)] | None, err | None)"},
    {"Consumer_metrics", py_Consumer_metrics, METH_VARARGS, "Metrics snapshot as list[dict] with name/group/description/tags/value"},
    {"Consumer_group_metadata", py_Consumer_group_metadata, METH_VARARGS, "(ConsumerGroupMetadata | None, err | None)"},
    {"Consumer_client_id", py_Consumer_client_id, METH_VARARGS, "Client id string"},
    {"Consumer_current_lag", py_Consumer_current_lag, METH_VARARGS, "(lag int | None, err | None)"},
    {"Consumer_poll", py_Consumer_poll, METH_VARARGS, "poll(h, timeout_ms) -> (ConsumerRecords | None, err | None)"},
    {"Consumer_poll_cb", py_Consumer_poll_cb, METH_VARARGS, "poll twin; cb(ConsumerRecords | None, err | None)"},
    {"Consumer_subscribe", py_Consumer_subscribe, METH_VARARGS, "subscribe(h, topics, listener_adapter | None) -> err | None"},
    {"Consumer_subscribe_cb", py_Consumer_subscribe_cb, METH_VARARGS, "subscribe twin; cb(err | None)"},
    {"Consumer_subscribe_pattern", py_Consumer_subscribe_pattern, METH_VARARGS, "subscribe_pattern(h, pattern, listener_adapter | None) -> err | None"},
    {"Consumer_subscribe_pattern_cb", py_Consumer_subscribe_pattern_cb, METH_VARARGS, "subscribe_pattern twin; cb(err | None)"},
    {"Consumer_unsubscribe", py_Consumer_unsubscribe, METH_VARARGS, "unsubscribe(h) -> err | None"},
    {"Consumer_unsubscribe_cb", py_Consumer_unsubscribe_cb, METH_VARARGS, "unsubscribe twin; cb(err | None)"},
    {"Consumer_assign", py_Consumer_assign, METH_VARARGS, "assign(h, [(topic, partition)]) -> err | None"},
    {"Consumer_assign_cb", py_Consumer_assign_cb, METH_VARARGS, "assign twin; cb(err | None)"},
    {"Consumer_pause", py_Consumer_pause, METH_VARARGS, "pause(h, [(topic, partition)]) -> err | None"},
    {"Consumer_pause_cb", py_Consumer_pause_cb, METH_VARARGS, "pause twin; cb(err | None)"},
    {"Consumer_resume", py_Consumer_resume, METH_VARARGS, "resume(h, [(topic, partition)]) -> err | None"},
    {"Consumer_resume_cb", py_Consumer_resume_cb, METH_VARARGS, "resume twin; cb(err | None)"},
    {"Consumer_seek", py_Consumer_seek, METH_VARARGS, "seek(h, topic, partition, offset) -> err | None"},
    {"Consumer_seek_cb", py_Consumer_seek_cb, METH_VARARGS, "seek twin; cb(err | None)"},
    {"Consumer_seek_with_metadata", py_Consumer_seek_with_metadata, METH_VARARGS,
     "seek_with_metadata(h, topic, partition, offset, leader_epoch, metadata | None) -> err | None"},
    {"Consumer_seek_with_metadata_cb", py_Consumer_seek_with_metadata_cb, METH_VARARGS, "seek_with_metadata twin; cb(err | None)"},
    {"Consumer_seek_to_beginning", py_Consumer_seek_to_beginning, METH_VARARGS, "seek_to_beginning(h, [(topic, partition)]) -> err | None"},
    {"Consumer_seek_to_beginning_cb", py_Consumer_seek_to_beginning_cb, METH_VARARGS, "seek_to_beginning twin; cb(err | None)"},
    {"Consumer_seek_to_end", py_Consumer_seek_to_end, METH_VARARGS, "seek_to_end(h, [(topic, partition)]) -> err | None"},
    {"Consumer_seek_to_end_cb", py_Consumer_seek_to_end_cb, METH_VARARGS, "seek_to_end twin; cb(err | None)"},
    {"Consumer_commit_sync", py_Consumer_commit_sync, METH_VARARGS, "commit_sync(h, offsets_spec | None, timeout_ms) -> err | None"},
    {"Consumer_commit_sync_cb", py_Consumer_commit_sync_cb, METH_VARARGS, "commit_sync twin; cb(err | None)"},
    {"Consumer_commit_async", py_Consumer_commit_async, METH_VARARGS,
     "commit_async(h, offsets_spec | None, callback_adapter | None) -> err | None; adapter(offsets_dict, err | None)"},
    {"Consumer_commit_async_cb", py_Consumer_commit_async_cb, METH_VARARGS, "commit_async twin; cb(err | None)"},
    {"Consumer_position", py_Consumer_position, METH_VARARGS, "position(h, topic, partition, timeout_ms) -> (int, err | None)"},
    {"Consumer_position_cb", py_Consumer_position_cb, METH_VARARGS, "position twin; cb(int, err | None)"},
    {"Consumer_committed", py_Consumer_committed, METH_VARARGS, "committed(h, [(topic, partition)], timeout_ms) -> (dict | None, err | None)"},
    {"Consumer_committed_cb", py_Consumer_committed_cb, METH_VARARGS, "committed twin; cb(dict | None, err | None)"},
    {"Consumer_beginning_offsets", py_Consumer_beginning_offsets, METH_VARARGS, "beginning_offsets(h, [(topic, partition)], timeout_ms) -> (dict | None, err | None)"},
    {"Consumer_beginning_offsets_cb", py_Consumer_beginning_offsets_cb, METH_VARARGS, "beginning_offsets twin; cb(dict | None, err | None)"},
    {"Consumer_end_offsets", py_Consumer_end_offsets, METH_VARARGS, "end_offsets(h, [(topic, partition)], timeout_ms) -> (dict | None, err | None)"},
    {"Consumer_end_offsets_cb", py_Consumer_end_offsets_cb, METH_VARARGS, "end_offsets twin; cb(dict | None, err | None)"},
    {"Consumer_offsets_for_times", py_Consumer_offsets_for_times, METH_VARARGS,
     "offsets_for_times(h, [(topic, partition, timestamp)], timeout_ms) -> (dict | None, err | None)"},
    {"Consumer_offsets_for_times_cb", py_Consumer_offsets_for_times_cb, METH_VARARGS, "offsets_for_times twin; cb(dict | None, err | None)"},
    {"Consumer_partitions_for", py_Consumer_partitions_for, METH_VARARGS, "partitions_for(h, topic, timeout_ms) -> (list | None, err | None)"},
    {"Consumer_partitions_for_cb", py_Consumer_partitions_for_cb, METH_VARARGS, "partitions_for twin; cb(list | None, err | None)"},
    {"Consumer_list_topics", py_Consumer_list_topics, METH_VARARGS, "list_topics(h, timeout_ms) -> (dict | None, err | None)"},
    {"Consumer_list_topics_cb", py_Consumer_list_topics_cb, METH_VARARGS, "list_topics twin; cb(dict | None, err | None)"},
    {"Consumer_enforce_rebalance", py_Consumer_enforce_rebalance, METH_VARARGS, "enforce_rebalance(h) -> err | None"},
    {"Consumer_enforce_rebalance_cb", py_Consumer_enforce_rebalance_cb, METH_VARARGS, "enforce_rebalance twin; cb(err | None)"},
    {"Consumer_enforce_rebalance_with_reason", py_Consumer_enforce_rebalance_with_reason, METH_VARARGS, "enforce_rebalance_with_reason(h, reason) -> err | None"},
    {"Consumer_enforce_rebalance_with_reason_cb", py_Consumer_enforce_rebalance_with_reason_cb, METH_VARARGS, "enforce_rebalance_with_reason twin; cb(err | None)"},
    {"Consumer_close", py_Consumer_close, METH_VARARGS, "close(h, timeout_ms, operation) -> error (negative timeout and DEFAULT: Java's close())"},
    {"Consumer_close_cb", py_Consumer_close_cb, METH_VARARGS, "close twin; cb(err | None)"},
    // ---- ConsumerHandle (re-entrancy handle; safe from inside listeners) ----
    {"Consumer_handle", py_Consumer_handle, METH_VARARGS, "New re-entrancy handle for a consumer"},
    {"ConsumerHandle_destroy", py_ConsumerHandle_destroy, METH_VARARGS, "Destroy a re-entrancy handle"},
    {"ConsumerHandle_wakeup", py_ConsumerHandle_wakeup, METH_VARARGS, "Wake up the owning consumer"},
    {"ConsumerHandle_assignment", py_ConsumerHandle_assignment, METH_VARARGS, "Assignment as list[(topic, partition)]"},
    {"ConsumerHandle_subscription", py_ConsumerHandle_subscription, METH_VARARGS, "Subscription as list[str]"},
    {"ConsumerHandle_paused", py_ConsumerHandle_paused, METH_VARARGS, "Paused partitions as list[(topic, partition)]"},
    {"ConsumerHandle_assign", py_ConsumerHandle_assign, METH_VARARGS, "assign(hh, [(topic, partition)]) -> err | None"},
    {"ConsumerHandle_assign_cb", py_ConsumerHandle_assign_cb, METH_VARARGS, "assign twin; cb(err | None)"},
    {"ConsumerHandle_seek", py_ConsumerHandle_seek, METH_VARARGS, "seek(hh, topic, partition, offset) -> err | None"},
    {"ConsumerHandle_seek_cb", py_ConsumerHandle_seek_cb, METH_VARARGS, "seek twin; cb(err | None)"},
    {"ConsumerHandle_seek_with_metadata", py_ConsumerHandle_seek_with_metadata, METH_VARARGS,
     "seek_with_metadata(hh, topic, partition, offset, leader_epoch, metadata | None) -> err | None"},
    {"ConsumerHandle_seek_with_metadata_cb", py_ConsumerHandle_seek_with_metadata_cb, METH_VARARGS, "seek_with_metadata twin; cb(err | None)"},
    {"ConsumerHandle_seek_to_beginning", py_ConsumerHandle_seek_to_beginning, METH_VARARGS, "seek_to_beginning(hh, tps) -> err | None"},
    {"ConsumerHandle_seek_to_beginning_cb", py_ConsumerHandle_seek_to_beginning_cb, METH_VARARGS, "seek_to_beginning twin; cb(err | None)"},
    {"ConsumerHandle_seek_to_end", py_ConsumerHandle_seek_to_end, METH_VARARGS, "seek_to_end(hh, tps) -> err | None"},
    {"ConsumerHandle_seek_to_end_cb", py_ConsumerHandle_seek_to_end_cb, METH_VARARGS, "seek_to_end twin; cb(err | None)"},
    {"ConsumerHandle_pause", py_ConsumerHandle_pause, METH_VARARGS, "pause(hh, tps) -> err | None"},
    {"ConsumerHandle_pause_cb", py_ConsumerHandle_pause_cb, METH_VARARGS, "pause twin; cb(err | None)"},
    {"ConsumerHandle_resume", py_ConsumerHandle_resume, METH_VARARGS, "resume(hh, tps) -> err | None"},
    {"ConsumerHandle_resume_cb", py_ConsumerHandle_resume_cb, METH_VARARGS, "resume twin; cb(err | None)"},
    {"ConsumerHandle_position", py_ConsumerHandle_position, METH_VARARGS, "position(hh, topic, partition, timeout_ms) -> (int, err | None)"},
    {"ConsumerHandle_position_cb", py_ConsumerHandle_position_cb, METH_VARARGS, "position twin; cb(int, err | None)"},
    {"ConsumerHandle_committed", py_ConsumerHandle_committed, METH_VARARGS, "committed(hh, tps) -> (dict | None, err | None)"},
    {"ConsumerHandle_committed_cb", py_ConsumerHandle_committed_cb, METH_VARARGS, "committed twin; cb(dict | None, err | None)"},
    {"ConsumerHandle_beginning_offsets", py_ConsumerHandle_beginning_offsets, METH_VARARGS, "beginning_offsets(hh, tps) -> (dict | None, err | None)"},
    {"ConsumerHandle_beginning_offsets_cb", py_ConsumerHandle_beginning_offsets_cb, METH_VARARGS, "beginning_offsets twin; cb(dict | None, err | None)"},
    {"ConsumerHandle_end_offsets", py_ConsumerHandle_end_offsets, METH_VARARGS, "end_offsets(hh, tps) -> (dict | None, err | None)"},
    {"ConsumerHandle_end_offsets_cb", py_ConsumerHandle_end_offsets_cb, METH_VARARGS, "end_offsets twin; cb(dict | None, err | None)"},
    {"ConsumerHandle_offsets_for_times", py_ConsumerHandle_offsets_for_times, METH_VARARGS, "offsets_for_times(hh, [(t, p, ts)]) -> (dict | None, err | None)"},
    {"ConsumerHandle_offsets_for_times_cb", py_ConsumerHandle_offsets_for_times_cb, METH_VARARGS, "offsets_for_times twin; cb(dict | None, err | None)"},
    {"ConsumerHandle_commit_sync", py_ConsumerHandle_commit_sync, METH_VARARGS, "commit_sync(hh, offsets_spec | None) -> err | None"},
    {"ConsumerHandle_commit_sync_cb", py_ConsumerHandle_commit_sync_cb, METH_VARARGS, "commit_sync twin; cb(err | None)"},
    {"ConsumerHandle_commit_async", py_ConsumerHandle_commit_async, METH_VARARGS, "commit_async(hh, offsets_spec | None) -> err | None"},
    {"ConsumerHandle_commit_async_cb", py_ConsumerHandle_commit_async_cb, METH_VARARGS, "commit_async twin; cb(err | None)"},
    {"ConsumerHandle_commit_async_callback", py_ConsumerHandle_commit_async_callback, METH_VARARGS,
     "commit_async_callback(h, hh, offsets_spec | None, adapter) -> err | None"},
    // ---- MockConsumer drivers ----
    {"MockConsumer_rebalance", py_MockConsumer_rebalance, METH_VARARGS, "Mock: drive a rebalance to an assignment (invokes the listener) -> err | None"},
    {"MockConsumer_rebalance_cb", py_MockConsumer_rebalance_cb, METH_VARARGS, "rebalance twin; cb(err | None)"},
    {"MockConsumer_add_record", py_MockConsumer_add_record, METH_VARARGS, "Mock: add_record(h, topic, partition, offset, key | None, value | None) -> err | None"},
    {"MockConsumer_update_beginning_offsets", py_MockConsumer_update_beginning_offsets, METH_VARARGS, "Mock: set beginning offsets from [(topic, partition, offset)]"},
    {"MockConsumer_update_end_offsets", py_MockConsumer_update_end_offsets, METH_VARARGS, "Mock: set end offsets from [(topic, partition, offset)]"},
    {"MockConsumer_update_duration_offsets", py_MockConsumer_update_duration_offsets, METH_VARARGS, "Mock: set duration offsets from [(topic, partition, offset)]"},
    {"MockConsumer_update_partitions", py_MockConsumer_update_partitions, METH_VARARGS,
     "Mock: update_partitions(h, topic, [(partition, leader_id, host, port)]) -> err | None"},
    {"MockConsumer_set_poll_error", py_MockConsumer_set_poll_error, METH_VARARGS, "Mock: set_poll_error(h, message, code | None)"},
    {"MockConsumer_set_offsets_error", py_MockConsumer_set_offsets_error, METH_VARARGS, "Mock: set_offsets_error(h, message, code | None)"},
    {"MockConsumer_set_max_poll_records", py_MockConsumer_set_max_poll_records, METH_VARARGS, "Mock: set_max_poll_records(h, n) -> err | None"},
    {"MockConsumer_closed", py_MockConsumer_closed, METH_VARARGS, "Mock: whether close() was called"},
    {"MockConsumer_should_rebalance", py_MockConsumer_should_rebalance, METH_VARARGS, "Mock: whether enforce_rebalance() was called"},
    {"MockConsumer_reset_should_rebalance", py_MockConsumer_reset_should_rebalance, METH_VARARGS, "Mock: clear the should_rebalance flag"},
    {"MockConsumer_last_poll_timeout", py_MockConsumer_last_poll_timeout, METH_VARARGS, "Mock: the timeout (ms) of the last poll()"},
    // ---- Admin ----
    {"Admin_MockAdminClient_new", py_Admin_MockAdminClient_new, METH_VARARGS, "Admin_MockAdminClient_new(num_brokers) -> handle"},
    {"Admin_AdminClient_new", py_Admin_AdminClient_new, METH_VARARGS, "Admin_AdminClient_new(config: dict[str, str]) -> handle; RuntimeError on a rejected config"},
    {"Admin_destroy", py_Admin_destroy, METH_VARARGS, "Admin_destroy(handle): destroy the client (runs the still pending callbacks)"},
    {"Admin_close", py_Admin_close, METH_VARARGS, "Admin_close(handle, timeout_ms): blocking close, GIL released; -1 = Java's no-timeout close() (the Python clients use Admin_close_cb)"},
    {"Admin_close_cb", py_Admin_close_cb, METH_VARARGS, "Admin_close_cb(handle, timeout_ms, cb): close twin; cb() queued on the callbacks vector when the close completed; -1 = Java's no-timeout close()"},
    {"Admin__set_callbacks_notify", py_Admin__set_callbacks_notify, METH_VARARGS, "Admin__set_callbacks_notify(handle, cb | None): cb() fires when the callback queue goes non-empty (schedule only)"},
    {"Admin__execute_callbacks", py_Admin__execute_callbacks, METH_VARARGS, "Admin__execute_callbacks(handle) -> number of pending completion callbacks run on this thread"},
    {"Admin_resolve", py_Admin_resolve, METH_VARARGS, "Admin_resolve(job) -> payload: block on every future (GIL released), convert, free the job"},
    {"Admin_resolve_cb", py_Admin_resolve_cb, METH_VARARGS, "Admin_resolve_cb(job, cb): cb(payload, None) / cb(None, exc) once every future delivered"},
    {"Admin_job_discard", py_Admin_job_discard, METH_VARARGS, "Admin_job_discard(job): free an unresolved job"},
    {"Admin_create_topics", py_Admin_create_topics, METH_VARARGS, "Admin_create_topics(handle, ...) -> (job, error)"},
    {"Admin_delete_topics", py_Admin_delete_topics, METH_VARARGS, "Admin_delete_topics(handle, ...) -> (job, error)"},
    {"Admin_list_topics", py_Admin_list_topics, METH_VARARGS, "Admin_list_topics(handle, ...) -> (job, error)"},
    {"Admin_describe_topics", py_Admin_describe_topics, METH_VARARGS, "Admin_describe_topics(handle, ...) -> (job, error)"},
    {"Admin_create_partitions", py_Admin_create_partitions, METH_VARARGS, "Admin_create_partitions(handle, ...) -> (job, error)"},
    {"Admin_delete_records", py_Admin_delete_records, METH_VARARGS, "Admin_delete_records(handle, ...) -> (job, error)"},
    {"Admin_describe_cluster", py_Admin_describe_cluster, METH_VARARGS, "Admin_describe_cluster(handle, ...) -> (job, error)"},
    {"Admin_describe_configs", py_Admin_describe_configs, METH_VARARGS, "Admin_describe_configs(handle, ...) -> (job, error)"},
    {"Admin_incremental_alter_configs", py_Admin_incremental_alter_configs, METH_VARARGS, "Admin_incremental_alter_configs(handle, ...) -> (job, error)"},
    {"Admin_list_config_resources", py_Admin_list_config_resources, METH_VARARGS, "Admin_list_config_resources(handle, ...) -> (job, error)"},
    {"Admin_describe_log_dirs", py_Admin_describe_log_dirs, METH_VARARGS, "Admin_describe_log_dirs(handle, ...) -> (job, error)"},
    {"Admin_alter_replica_log_dirs", py_Admin_alter_replica_log_dirs, METH_VARARGS, "Admin_alter_replica_log_dirs(handle, ...) -> (job, error)"},
    {"Admin_describe_replica_log_dirs", py_Admin_describe_replica_log_dirs, METH_VARARGS, "Admin_describe_replica_log_dirs(handle, ...) -> (job, error)"},
    {"Admin_elect_leaders", py_Admin_elect_leaders, METH_VARARGS, "Admin_elect_leaders(handle, ...) -> (job, error)"},
    {"Admin_alter_partition_reassignments", py_Admin_alter_partition_reassignments, METH_VARARGS, "Admin_alter_partition_reassignments(handle, ...) -> (job, error)"},
    {"Admin_list_partition_reassignments", py_Admin_list_partition_reassignments, METH_VARARGS, "Admin_list_partition_reassignments(handle, ...) -> (job, error)"},
    {"Admin_list_offsets", py_Admin_list_offsets, METH_VARARGS, "Admin_list_offsets(handle, ...) -> (job, error)"},
    {"Admin_list_groups", py_Admin_list_groups, METH_VARARGS, "Admin_list_groups(handle, ...) -> (job, error)"},
    {"Admin_describe_consumer_groups", py_Admin_describe_consumer_groups, METH_VARARGS, "Admin_describe_consumer_groups(handle, ...) -> (job, error)"},
    {"Admin_describe_classic_groups", py_Admin_describe_classic_groups, METH_VARARGS, "Admin_describe_classic_groups(handle, ...) -> (job, error)"},
    {"Admin_list_consumer_group_offsets", py_Admin_list_consumer_group_offsets, METH_VARARGS, "Admin_list_consumer_group_offsets(handle, ...) -> (job, error)"},
    {"Admin_alter_consumer_group_offsets", py_Admin_alter_consumer_group_offsets, METH_VARARGS, "Admin_alter_consumer_group_offsets(handle, ...) -> (job, error)"},
    {"Admin_delete_consumer_group_offsets", py_Admin_delete_consumer_group_offsets, METH_VARARGS, "Admin_delete_consumer_group_offsets(handle, ...) -> (job, error)"},
    {"Admin_delete_consumer_groups", py_Admin_delete_consumer_groups, METH_VARARGS, "Admin_delete_consumer_groups(handle, ...) -> (job, error)"},
    {"Admin_remove_members_from_consumer_group", py_Admin_remove_members_from_consumer_group, METH_VARARGS, "Admin_remove_members_from_consumer_group(handle, ...) -> (job, error)"},
    {"Admin_create_acls", py_Admin_create_acls, METH_VARARGS, "Admin_create_acls(handle, ...) -> (job, error)"},
    {"Admin_describe_acls", py_Admin_describe_acls, METH_VARARGS, "Admin_describe_acls(handle, ...) -> (job, error)"},
    {"Admin_delete_acls", py_Admin_delete_acls, METH_VARARGS, "Admin_delete_acls(handle, ...) -> (job, error)"},
    {"Admin_describe_client_quotas", py_Admin_describe_client_quotas, METH_VARARGS, "Admin_describe_client_quotas(handle, ...) -> (job, error)"},
    {"Admin_alter_client_quotas", py_Admin_alter_client_quotas, METH_VARARGS, "Admin_alter_client_quotas(handle, ...) -> (job, error)"},
    {"Admin_describe_user_scram_credentials", py_Admin_describe_user_scram_credentials, METH_VARARGS, "Admin_describe_user_scram_credentials(handle, ...) -> (job, error)"},
    {"Admin_alter_user_scram_credentials", py_Admin_alter_user_scram_credentials, METH_VARARGS, "Admin_alter_user_scram_credentials(handle, ...) -> (job, error)"},
    {"Admin_create_delegation_token", py_Admin_create_delegation_token, METH_VARARGS, "Admin_create_delegation_token(handle, ...) -> (job, error)"},
    {"Admin_renew_delegation_token", py_Admin_renew_delegation_token, METH_VARARGS, "Admin_renew_delegation_token(handle, ...) -> (job, error)"},
    {"Admin_expire_delegation_token", py_Admin_expire_delegation_token, METH_VARARGS, "Admin_expire_delegation_token(handle, ...) -> (job, error)"},
    {"Admin_describe_delegation_token", py_Admin_describe_delegation_token, METH_VARARGS, "Admin_describe_delegation_token(handle, ...) -> (job, error)"},
    {"Admin_describe_features", py_Admin_describe_features, METH_VARARGS, "Admin_describe_features(handle, ...) -> (job, error)"},
    {"Admin_update_features", py_Admin_update_features, METH_VARARGS, "Admin_update_features(handle, ...) -> (job, error)"},
    {"Admin_describe_producers", py_Admin_describe_producers, METH_VARARGS, "Admin_describe_producers(handle, ...) -> (job, error)"},
    {"Admin_describe_transactions", py_Admin_describe_transactions, METH_VARARGS, "Admin_describe_transactions(handle, ...) -> (job, error)"},
    {"Admin_fence_producers", py_Admin_fence_producers, METH_VARARGS, "Admin_fence_producers(handle, ...) -> (job, error)"},
    {"Admin_list_transactions", py_Admin_list_transactions, METH_VARARGS, "Admin_list_transactions(handle, ...) -> (job, error)"},
    {"Admin_abort_transaction", py_Admin_abort_transaction, METH_VARARGS, "Admin_abort_transaction(handle, ...) -> (job, error)"},
    {"Admin_unregister_controller", py_Admin_unregister_controller, METH_VARARGS, "Admin_unregister_controller(handle, ...) -> (job, error)"},
    {"Admin_force_terminate_transaction", py_Admin_force_terminate_transaction, METH_VARARGS, "Admin_force_terminate_transaction(handle, ...) -> (job, error)"},
    {"MockAdminClient_timeout_next_request", py_MockAdminClient_timeout_next_request, METH_VARARGS, "Mock-only seeding; returns an owned error handle (0 = ok)"},
    {"MockAdminClient_set_using_raft_controller", py_MockAdminClient_set_using_raft_controller, METH_VARARGS, "Mock-only seeding; returns an owned error handle (0 = ok)"},
    {"MockAdminClient_update_beginning_offsets", py_MockAdminClient_update_beginning_offsets, METH_VARARGS, "Mock-only seeding; returns an owned error handle (0 = ok)"},
    {"MockAdminClient_update_end_offsets", py_MockAdminClient_update_end_offsets, METH_VARARGS, "Mock-only seeding; returns an owned error handle (0 = ok)"},
    {"MockAdminClient_update_consumer_group_offsets", py_MockAdminClient_update_consumer_group_offsets, METH_VARARGS, "Mock-only seeding; returns an owned error handle (0 = ok)"},
    {"MockAdminClient_set_feature_levels", py_MockAdminClient_set_feature_levels, METH_VARARGS, "Mock-only seeding; returns an owned error handle (0 = ok)"},
    {NULL, NULL, 0, NULL}
};

// Module definition
static struct PyModuleDef kafkanativemodule = {
    PyModuleDef_HEAD_INIT,
    "_confluentkafka",
    "Native producer module for Confluent Kafka Rust",
    -1,
    ProducerNativeMethods
};

// Module initialization
PyMODINIT_FUNC PyInit__confluentkafka(void) {
    PyObject* m;

    if (PyType_Ready(&ProducerRecordType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&KafkaFutureType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&ConsumerRecordType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&BorrowedBytesType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&ConsumerRecordsType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&ConsumerGroupMetadataType) < 0) {
        return NULL;
    }

    m = PyModule_Create(&kafkanativemodule);
    if (m == NULL) {
        return NULL;
    }

    Py_INCREF(&ProducerRecordType);
    if (PyModule_AddObject(m, "ProducerRecord", (PyObject*)&ProducerRecordType) < 0) {
        Py_DECREF(&ProducerRecordType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&KafkaFutureType);
    if (PyModule_AddObject(m, "KafkaFuture", (PyObject*)&KafkaFutureType) < 0) {
        Py_DECREF(&KafkaFutureType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&ConsumerRecordType);
    if (PyModule_AddObject(m, "ConsumerRecord", (PyObject*)&ConsumerRecordType) < 0) {
        Py_DECREF(&ConsumerRecordType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&ConsumerRecordsType);
    if (PyModule_AddObject(m, "ConsumerRecords", (PyObject*)&ConsumerRecordsType) < 0) {
        Py_DECREF(&ConsumerRecordsType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&ConsumerGroupMetadataType);
    if (PyModule_AddObject(m, "ConsumerGroupMetadata", (PyObject*)&ConsumerGroupMetadataType) < 0) {
        Py_DECREF(&ConsumerGroupMetadataType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
