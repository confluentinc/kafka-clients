#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <confluent_kafka.h>
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

// ProducerRecord C extension type
typedef struct {
    PyObject_HEAD
    PyObject* key;          // PyBytesObject or Py_None
    PyObject* value;        // PyBytesObject
    char* topic_owned;      // owned copy of topic string
    int32_t partition;      // -1 = unset (Java null)
    int64_t timestamp;      // -1 = unset (Java null)
} ProducerRecordObject;

// The kafka_producer_ProducerRecord_t is opaque and is built per send
// (producer_record_to_c below) over this object's bytes, zero-copy.

static int ProducerRecord_traverse(ProducerRecordObject* self, visitproc visit, void* arg) {
    Py_VISIT(self->key);
    Py_VISIT(self->value);
    return 0;
}

static int ProducerRecord_clear(ProducerRecordObject* self) {
    Py_CLEAR(self->key);
    Py_CLEAR(self->value);
    PyMem_Free(self->topic_owned);
    self->topic_owned = NULL;
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
        self->topic_owned = NULL;
        self->partition = -1;
        self->timestamp = -1;
    }
    return (PyObject*)self;
}

static int ProducerRecord_init(ProducerRecordObject* self, PyObject* args, PyObject* kwds) {
    static char* kwlist[] = {"topic", "value", "key", "partition", "timestamp", NULL};
    const char* topic = NULL;
    PyObject* value = NULL;
    PyObject* key = Py_None;  // Default to Py_None instead of NULL
    int partition = -1;
    long long timestamp = -1;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "sO|OiL", kwlist,
                                      &topic, &value, &key, &partition, &timestamp)) {
        return -1;
    }

    // Validate topic
    if (topic == NULL) {
        PyErr_SetString(PyExc_ValueError, "Topic cannot be None");
        return -1;
    }

    // Validate value
    if (value == NULL || value == Py_None) {
        PyErr_SetString(PyExc_ValueError, "Value cannot be None");
        return -1;
    }
    if (!PyBytes_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "Value must be bytes");
        return -1;
    }

    // Validate key - handle NULL, Py_None, and bytes
    if (key != NULL && key != Py_None && !PyBytes_Check(key)) {
        PyErr_SetString(PyExc_TypeError, "Key must be bytes or None");
        return -1;
    }

    // Normalize NULL to Py_None for consistent handling
    if (key == NULL) {
        key = Py_None;
    }

    // Validate partition
    if (partition < -1) {
        PyErr_Format(PyExc_ValueError,
                     "Invalid partition: %d. Partition number should always be non-negative or null.",
                     partition);
        return -1;
    }

    // Validate timestamp
    if (timestamp < -1) {
        PyErr_Format(PyExc_ValueError,
                     "Invalid timestamp: %lld. Timestamp should always be non-negative or null.",
                     timestamp);
        return -1;
    }

    // Store key and value
    Py_INCREF(key);
    self->key = key;

    Py_INCREF(value);
    self->value = value;

    self->topic_owned = PyMem_Malloc(strlen(topic) + 1);
    if (self->topic_owned == NULL) {
        PyErr_NoMemory();
        Py_DECREF(self->key);
        self->key = NULL;
        Py_DECREF(self->value);
        self->value = NULL;
        return -1;
    }
    strcpy(self->topic_owned, topic);
    self->partition = (int32_t)partition;
    self->timestamp = (int64_t)timestamp;

    return 0;
}

// Getters
static PyObject* ProducerRecord_get_topic(ProducerRecordObject* self, void* closure) {
    if (self->topic_owned == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(self->topic_owned);
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

static PyGetSetDef ProducerRecord_getsetters[] = {
    {"topic", (getter)ProducerRecord_get_topic, NULL, "Topic name", NULL},
    {"partition", (getter)ProducerRecord_get_partition, NULL, "Partition number", NULL},
    {"timestamp", (getter)ProducerRecord_get_timestamp, NULL, "Timestamp", NULL},
    {"key", (getter)ProducerRecord_get_key, NULL, "Message key", NULL},
    {"value", (getter)ProducerRecord_get_value, NULL, "Message value", NULL},
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
// kafka_producer_Producer_send_offsets_to_transaction, then freed exactly once.
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
    // Double-free guard mirrors ConsumerRecords_dealloc.
    if (self->handle != NULL) {
        kafka_consumer_ConsumerGroupMetadata_destroy(self->handle);
        self->handle = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* ConsumerGroupMetadata_get_group_id(ConsumerGroupMetadataObject* self, void* closure) {
    if (self->handle == NULL) Py_RETURN_NONE;
    const char* s = kafka_consumer_ConsumerGroupMetadata_group_id(self->handle);
    return PyUnicode_FromString(s ? s : "");
}

static PyObject* ConsumerGroupMetadata_get_generation_id(ConsumerGroupMetadataObject* self, void* closure) {
    if (self->handle == NULL) Py_RETURN_NONE;
    return PyLong_FromLong(kafka_consumer_ConsumerGroupMetadata_generation_id(self->handle));
}

static PyObject* ConsumerGroupMetadata_get_member_id(ConsumerGroupMetadataObject* self, void* closure) {
    if (self->handle == NULL) Py_RETURN_NONE;
    const char* s = kafka_consumer_ConsumerGroupMetadata_member_id(self->handle);
    return PyUnicode_FromString(s ? s : "");
}

static PyObject* ConsumerGroupMetadata_get_group_instance_id(ConsumerGroupMetadataObject* self, void* closure) {
    if (self->handle == NULL) Py_RETURN_NONE;
    const char* s = kafka_consumer_ConsumerGroupMetadata_group_instance_id(self->handle);
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
// The C layer runs NO background threads of its own (plan decision D8). BOTH
// Python producers drive the `_cb` twins of the blocking-in-Java operations
// (`send_with_callback_cb`, `flush_cb`, `partitions_for_cb`, `close_cb`, the
// transaction-control `_cb` ops); the blocking entry points (`Producer_send`,
// `Producer_flush`, ...) stay exported for completeness but neither Python
// class calls them. Delivery completions and `_cb` completions are QUEUED by
// Rust on the producer's callback vector and only run when this extension
// calls `kafka_producer_Producer_execute_callbacks` (Producer_poll /
// Producer_execute_callbacks), i.e. on the calling thread.
//
//   * The sync `Producer` registers NO Python notify callable. It waits for a
//     completion in 100 ms slices of `Producer_poll(handle, 100)` -- drain,
//     wait on the condvar below (GIL released), drain again -- driven by
//     `python/_sync_wait.py`'s `SyncWaiter`. Between two slices the
//     interpreter runs pending signal handlers, so `Ctrl-C` is honoured
//     promptly (a native `block_on` would defer `KeyboardInterrupt` until the
//     whole call returned); the in-flight operation is let finish before the
//     interrupt is re-raised, so the client stays usable. `poll(timeout)` is
//     sliced the same way, and `SendFuture.result()` registers the future's
//     queued `get_cb` and waits on the same slices. Every callback -- the
//     `_cb` completions and the delivery callbacks -- therefore runs on the
//     thread that is waiting (Java's sender-thread callbacks moved onto the
//     caller).
//   * The asyncio producer registers a Python notify callable that does
//     nothing but `loop.call_soon_threadsafe(pump)`; the pump runs the queued
//     callbacks on the event loop.
//
// The hook installed with `kafka_producer_Producer_set_callbacks_notify` fires
// from a Rust task once per empty->non-empty transition of that vector. It
// only signals: it sets `notified` under `mtx` and broadcasts `cnd` (what the
// sync producer's `Producer_poll` slices wait on) and, when one is set,
// invokes the Python notify callable. It never calls back into the C API.
//
// Ownership conventions (CLAUDE.md §4): `kafka_common_Error_t *` returns are
// owned (NULL = success); a `RecordMetadata_t` read from a future is BORROWED
// from the future (fields are copied, the future is destroyed, the metadata is
// never destroyed); `const` pointers handed to a C interface method are
// borrowed for the duration of the call. Errors and metadata cross into Python
// as plain tuples (`error_to_py`, `metadata_to_py`) so no handle outlives the
// C frame that produced it.
// ===========================================================================

// Defined in the consumer section below; the producer's partitions_for reuses
// it (the FFI returns the same kafka_common_PartitionInfo_t).
static PyObject* partition_info_to_py(const kafka_common_PartitionInfo_t* info);

// A `kafka_producer_Callback_t` implementation: Java's `Callback`. The `void
// *self` Rust hands back is this struct; it lives until `on_completion` fired
// (or, when the send that took it returned Err and the callback can therefore
// never fire, until the producer is destroyed -- see producer_orphan_delivery).
typedef struct DeliveryCtx {
    PyObject* callable;        // Python callable(meta_tuple | None, err_tuple | None)
    PyObject* record;          // keeps the record's key/value bytes alive
    int orphaned;              // the send returned Err: freed at destroy, not here
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
    DeliveryCtx* orphans;      // delivery contexts whose send returned Err (GIL-guarded)
} Producer;

static inline Producer* producer_from_handle(unsigned long long ptr) {
    return (Producer*)(uintptr_t)ptr;
}

// ---- error / metadata -> Python tuples -------------------------------------

// Mirrors KafkaError._from_c field for field, from a BORROWED error:
// (code, message | None, is_retriable, is_fatal, txn_requires_abort).
// `error_is_fatal` (defined with the KafkaError accessors below) composes
// RequestUtils.isFatalException from the exported predicates.
static int error_is_fatal(const kafka_common_Error_t* e);

static PyObject* error_to_py(const kafka_common_Error_t* e) {
    const char* msg = kafka_common_Error_message(e);
    PyObject* py_msg = msg ? PyUnicode_FromString(msg) : (Py_INCREF(Py_None), Py_None);
    if (py_msg == NULL) return NULL;
    return Py_BuildValue("(iNOOO)",
                         (int)kafka_common_Error_code(e),
                         py_msg,
                         kafka_common_Error_is_retriable_error(e) ? Py_True : Py_False,
                         error_is_fatal(e) ? Py_True : Py_False,
                         kafka_common_Error_is_transaction_abortable_error(e) ? Py_True : Py_False);
}

// Same, consuming an OWNED error.
static PyObject* owned_error_to_py(kafka_common_Error_t* e) {
    PyObject* out = error_to_py(e);
    kafka_common_Error_destroy(e);
    return out;
}

// (topic, partition, offset, timestamp, serialized_key_size,
// serialized_value_size) from a BORROWED RecordMetadata; `offset` /
// `timestamp` are -1 when Java's hasOffset() / hasTimestamp() is false.
static PyObject* metadata_to_py(const kafka_producer_RecordMetadata_t* m) {
    const char* topic = kafka_producer_RecordMetadata_topic(m);
    long long offset = kafka_producer_RecordMetadata_has_offset(m)
        ? (long long)kafka_producer_RecordMetadata_offset(m) : -1LL;
    long long timestamp = kafka_producer_RecordMetadata_has_timestamp(m)
        ? (long long)kafka_producer_RecordMetadata_timestamp(m) : -1LL;
    return Py_BuildValue("(siLLii)",
                         topic ? topic : "",
                         (int)kafka_producer_RecordMetadata_partition(m),
                         offset, timestamp,
                         (int)kafka_producer_RecordMetadata_serialized_key_size(m),
                         (int)kafka_producer_RecordMetadata_serialized_value_size(m));
}

// Returns a new reference to a (value, error) pair where `value` is a borrowed
// reference and the error is an owned handle (NULL = None).
static PyObject* build_value_error(PyObject* value, kafka_common_Error_t* err) {
    PyObject* py_err = err ? owned_error_to_py(err) : (Py_INCREF(Py_None), Py_None);
    if (py_err == NULL) return NULL;
    return Py_BuildValue("(ON)", value, py_err);
}

// ---- delivery callback (kafka_producer_Callback_t) -------------------------

static DeliveryCtx* delivery_ctx_new(PyObject* callable, PyObject* record) {
    DeliveryCtx* ctx = (DeliveryCtx*)PyMem_Malloc(sizeof(DeliveryCtx));
    if (ctx == NULL) { PyErr_NoMemory(); return NULL; }
    Py_INCREF(callable);
    Py_XINCREF(record);
    ctx->callable = callable;
    ctx->record = record;
    ctx->orphaned = 0;
    ctx->next = NULL;
    return ctx;
}

static void delivery_ctx_free(DeliveryCtx* ctx) {
    Py_DECREF(ctx->callable);
    Py_XDECREF(ctx->record);
    PyMem_Free(ctx);
}

// A send that returned Err does not fire the delivery callback (the Rust
// producer only fires it from a registered record, which an Err means there is
// not -- `KafkaProducer.doSend`'s rethrow paths drop the callback). The one
// exception is a record that WAS appended before a non-API error surfaced,
// whose future still carries the callback. Rather than reason about that edge
// from C, an Err'd context is parked here and freed at destroy, after
// `_destroy` ran every still-pending callback: a late `on_completion` finds
// the context alive, a never-firing one is reclaimed with the producer.
static void producer_orphan_delivery(Producer* p, DeliveryCtx* ctx) {
    ctx->orphaned = 1;
    ctx->next = p->orphans;
    p->orphans = ctx;
}

// Runs inside kafka_producer_Producer_execute_callbacks, i.e. on the thread
// that called Producer_poll / Producer_execute_callbacks / Producer_destroy,
// with the GIL released by that wrapper. Both arguments are BORROWED; exactly
// one is non-NULL except for a pre-accumulator rejection, which delivers the
// placeholder metadata (offset/partition -1) beside the error, as Java's
// `callback.onCompletion(nullMetadata, e)`.
static void producer_on_completion(void* self_,
                                   const kafka_producer_RecordMetadata_t* metadata,
                                   const kafka_common_Error_t* error) {
    DeliveryCtx* ctx = (DeliveryCtx*)self_;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* meta = metadata ? metadata_to_py(metadata) : (Py_INCREF(Py_None), Py_None);
    PyObject* err = error ? error_to_py(error) : (Py_INCREF(Py_None), Py_None);
    if (meta != NULL && err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->callable, meta, err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->callable); else Py_DECREF(r);
    } else {
        PyErr_WriteUnraisable(ctx->callable);
    }
    Py_XDECREF(meta);
    Py_XDECREF(err);
    if (!ctx->orphaned) delivery_ctx_free(ctx);
    PyGILState_Release(g);
}

// ---- notify hook -----------------------------------------------------------

// Fired by Rust once each time the callback vector goes from empty to
// non-empty, from a Rust task (or from the calling thread when a blocking call
// completes a record synchronously -- the mock with auto_complete). It only
// signals. `notify_cb` is read without the GIL: it is set once before any send
// can queue a completion and released only after the class handle is destroyed
// (no hook can fire past that point), so the pointer is stable here.
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
        PyGILState_STATE g = PyGILState_Ensure();
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

// MockProducer_new(auto_complete: bool, notify_cb: callable | None) -> handle
static PyObject* py_MockProducer_new(PyObject* self, PyObject* args) {
    int auto_complete;
    PyObject* notify_cb = Py_None;
    if (!PyArg_ParseTuple(args, "p|O", &auto_complete, &notify_cb)) return NULL;
    Producer* p = producer_alloc(notify_cb);
    if (p == NULL) return NULL;
    p->is_mock = 1;
    p->mp = kafka_producer_MockProducer_with_auto_complete((int8_t)(auto_complete ? 1 : 0));
    p->producer = kafka_producer_MockProducer__as_Producer(p->mp);
    kafka_producer_Producer_set_callbacks_notify(p->producer, producer_callbacks_notify, p);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)p);
}

// KafkaProducer_new(config: dict[str, str], notify_cb: callable | None) -> handle
// Raises RuntimeError with the Rust error's message when the config is
// rejected (the pre-existing contract of KafkaProducer({...bad config...})).
static PyObject* py_KafkaProducer_new(PyObject* self, PyObject* args) {
    PyObject* config_dict;
    PyObject* notify_cb = Py_None;
    if (!PyArg_ParseTuple(args, "O|O", &config_dict, &notify_cb)) return NULL;
    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }

    // A C-built kafka_Map_t holds BORROWED char* -> char*: the strings stay
    // owned by the dict's str objects, which outlive this call.
    kafka_Map_t* props = kafka_Map_new();
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_Check(key) ? PyUnicode_AsUTF8(key) : NULL;
        const char* v = PyUnicode_Check(value) ? PyUnicode_AsUTF8(value) : NULL;
        if (k == NULL || v == NULL) {
            kafka_Map_destroy(props);
            if (!PyErr_Occurred())
                PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
        kafka_Map_put(props, (void*)k, (void*)v);
    }

    kafka_producer_ProducerConfig_t* config = NULL;
    kafka_common_Error_t* err = kafka_producer_ProducerConfig_new(props, &config);
    kafka_Map_destroy(props);
    kafka_producer_KafkaProducer_t* kp = NULL;
    if (err == NULL) {
        // NULL serializers: key and value cross as kafka_Bytes_t *.
        Py_BEGIN_ALLOW_THREADS
        err = kafka_producer_KafkaProducer_new(config, NULL, NULL, &kp);
        Py_END_ALLOW_THREADS
        kafka_producer_ProducerConfig_destroy(config);
    }
    if (err != NULL) {
        const char* msg = kafka_common_Error_message(err);
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create KafkaProducer");
        kafka_common_Error_destroy(err);
        return NULL;
    }

    Producer* p = producer_alloc(notify_cb);
    if (p == NULL) {
        kafka_producer_KafkaProducer_destroy(kp);
        return NULL;
    }
    p->kp = kp;
    p->producer = kafka_producer_KafkaProducer__as_Producer(kp);
    kafka_producer_Producer_set_callbacks_notify(p->producer, producer_callbacks_notify, p);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)p);
}

// Producer_close(handle) -> err_tuple | None. Java close(): blocks until the
// in-flight records are delivered. Does not free anything.
static PyObject* py_Producer_close(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_close(p->producer);
    Py_END_ALLOW_THREADS
    if (err == NULL) Py_RETURN_NONE;
    return owned_error_to_py(err);
}

// Producer_destroy(handle): frees the class handle (which stops the submission
// task, waits for the `_cb` tasks and runs every still-pending callback exactly
// once -- on this thread) and then the C struct. Call Producer_close first for
// a graceful close.
static PyObject* py_Producer_destroy(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    Py_BEGIN_ALLOW_THREADS
    if (p->is_mock) kafka_producer_MockProducer_destroy(p->mp);
    else kafka_producer_KafkaProducer_destroy(p->kp);
    Py_END_ALLOW_THREADS
    // No callback can fire past this point: reclaim the Err'd contexts.
    while (p->orphans != NULL) {
        DeliveryCtx* next = p->orphans->next;
        delivery_ctx_free(p->orphans);
        p->orphans = next;
    }
    Py_XDECREF(p->notify_cb);
    mtx_destroy(&p->mtx);
    cnd_destroy(&p->cnd);
    PyMem_Free(p);
    Py_RETURN_NONE;
}

// ---- callback pump ---------------------------------------------------------

// Producer_execute_callbacks(handle) -> int: runs the queued callbacks on this
// thread (with the GIL released around the drain; each callback re-acquires
// it) and returns how many ran.
static PyObject* py_Producer_execute_callbacks(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    int32_t n;
    Py_BEGIN_ALLOW_THREADS
    n = kafka_producer_Producer_execute_callbacks(p->producer);
    Py_END_ALLOW_THREADS
    return PyLong_FromLong((long)n);
}

// Producer_poll(handle, timeout_ms) -> int: the sync producer's pump. Runs the
// queued callbacks; when none were queued and timeout_ms != 0, waits on the
// notify condition (up to timeout_ms, forever when negative) and drains again.
// `notified` is cleared BEFORE the first drain so a notification that races
// the drain is not lost, and a notification already consumed by the drain
// costs at most one spurious early return.
static PyObject* py_Producer_poll(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KL", &ptr, &timeout_ms)) return NULL;
    Producer* p = producer_from_handle(ptr);
    int32_t n;
    Py_BEGIN_ALLOW_THREADS
    mtx_lock(&p->mtx);
    p->notified = 0;
    mtx_unlock(&p->mtx);
    n = kafka_producer_Producer_execute_callbacks(p->producer);
    if (n == 0 && timeout_ms != 0) {
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
        n = kafka_producer_Producer_execute_callbacks(p->producer);
    }
    Py_END_ALLOW_THREADS
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
    if (self->handle != NULL) {
        kafka_common_KafkaFuture_destroy(self->handle);
        self->handle = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* KafkaFuture_wrap(kafka_common_KafkaFuture_t* handle);

// get(timeout_ms=None) -> (meta_tuple | None, err_tuple | None), or None when
// the wait timed out (Java's java.util.concurrent.TimeoutException, i.e.
// kafka_common_Error_is_local_timeout_error on a future that is not done).
static PyObject* KafkaFuture_get(KafkaFutureObject* self, PyObject* args) {
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
        err = kafka_common_KafkaFuture_get(self->handle, &out);
    } else {
        err = kafka_common_KafkaFuture_get_with_timeout(self->handle, (int64_t)timeout_ms, &out);
    }
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (kafka_common_Error_is_local_timeout_error(err) &&
            !kafka_common_KafkaFuture_is_done(self->handle)) {
            kafka_common_Error_destroy(err);
            Py_RETURN_NONE;
        }
        return build_value_error(Py_None, err);
    }
    PyObject* meta = metadata_to_py((const kafka_producer_RecordMetadata_t*)out);
    if (meta == NULL) return NULL;
    return Py_BuildValue("(NO)", meta, Py_None);
}

static PyObject* KafkaFuture_is_done(KafkaFutureObject* self, PyObject* Py_UNUSED(ignored)) {
    return PyBool_FromLong(kafka_common_KafkaFuture_is_done(self->handle) ? 1 : 0);
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
    PyGILState_STATE g = PyGILState_Ensure();
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
// (`Producer_execute_callbacks` / `Producer_poll`) once the record completed,
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
    KafkaFutureObject* f = PyObject_New(KafkaFutureObject, &KafkaFutureType);
    if (f == NULL) { kafka_common_KafkaFuture_destroy(handle); return NULL; }
    f->handle = handle;
    return (PyObject*)f;
}

// ---- ProducerRecord -> kafka_producer_ProducerRecord_t ---------------------

// Builds the C record over the Python record's bytes, zero-copy: `key_b` /
// `value_b` (caller storage) point straight into the bytes objects, so they --
// and the Python record -- must stay alive for as long as Rust may read them:
// the duration of a blocking send, or until the `_cb` completion fires for a
// queued one. A None key crosses as a NULL key pointer (Java null); -1
// partition / timestamp are the constructors' "unset".
static kafka_producer_ProducerRecord_t* producer_record_to_c(ProducerRecordObject* rec,
                                                             kafka_Bytes_t* key_b,
                                                             kafka_Bytes_t* value_b,
                                                             kafka_common_Error_t** out_err) {
    const void* key_ptr = NULL;
    if (rec->key != NULL && rec->key != Py_None) {
        key_b->data = (const uint8_t*)PyBytes_AS_STRING(rec->key);
        key_b->len = (int32_t)PyBytes_GET_SIZE(rec->key);
        key_ptr = key_b;
    }
    value_b->data = (const uint8_t*)PyBytes_AS_STRING(rec->value);
    value_b->len = (int32_t)PyBytes_GET_SIZE(rec->value);
    kafka_producer_ProducerRecord_t* out = NULL;
    *out_err = kafka_producer_ProducerRecord_with_partition_timestamp_key(
        rec->topic_owned, rec->partition, rec->timestamp, key_ptr, value_b, &out);
    return *out_err == NULL ? out : NULL;
}

// ---- send (blocking) -------------------------------------------------------

// Producer_send(handle, record, on_completion | None)
//   -> (KafkaFuture | None, err_tuple | None)
// Blocks (GIL released) until the record is registered, as Java's send(). The
// on_completion callable is registered as a kafka_producer_Callback_t and is
// run by Producer_poll / Producer_execute_callbacks / Producer_destroy on the
// calling thread, exactly once; it receives (meta_tuple | None, err_tuple | None).
static PyObject* py_Producer_send(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    PyObject* rec_obj;
    PyObject* callable;
    if (!PyArg_ParseTuple(args, "KO!O", &ptr, &ProducerRecordType, &rec_obj, &callable)) return NULL;
    Producer* p = producer_from_handle(ptr);

    kafka_Bytes_t key_b = {NULL, 0}, value_b = {NULL, 0};
    kafka_common_Error_t* err = NULL;
    kafka_producer_ProducerRecord_t* rec =
        producer_record_to_c((ProducerRecordObject*)rec_obj, &key_b, &value_b, &err);
    if (rec == NULL) return build_value_error(Py_None, err);

    DeliveryCtx* ctx = NULL;
    kafka_producer_Callback_t* cb = NULL;
    if (callable != Py_None) {
        ctx = delivery_ctx_new(callable, rec_obj);
        if (ctx == NULL) { kafka_producer_ProducerRecord_destroy(rec); return NULL; }
        cb = kafka_producer_Callback_new(ctx, producer_on_completion);
    }

    kafka_common_KafkaFuture_t* fut = NULL;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_send_with_callback(p->producer, rec, cb, &fut);
    Py_END_ALLOW_THREADS
    // Both are read during the call only (the record was cloned, the callback
    // registration copied into the Rust closure).
    if (cb != NULL) kafka_producer_Callback_destroy(cb);
    kafka_producer_ProducerRecord_destroy(rec);

    if (err != NULL) {
        if (ctx != NULL) producer_orphan_delivery(p, ctx);
        return build_value_error(Py_None, err);
    }
    PyObject* py_fut = KafkaFuture_wrap(fut);
    if (py_fut == NULL) return NULL;
    return Py_BuildValue("(NO)", py_fut, Py_None);
}

// ---- send (queued, asyncio) ------------------------------------------------

// The completion of a `send_with_callback_cb`: the record handle, its bytes
// (key_b / value_b, pointed at by the C record's key/value void *) and the
// Python record stay alive until `cb` fires. `delivery` is only touched when
// the send FAILED (the Callback can then never fire) -- on success it is owned
// by the pending on_completion, which may already have run.
typedef struct {
    PyObject* resolve;         // Python callable(KafkaFuture | None, err_tuple | None)
    PyObject* record;
    kafka_producer_ProducerRecord_t* c_record;
    kafka_Bytes_t key_b;
    kafka_Bytes_t value_b;
    DeliveryCtx* delivery;     // may be NULL
    Producer* producer;
} SendCtx;

static void producer_send_cb(kafka_common_KafkaFuture_t* value, kafka_common_Error_t* error, void* opaque) {
    SendCtx* ctx = (SendCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* py_fut = value ? KafkaFuture_wrap(value) : (Py_INCREF(Py_None), Py_None);
    PyObject* py_err = error ? owned_error_to_py(error) : (Py_INCREF(Py_None), Py_None);
    if (error != NULL && ctx->delivery != NULL) producer_orphan_delivery(ctx->producer, ctx->delivery);
    if (py_fut != NULL && py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->resolve, py_fut, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->resolve); else Py_DECREF(r);
    } else {
        PyErr_WriteUnraisable(ctx->resolve);
    }
    Py_XDECREF(py_fut);
    Py_XDECREF(py_err);
    kafka_producer_ProducerRecord_destroy(ctx->c_record);
    Py_DECREF(ctx->record);
    Py_DECREF(ctx->resolve);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// Producer_send_cb(handle, record, on_completion | None, resolve) -> None
// Queues the record on the producer's submission task and returns at once;
// `resolve(KafkaFuture | None, err_tuple | None)` runs from the callback pump
// once the record is registered (or rejected). `on_completion`, when given,
// is registered exactly as for Producer_send.
static PyObject* py_Producer_send_cb(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    PyObject* rec_obj;
    PyObject* callable;
    PyObject* resolve;
    if (!PyArg_ParseTuple(args, "KO!OO", &ptr, &ProducerRecordType, &rec_obj, &callable, &resolve)) return NULL;
    Producer* p = producer_from_handle(ptr);

    SendCtx* ctx = (SendCtx*)PyMem_Malloc(sizeof(SendCtx));
    if (ctx == NULL) return PyErr_NoMemory();
    memset(ctx, 0, sizeof(*ctx));
    kafka_common_Error_t* err = NULL;
    ctx->c_record = producer_record_to_c((ProducerRecordObject*)rec_obj, &ctx->key_b, &ctx->value_b, &err);
    if (ctx->c_record == NULL) {
        PyMem_Free(ctx);
        PyObject* py_err = owned_error_to_py(err);
        if (py_err == NULL) return NULL;
        // Deliver the rejection through resolve(), exactly like a queued failure.
        PyObject* r = PyObject_CallFunctionObjArgs(resolve, Py_None, py_err, NULL);
        Py_DECREF(py_err);
        if (r == NULL) return NULL;
        Py_DECREF(r);
        Py_RETURN_NONE;
    }
    Py_INCREF(resolve);
    Py_INCREF(rec_obj);
    ctx->resolve = resolve;
    ctx->record = rec_obj;
    ctx->producer = p;

    kafka_producer_Callback_t* cb = NULL;
    if (callable != Py_None) {
        ctx->delivery = delivery_ctx_new(callable, rec_obj);
        if (ctx->delivery == NULL) {
            kafka_producer_ProducerRecord_destroy(ctx->c_record);
            Py_DECREF(ctx->resolve);
            Py_DECREF(ctx->record);
            PyMem_Free(ctx);
            return NULL;
        }
        cb = kafka_producer_Callback_new(ctx->delivery, producer_on_completion);
    }
    Py_BEGIN_ALLOW_THREADS
    kafka_producer_Producer_send_with_callback_cb(p->producer, ctx->c_record, cb, producer_send_cb, ctx);
    Py_END_ALLOW_THREADS
    if (cb != NULL) kafka_producer_Callback_destroy(cb);
    Py_RETURN_NONE;
}

// ---- void ops: blocking + _cb ----------------------------------------------

typedef kafka_common_Error_t* (*producer_void_op_fn)(const kafka_producer_Producer_t*);

// Shared body of the blocking void ops: GIL released around the call, the
// owned error (or None) returned as a tuple for KafkaError._from_parts.
static PyObject* producer_void_op(PyObject* args, producer_void_op_fn op) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = op(p->producer);
    Py_END_ALLOW_THREADS
    if (err == NULL) Py_RETURN_NONE;
    return owned_error_to_py(err);
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

// A queued void op's completion: `cb(err_tuple | None)` runs from the pump.
typedef struct {
    PyObject* cb;
} VoidCbCtx;

static void producer_void_cb(kafka_common_Error_t* error, void* opaque) {
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* py_err = error ? owned_error_to_py(error) : (Py_INCREF(Py_None), Py_None);
    if (py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
        Py_DECREF(py_err);
    } else {
        PyErr_WriteUnraisable(ctx->cb);
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
    Py_RETURN_NONE;
}

static PyObject* py_Producer_flush_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_flush_cb);
}
static PyObject* py_Producer_init_transactions_cb(PyObject* self, PyObject* args) {
    return producer_void_cb_op(args, kafka_producer_Producer_init_transactions_cb);
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
    for (Py_ssize_t i = 0; i < m->n; i++) {
        if (m->oams && m->oams[i]) kafka_consumer_OffsetAndMetadata_destroy(m->oams[i]);
        if (m->tps && m->tps[i]) kafka_common_TopicPartition_destroy(m->tps[i]);
    }
    PyMem_Free(m->tps);
    PyMem_Free(m->oams);
    if (m->map) kafka_Map_destroy(m->map);
}

// Returns 0 on success (exception set otherwise).
static int offsets_map_build(PyObject* spec, offsets_map_t* m) {
    memset(m, 0, sizeof(*m));
    offset_arrays_t a;
    Py_ssize_t n = offsets_to_arrays(spec, &a);
    if (n < 0) return -1;
    m->map = kafka_Map_new();
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
        m->tps[i] = kafka_common_TopicPartition_new(a.topics[i], a.parts[i]);
        // A negative leader_epoch is Optional.empty(); a NULL metadata is
        // Java null, stored by the class as "".
        kafka_common_Error_t* err = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
            a.offs[i], a.epochs[i], a.metas[i], &m->oams[i]);
        if (err != NULL) {
            const char* msg = kafka_common_Error_message(err);
            PyErr_SetString(PyExc_ValueError, msg ? msg : "invalid OffsetAndMetadata");
            kafka_common_Error_destroy(err);
            offset_arrays_free(&a);
            offsets_map_free(m);
            return -1;
        }
        kafka_Map_put(m->map, m->tps[i], m->oams[i]);
    }
    offset_arrays_free(&a);
    return 0;
}

// Producer_send_offsets_to_transaction(handle, spec, group_metadata)
//   -> err_tuple | None (blocking)
static PyObject* py_Producer_send_offsets_to_transaction(PyObject* self, PyObject* args) {
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
    err = kafka_producer_Producer_send_offsets_to_transaction(p->producer, m.map, gm);
    Py_END_ALLOW_THREADS
    offsets_map_free(&m);
    if (err == NULL) Py_RETURN_NONE;
    return owned_error_to_py(err);
}

// Producer_send_offsets_to_transaction_cb(handle, spec, group_metadata, cb)
// The offsets map and group metadata are read during the call (the Rust side
// clones them for the submission task), so they are freed right after it.
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
    if (infos == NULL) return PyList_New(0);
    int32_t n = kafka_List_size(infos);
    PyObject* out = PyList_New(n);
    if (out == NULL) { kafka_List_destroy(infos); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* item = partition_info_to_py((const kafka_common_PartitionInfo_t*)kafka_List_get(infos, i));
        if (item == NULL) { Py_DECREF(out); kafka_List_destroy(infos); return NULL; }
        PyList_SET_ITEM(out, i, item);
    }
    kafka_List_destroy(infos);
    return out;
}

// Producer_partitions_for(handle, topic) -> (list | None, err_tuple | None)
static PyObject* py_Producer_partitions_for(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    const char* topic;
    if (!PyArg_ParseTuple(args, "Ks", &ptr, &topic)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_List_t* infos = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_partitions_for(p->producer, topic, &infos);
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (infos != NULL) kafka_List_destroy(infos);
        return build_value_error(Py_None, err);
    }
    PyObject* list = partition_info_list_to_py(infos);
    if (list == NULL) return NULL;
    return Py_BuildValue("(NO)", list, Py_None);
}

static void producer_partitions_for_cb(kafka_List_t* value, kafka_common_Error_t* error, void* opaque) {
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* py_list = NULL;
    PyObject* py_err = NULL;
    if (error != NULL) {
        if (value != NULL) kafka_List_destroy(value);
        py_err = owned_error_to_py(error);
        py_list = (Py_INCREF(Py_None), Py_None);
    } else {
        py_list = partition_info_list_to_py(value);
        py_err = (Py_INCREF(Py_None), Py_None);
    }
    if (py_list != NULL && py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_list, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
    } else {
        PyErr_WriteUnraisable(ctx->cb);
    }
    Py_XDECREF(py_list);
    Py_XDECREF(py_err);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// Producer_partitions_for_cb(handle, topic, cb); cb(list | None, err_tuple | None)
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
// kafka_common_MetricValue__enum; `value` is read with the matching typed
// accessor (float / str / int / int). `as_string` is borrowed until the
// MetricValue handle is destroyed, so it is copied into a str first.
// Converts an OWNED metrics map (kafka_common_MetricName_t* ->
// kafka_common_metrics_KafkaMetric_t*, both owned by the map) into the
// list[dict] shape above and frees it. Shared with the consumer's metrics().
static PyObject* metrics_map_to_py(kafka_Map_t* map) {
    if (map == NULL) return PyList_New(0);
    int32_t n = kafka_Map_size(map);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_Map_destroy(map); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_common_MetricName_t* name =
            (const kafka_common_MetricName_t*)kafka_Map_key(map, i);
        const kafka_common_metrics_KafkaMetric_t* metric =
            (const kafka_common_metrics_KafkaMetric_t*)kafka_Map_value(map, i);

        PyObject* tags = PyDict_New();
        if (tags == NULL) goto fail;
        kafka_Map_t* tag_map = kafka_common_MetricName_tags(name);  // owned, char* -> char*
        if (tag_map != NULL) {
            int32_t tn = kafka_Map_size(tag_map);
            for (int32_t t = 0; t < tn; t++) {
                const char* k = (const char*)kafka_Map_key(tag_map, t);
                const char* v = (const char*)kafka_Map_value(tag_map, t);
                PyObject* pv = PyUnicode_FromString(v ? v : "");
                if (pv == NULL || PyDict_SetItemString(tags, k ? k : "", pv) != 0) {
                    Py_XDECREF(pv); Py_DECREF(tags); kafka_Map_destroy(tag_map); goto fail;
                }
                Py_DECREF(pv);
            }
            kafka_Map_destroy(tag_map);
        }

        PyObject* value = NULL;
        int kind = 0;
        kafka_common_MetricValue_t* mv =
            kafka_common_Metric_metric_value(kafka_common_metrics_KafkaMetric__as_Metric(metric));
        if (mv != NULL) {
            switch (kafka_common_MetricValue__enum(mv)) {
                case kafka_common_MetricValue_e_string: {
                    kind = 1;
                    const char* s = kafka_common_MetricValue_as_string(mv);
                    value = s ? PyUnicode_FromString(s) : (Py_INCREF(Py_None), Py_None);
                    break;
                }
                case kafka_common_MetricValue_e_long_:
                    kind = 2;
                    value = PyLong_FromLongLong((long long)kafka_common_MetricValue_as_long(mv));
                    break;
                case kafka_common_MetricValue_e_int_:
                    kind = 3;
                    value = PyLong_FromLong((long)kafka_common_MetricValue_as_int(mv));
                    break;
                default:
                    kind = 0;
                    value = PyFloat_FromDouble(kafka_common_MetricValue_as_double(mv));
                    break;
            }
            kafka_common_MetricValue_destroy(mv);
        } else {
            value = (Py_INCREF(Py_None), Py_None);
        }
        if (value == NULL) { Py_DECREF(tags); goto fail; }

        const char* mname = kafka_common_MetricName_name(name);
        const char* group = kafka_common_MetricName_group(name);
        const char* desc = kafka_common_MetricName_description(name);
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
    kafka_Map_destroy(map);
    return out;
fail:
    Py_DECREF(out);
    kafka_Map_destroy(map);
    return NULL;
}

static PyObject* py_Producer_metrics(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    return metrics_map_to_py(kafka_producer_Producer_metrics(p->producer));  // owned, owns its entries
}

// ---- MockProducer hooks ----------------------------------------------------

// Builds the error a mock test hook injects. There is no factory from a
// numeric code -- an error is a class, built by the kafka_common_Error_*
// factory of that class -- so the codes the Python tests use are mapped to
// their class here; anything else raises ValueError. `message` may be NULL.
static kafka_common_Error_t* mock_error_from_code(int code, const char* message) {
    switch (code) {
        case kafka_common_ErrorCode_e_LOCAL_TIMEOUT:
            return kafka_common_Error_local_timeout(message);
        case kafka_common_ErrorCode_e_LOCAL_ILLEGAL_STATE:
            return kafka_common_Error_local_illegal_state(message);
        case kafka_common_ErrorCode_e_LOCAL_ILLEGAL_ARGUMENT:
            return kafka_common_Error_local_illegal_argument(message);
        case kafka_common_ErrorCode_e_LOCAL_CONCURRENT_MODIFICATION:
            return kafka_common_Error_local_concurrent_modification(message);
        case kafka_common_ErrorCode_e_REQUEST_TIMED_OUT:
            return kafka_common_Error_timeout(message);
        case kafka_common_ErrorCode_e_MESSAGE_TOO_LARGE:
            return kafka_common_Error_record_too_large(message);
        case kafka_common_ErrorCode_e_RECORD_LIST_TOO_LARGE:
            return kafka_common_Error_record_batch_too_large(message);
        case kafka_common_ErrorCode_e_INVALID_GROUP_ID:
            return kafka_common_Error_invalid_group_id(message);
        case kafka_common_ErrorCode_e_UNSUPPORTED_VERSION:
            return kafka_common_Error_unsupported_version(message);
        case kafka_common_ErrorCode_e_THROTTLING_QUOTA_EXCEEDED:
            return kafka_common_Error_throttling_quota_exceeded(0, message);
        default:
            PyErr_Format(PyExc_ValueError,
                         "no kafka_common_Error_* factory is mapped for error code %d; "
                         "see mock_error_from_code in _confluentkafka.c", code);
            return NULL;
    }
}

static PyObject* py_MockProducer_complete_next(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    int8_t r;
    Py_BEGIN_ALLOW_THREADS
    r = kafka_producer_MockProducer_complete_next(p->mp);
    Py_END_ALLOW_THREADS
    return PyBool_FromLong(r ? 1 : 0);
}

// MockProducer_error_next(handle, code, message | None) -> bool
static PyObject* py_MockProducer_error_next(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    int code;
    const char* message = NULL;
    if (!PyArg_ParseTuple(args, "Kiz", &ptr, &code, &message)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_Error_t* err = mock_error_from_code(code, message);
    if (err == NULL) return NULL;
    int8_t r;
    Py_BEGIN_ALLOW_THREADS
    r = kafka_producer_MockProducer_error_next(p->mp, err);  // ownership moves to the mock
    Py_END_ALLOW_THREADS
    return PyBool_FromLong(r ? 1 : 0);
}

static PyObject* py_MockProducer_history_count(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_List_t* history = kafka_producer_MockProducer_history(p->mp);  // owned
    int32_t n = history ? kafka_List_size(history) : 0;
    if (history) kafka_List_destroy(history);
    return PyLong_FromLong((long)n);
}

static PyObject* py_MockProducer_clear(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_producer_MockProducer_clear(p->mp);
    Py_RETURN_NONE;
}

// MockProducer_set_commit_transaction_error(handle, code | None, message | None)
// None clears the injected error.
static PyObject* py_MockProducer_set_commit_transaction_error(PyObject* self, PyObject* args) {
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
    kafka_producer_MockProducer_set_commit_transaction_error(p->mp, err);  // ownership moves
    Py_RETURN_NONE;
}

static PyObject* py_MockProducer_sent_offsets(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    Producer* p = producer_from_handle(ptr);
    return PyBool_FromLong(kafka_producer_MockProducer_sent_offsets(p->mp) ? 1 : 0);
}

// MockProducer_committed_offset(handle, group_id, topic, partition)
//   -> (offset, leader_epoch | None, metadata) or None
static PyObject* py_MockProducer_committed_offset(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    const char* group;
    const char* topic;
    int partition;
    if (!PyArg_ParseTuple(args, "Kssi", &ptr, &group, &topic, &partition)) return NULL;
    Producer* p = producer_from_handle(ptr);
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, partition);
    kafka_consumer_OffsetAndMetadata_t* oam =
        kafka_producer_MockProducer_committed_offset(p->mp, group, tp);  // owned or NULL
    kafka_common_TopicPartition_destroy(tp);
    if (oam == NULL) Py_RETURN_NONE;
    int32_t epoch = kafka_consumer_OffsetAndMetadata_leader_epoch(oam);  // -1 = Optional.empty()
    const char* metadata = kafka_consumer_OffsetAndMetadata_metadata(oam);
    PyObject* py_epoch = epoch >= 0 ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None);
    PyObject* out = py_epoch ? Py_BuildValue("(LNs)",
                                             (long long)kafka_consumer_OffsetAndMetadata_offset(oam),
                                             py_epoch,
                                             metadata ? metadata : "")
                             : NULL;
    kafka_consumer_OffsetAndMetadata_destroy(oam);
    return out;
}

// KafkaError accessor/destroy functions
static PyObject* py_KafkaError_code(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    return PyLong_FromLong(kafka_common_Error_code(e));
}

static PyObject* py_KafkaError_message(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    const char *msg = kafka_common_Error_message(e);
    if (msg == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(msg);
}

static PyObject* py_KafkaError_is_retriable(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    return PyBool_FromLong(kafka_common_Error_is_retriable_error(e) ? 1 : 0);
}

// `RequestUtils.isFatalException` composed from the exported predicates and the
// error code, which is what the FFI's own note on
// kafka_common_Error_is_retriable_error prescribes: fatality lives in
// `org.apache.kafka.common.requests`, a package Kafka disclaims as unsupported,
// so CLAUDE.md §4 forbids giving it a C binding of its own.
//
// The composition mirrors `request_utils::is_fatal_error` term for term — the
// two authorization/authentication predicates, plus the five coded classes it
// lists — so `KafkaError.is_fatal`, which this backs, keeps answering exactly
// what it did before.
static int error_is_fatal(const kafka_common_Error_t* e) {
    if (e == NULL) return 0;
    if (kafka_common_Error_is_authentication_error(e)) return 1;
    if (kafka_common_Error_is_authorization_error(e)) return 1;
    switch (kafka_common_Error_code(e)) {
        case kafka_common_ErrorCode_e_MISMATCHED_ENDPOINT_TYPE:
        case kafka_common_ErrorCode_e_SECURITY_DISABLED:
        case kafka_common_ErrorCode_e_UNSUPPORTED_VERSION:
        case kafka_common_ErrorCode_e_UNSUPPORTED_ENDPOINT_TYPE:
        case kafka_common_ErrorCode_e_UNSUPPORTED_FOR_MESSAGE_FORMAT:
            return 1;
        default:
            return 0;
    }
}

static PyObject* py_KafkaError_is_fatal(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    return PyBool_FromLong(error_is_fatal(e) ? 1 : 0);
}

// Java's `TransactionAbortableException` test. The Python-visible name stays
// `txn_requires_abort` (librdkafka's spelling, and the public API); only the
// FFI symbol it calls was renamed by the error redesign.
static PyObject* py_KafkaError_txn_requires_abort(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    return PyBool_FromLong(kafka_common_Error_is_transaction_abortable_error(e) ? 1 : 0);
}

static PyObject* py_KafkaError_destroy(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_Error_t *e = (kafka_common_Error_t*)(uintptr_t)ptr;
    kafka_common_Error_destroy(e);
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
//     vector, run only by `Consumer_execute_callbacks` (the asyncio pump).
//     `Consumer_set_callbacks_notify` registers the Python callable the Rust
//     notify hook fires once each time the vector goes from empty to non-empty
//     (it may only schedule the pump, never run callbacks).
//   * Interface methods carry an `int64_t callback_id` and are reported through
//     `kafka_consumer_Consumer_set_callback_result`, synchronously by the
//     trampoline when the Python adapter returns, or later from Python
//     (`Consumer_set_callback_result`) when the adapter deferred (a coroutine).
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
    if (node == NULL) Py_RETURN_NONE;
    int32_t id = kafka_common_Node_id(node);
    const char* host = kafka_common_Node_host(node);  // NUL-terminated, borrowed
    int32_t port = kafka_common_Node_port(node);
    const char* rack = kafka_common_Node_rack(node);  // NULL when Java's rack is null
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
    if (nodes == NULL) Py_RETURN_NONE;
    int32_t n = kafka_List_size(nodes);
    PyObject* out = PyList_New(n);
    if (out == NULL) { kafka_List_destroy(nodes); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* item = node_to_py((const kafka_common_Node_t*)kafka_List_get(nodes, i));
        if (item == NULL) { Py_DECREF(out); kafka_List_destroy(nodes); return NULL; }
        PyList_SET_ITEM(out, i, item);
    }
    kafka_List_destroy(nodes);
    return out;
}

static PyObject* partition_info_to_py(const kafka_common_PartitionInfo_t* info) {
    const char* topic = kafka_common_PartitionInfo_topic(info);  // NUL-terminated
    int32_t partition = kafka_common_PartitionInfo_partition(info);
    PyObject* leader = node_to_py(kafka_common_PartitionInfo_leader(info));
    PyObject* replicas = node_list_to_py(kafka_common_PartitionInfo_replicas(info));
    PyObject* isr = node_list_to_py(kafka_common_PartitionInfo_in_sync_replicas(info));
    PyObject* offline = node_list_to_py(kafka_common_PartitionInfo_offline_replicas(info));
    PyObject* py_topic = PyUnicode_FromString(topic ? topic : "");
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
    if (infos == NULL) return PyList_New(0);
    int32_t n = kafka_List_size(infos);
    PyObject* out = PyList_New(n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* item = partition_info_to_py((const kafka_common_PartitionInfo_t*)kafka_List_get(infos, i));
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
    PyObject *type = NULL, *value = NULL, *tb = NULL;
    PyErr_Fetch(&type, &value, &tb);  // clears the indicator
    PyErr_NormalizeException(&type, &value, &tb);
    PyObject* text = value ? PyObject_Str(value) : NULL;
    const char* msg = text ? PyUnicode_AsUTF8(text) : NULL;
    kafka_common_Error_t* err = kafka_common_Error_kafka_message(msg ? msg : fallback);
    Py_XDECREF(text);
    Py_XDECREF(type); Py_XDECREF(value); Py_XDECREF(tb);
    PyErr_Clear();  // defensive: PyObject_Str / NormalizeException may re-set it
    return err;
}

// NULL -> None; an owned error -> its tuple (the error is destroyed).
static PyObject* err_result(kafka_common_Error_t* err) {
    if (err == NULL) Py_RETURN_NONE;
    return owned_error_to_py(err);
}

// ---- conversions: C values -> Python ---------------------------------------

// kafka_Bytes_t* -> bytes (copied) or None for NULL / Java null.
static PyObject* bytes_to_py(const kafka_Bytes_t* b) {
    if (b == NULL || b->data == NULL) Py_RETURN_NONE;
    return PyBytes_FromStringAndSize((const char*)b->data, (Py_ssize_t)b->len);
}

static PyObject* tp_to_py(const kafka_common_TopicPartition_t* tp) {
    const char* topic = kafka_common_TopicPartition_topic(tp);
    return Py_BuildValue("(si)", topic ? topic : "", (int)kafka_common_TopicPartition_partition(tp));
}

// BORROWED list of TopicPartition_t* -> list[(topic, partition)].
static PyObject* tp_list_to_py_borrowed(const kafka_List_t* list) {
    if (list == NULL) return PyList_New(0);
    int32_t n = kafka_List_size(list);
    PyObject* out = PyList_New(n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* t = tp_to_py((const kafka_common_TopicPartition_t*)kafka_List_get(list, i));
        if (t == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, t);
    }
    return out;
}

// OWNED list of owned TopicPartition_t* -> list[(topic, partition)]; frees it.
// NULL (never returned by the FFI, which gives empty containers while an
// operation is in flight) is treated as empty.
static PyObject* tp_list_to_py(kafka_List_t* list) {
    PyObject* out = tp_list_to_py_borrowed(list);
    if (list) kafka_List_destroy(list);
    return out;
}

// OWNED list of owned char* -> list[str]; frees it.
static PyObject* string_list_to_py(kafka_List_t* list) {
    if (list == NULL) return PyList_New(0);
    int32_t n = kafka_List_size(list);
    PyObject* out = PyList_New(n);
    if (out == NULL) { kafka_List_destroy(list); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const char* s = (const char*)kafka_List_get(list, i);
        PyObject* t = PyUnicode_FromString(s ? s : "");
        if (t == NULL) { Py_DECREF(out); kafka_List_destroy(list); return NULL; }
        PyList_SET_ITEM(out, i, t);
    }
    kafka_List_destroy(list);
    return out;
}

// BORROWED OffsetAndMetadata -> (offset, metadata, leader_epoch | None), or
// None for a NULL value (Java null: no committed offset).
static PyObject* oam_to_py(const kafka_consumer_OffsetAndMetadata_t* oam) {
    if (oam == NULL) Py_RETURN_NONE;
    int32_t epoch = kafka_consumer_OffsetAndMetadata_leader_epoch(oam);  // -1 = Optional.empty()
    const char* metadata = kafka_consumer_OffsetAndMetadata_metadata(oam);
    PyObject* py_epoch = epoch >= 0 ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None);
    if (py_epoch == NULL) return NULL;
    return Py_BuildValue("(LsN)", (long long)kafka_consumer_OffsetAndMetadata_offset(oam),
                         metadata ? metadata : "", py_epoch);
}

// BORROWED map TopicPartition_t* -> OffsetAndMetadata_t* ->
// {(topic, partition): (offset, metadata, leader_epoch | None) | None}.
static PyObject* offset_map_to_py_borrowed(const kafka_Map_t* map) {
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) return d;
    int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = tp_to_py((const kafka_common_TopicPartition_t*)kafka_Map_key(map, i));
        PyObject* val = oam_to_py((const kafka_consumer_OffsetAndMetadata_t*)kafka_Map_value(map, i));
        if (key == NULL || val == NULL || PyDict_SetItem(d, key, val) != 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    return d;
}

// OWNED variant of the above; frees the map (and its owned entries).
static PyObject* oam_map_to_py(kafka_Map_t* map) {
    PyObject* d = offset_map_to_py_borrowed(map);
    if (map) kafka_Map_destroy(map);
    return d;
}

// OWNED map TopicPartition_t* -> int64_t* -> {(topic, partition): int}; frees it.
static PyObject* long_map_to_py(kafka_Map_t* map) {
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) { if (map) kafka_Map_destroy(map); return d; }
    int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = tp_to_py((const kafka_common_TopicPartition_t*)kafka_Map_key(map, i));
        const int64_t* v = (const int64_t*)kafka_Map_value(map, i);
        PyObject* val = v ? PyLong_FromLongLong((long long)*v) : (Py_INCREF(Py_None), Py_None);
        if (key == NULL || val == NULL || PyDict_SetItem(d, key, val) != 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d); kafka_Map_destroy(map); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_Map_destroy(map);
    return d;
}

// OWNED map TopicPartition_t* -> OffsetAndTimestamp_t* ->
// {(topic, partition): (offset, timestamp, leader_epoch | None) | None}; frees it.
static PyObject* oat_map_to_py(kafka_Map_t* map) {
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) { if (map) kafka_Map_destroy(map); return d; }
    int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = tp_to_py((const kafka_common_TopicPartition_t*)kafka_Map_key(map, i));
        const kafka_consumer_OffsetAndTimestamp_t* v =
            (const kafka_consumer_OffsetAndTimestamp_t*)kafka_Map_value(map, i);
        PyObject* val;
        if (v == NULL) {
            val = (Py_INCREF(Py_None), Py_None);
        } else {
            int32_t epoch = kafka_consumer_OffsetAndTimestamp_leader_epoch(v);
            PyObject* py_epoch = epoch >= 0 ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None);
            val = py_epoch ? Py_BuildValue("(LLN)", (long long)kafka_consumer_OffsetAndTimestamp_offset(v),
                                           (long long)kafka_consumer_OffsetAndTimestamp_timestamp(v), py_epoch)
                           : NULL;
        }
        if (key == NULL || val == NULL || PyDict_SetItem(d, key, val) != 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d); kafka_Map_destroy(map); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_Map_destroy(map);
    return d;
}

// OWNED map char* -> kafka_List_t* of PartitionInfo_t* (all owned by the map)
// -> {topic: [partition_info_tuple]}; frees it.
static PyObject* topics_map_to_py(kafka_Map_t* map) {
    PyObject* d = PyDict_New();
    if (d == NULL || map == NULL) { if (map) kafka_Map_destroy(map); return d; }
    int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
        const char* topic = (const char*)kafka_Map_key(map, i);
        PyObject* val = partition_info_list_to_py_borrowed((const kafka_List_t*)kafka_Map_value(map, i));
        if (val == NULL || PyDict_SetItemString(d, topic ? topic : "", val) != 0) {
            Py_XDECREF(val); Py_DECREF(d); kafka_Map_destroy(map); return NULL;
        }
        Py_DECREF(val);
    }
    kafka_Map_destroy(map);
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
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->tps && l->tps[i]) kafka_common_TopicPartition_destroy(l->tps[i]);
    }
    PyMem_Free(l->tps);
    if (l->list) kafka_List_destroy(l->list);
    memset(l, 0, sizeof(*l));
}

// Returns 0 on success (exception set otherwise).
static int tp_list_build(PyObject* seq, tp_list_t* l) {
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "partitions must be a sequence of (topic, partition)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    l->list = kafka_List_new();
    l->tps = n > 0 ? PyMem_Calloc(n, sizeof(*l->tps)) : NULL;
    if (n > 0 && l->tps == NULL) { Py_DECREF(fast); tp_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);  // borrowed
        const char* t = NULL; int p = 0;
        if (!PyArg_ParseTuple(item, "si", &t, &p)) { Py_DECREF(fast); tp_list_free(l); return -1; }
        l->tps[i] = kafka_common_TopicPartition_new(t, (int32_t)p);
        kafka_List_add(l->list, l->tps[i]);
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
    if (l->list) kafka_List_destroy(l->list);
    Py_XDECREF(l->seq);
    memset(l, 0, sizeof(*l));
}

static int str_list_build(PyObject* seq, str_list_t* l) {
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "topics must be a sequence of str");
    if (fast == NULL) return -1;
    l->seq = fast;
    l->list = kafka_List_new();
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);  // borrowed
        const char* s = PyUnicode_Check(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (s == NULL) {
            if (!PyErr_Occurred()) PyErr_SetString(PyExc_TypeError, "topics must be str");
            str_list_free(l);
            return -1;
        }
        kafka_List_add(l->list, (void*)s);
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
    for (Py_ssize_t i = 0; i < m->n; i++) {
        if (m->tps && m->tps[i]) kafka_common_TopicPartition_destroy(m->tps[i]);
    }
    PyMem_Free(m->tps);
    PyMem_Free(m->vals);
    if (m->map) kafka_Map_destroy(m->map);
    memset(m, 0, sizeof(*m));
}

static int tp_i64_map_build(PyObject* seq, tp_i64_map_t* m) {
    memset(m, 0, sizeof(*m));
    PyObject* fast = PySequence_Fast(seq, "expected a sequence of (topic, partition, value)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    m->map = kafka_Map_new();
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
        m->tps[i] = kafka_common_TopicPartition_new(t, (int32_t)p);
        m->vals[i] = (int64_t)v;
        kafka_Map_put(m->map, m->tps[i], &m->vals[i]);
    }
    Py_DECREF(fast);
    return 0;
}

// ---- ConsumerRecords / ConsumerRecord extension types ----------------------

// Owns the Rust batch handle; every record object borrows from it.
typedef struct {
    PyObject_HEAD
    kafka_consumer_ConsumerRecords_t* records;  // owned; freed in dealloc
} ConsumerRecordsObject;

static void ConsumerRecords_dealloc(ConsumerRecordsObject* self) {
    if (self->records != NULL) {
        kafka_consumer_ConsumerRecords_destroy(self->records);
        self->records = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyTypeObject ConsumerRecordsType;  // forward decl (defined after record)
static PyTypeObject ConsumerRecordType;

// Borrows a record from the batch (strong ref to it). The key and value are
// `kafka_Bytes_t *` -- owned by the batch for a KafkaConsumer with NULL
// deserializers, shared with the mock's add_record caller for a MockConsumer --
// and are copied into `bytes` when the record object is created, while both
// owners are certainly alive.
typedef struct {
    PyObject_HEAD
    PyObject* batch;                            // strong ref to ConsumerRecordsObject
    const kafka_consumer_ConsumerRecord_t* rec; // borrowed from the batch
    PyObject* key;                              // bytes or None
    PyObject* value;                            // bytes or None
} ConsumerRecordObject;

static void ConsumerRecord_dealloc(ConsumerRecordObject* self) {
    Py_XDECREF(self->key);
    Py_XDECREF(self->value);
    Py_XDECREF(self->batch);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* ConsumerRecord_get_topic(ConsumerRecordObject* self, void* closure) {
    const char* topic = kafka_consumer_ConsumerRecord_topic(self->rec);
    if (topic == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(topic);
}

static PyObject* ConsumerRecord_get_partition(ConsumerRecordObject* self, void* closure) {
    return PyLong_FromLong(kafka_consumer_ConsumerRecord_partition(self->rec));
}

static PyObject* ConsumerRecord_get_offset(ConsumerRecordObject* self, void* closure) {
    return PyLong_FromLongLong(kafka_consumer_ConsumerRecord_offset(self->rec));
}

static PyObject* ConsumerRecord_get_timestamp(ConsumerRecordObject* self, void* closure) {
    return PyLong_FromLongLong(kafka_consumer_ConsumerRecord_timestamp(self->rec));
}

// Java's TimestampType.id: -1 NO_TIMESTAMP_TYPE, 0 CREATE_TIME, 1 LOG_APPEND_TIME.
static PyObject* ConsumerRecord_get_timestamp_type(ConsumerRecordObject* self, void* closure) {
    const kafka_common_record_TimestampType_t* t = kafka_consumer_ConsumerRecord_timestamp_type(self->rec);
    if (t == NULL) Py_RETURN_NONE;
    return PyLong_FromLong(kafka_common_record_TimestampType_id(t));
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
    return PyLong_FromLong(kafka_consumer_ConsumerRecord_serialized_key_size(self->rec));
}

static PyObject* ConsumerRecord_get_serialized_value_size(ConsumerRecordObject* self, void* closure) {
    return PyLong_FromLong(kafka_consumer_ConsumerRecord_serialized_value_size(self->rec));
}

static PyObject* ConsumerRecord_get_leader_epoch(ConsumerRecordObject* self, void* closure) {
    int32_t epoch = kafka_consumer_ConsumerRecord_leader_epoch(self->rec);  // -1 = Optional.empty()
    if (epoch < 0) Py_RETURN_NONE;
    return PyLong_FromLong(epoch);
}

static PyObject* ConsumerRecord_get_delivery_count(ConsumerRecordObject* self, void* closure) {
    int16_t count = kafka_consumer_ConsumerRecord_delivery_count(self->rec);  // -1 = Optional.empty()
    if (count < 0) Py_RETURN_NONE;
    return PyLong_FromLong(count);
}

// headers -> list[(key: str, value: bytes | None)], in insertion order.
static PyObject* ConsumerRecord_get_headers(ConsumerRecordObject* self, void* closure) {
    const kafka_common_header_internals_RecordHeaders_t* rh = kafka_consumer_ConsumerRecord_headers(self->rec);
    if (rh == NULL) return PyList_New(0);
    // The interface view is borrowed from the class handle (never destroyed);
    // the `__as_` conversion takes the handle non-const.
    const kafka_common_header_Headers_t* headers =
        kafka_common_header_internals_RecordHeaders__as_Headers((kafka_common_header_internals_RecordHeaders_t*)rh);
    kafka_List_t* arr = kafka_common_header_Headers_to_array(headers);  // owned list of borrowed headers
    if (arr == NULL) return PyList_New(0);
    int32_t n = kafka_List_size(arr);
    PyObject* list = PyList_New(n);
    if (list == NULL) { kafka_List_destroy(arr); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_common_header_Header_t* h = kafka_common_header_internals_RecordHeader__as_Header(
            (const kafka_common_header_internals_RecordHeader_t*)kafka_List_get(arr, i));
        const char* hkey = kafka_common_header_Header_key(h);
        kafka_Bytes_t hval = kafka_common_header_Header_value(h);
        PyObject* pykey = PyUnicode_FromString(hkey ? hkey : "");
        PyObject* pyval = bytes_to_py(&hval);
        if (pykey == NULL || pyval == NULL) {
            Py_XDECREF(pykey); Py_XDECREF(pyval); Py_DECREF(list); kafka_List_destroy(arr);
            return NULL;
        }
        PyObject* tuple = Py_BuildValue("(NN)", pykey, pyval);  // steals both
        if (tuple == NULL) { Py_DECREF(list); kafka_List_destroy(arr); return NULL; }
        PyList_SET_ITEM(list, i, tuple);  // steals ref
    }
    kafka_List_destroy(arr);
    return list;
}

static PyObject* ConsumerRecord_repr(ConsumerRecordObject* self) {
    char* s = kafka_consumer_ConsumerRecord_to_string(self->rec);
    if (s == NULL) return PyUnicode_FromString("ConsumerRecord(...)");
    PyObject* out = PyUnicode_FromString(s);
    kafka_string_destroy(s);
    return out;
}

static PyGetSetDef ConsumerRecord_getsetters[] = {
    {"topic", (getter)ConsumerRecord_get_topic, NULL, "Topic name", NULL},
    {"partition", (getter)ConsumerRecord_get_partition, NULL, "Partition", NULL},
    {"offset", (getter)ConsumerRecord_get_offset, NULL, "Offset", NULL},
    {"timestamp", (getter)ConsumerRecord_get_timestamp, NULL, "Timestamp", NULL},
    {"timestamp_type", (getter)ConsumerRecord_get_timestamp_type, NULL, "Timestamp type id", NULL},
    {"key", (getter)ConsumerRecord_get_key, NULL, "Key (bytes or None)", NULL},
    {"value", (getter)ConsumerRecord_get_value, NULL, "Value (bytes or None)", NULL},
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

// Wraps a BORROWED record of `batch` into a ConsumerRecord object, copying its
// key and value bytes now.
static PyObject* wrap_record(ConsumerRecordsObject* batch, const kafka_consumer_ConsumerRecord_t* rec) {
    PyObject* key = bytes_to_py((const kafka_Bytes_t*)kafka_consumer_ConsumerRecord_key(rec));
    PyObject* value = bytes_to_py((const kafka_Bytes_t*)kafka_consumer_ConsumerRecord_value(rec));
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
    if (recs == NULL) return 0;
    int32_t n = kafka_List_size(recs);
    for (int32_t i = 0; i < n; i++) {
        PyObject* r = wrap_record(batch, (const kafka_consumer_ConsumerRecord_t*)kafka_List_get(recs, i));
        if (r == NULL || PyList_Append(out, r) != 0) { Py_XDECREF(r); kafka_List_destroy(recs); return -1; }
        Py_DECREF(r);
    }
    kafka_List_destroy(recs);
    return 0;
}

// ConsumerRecords.records() -> list[ConsumerRecord], partition by partition in
// fetch order (Java's iterator order).
static PyObject* ConsumerRecords_records(ConsumerRecordsObject* self, PyObject* args) {
    PyObject* out = PyList_New(0);
    if (out == NULL) return NULL;
    kafka_List_t* parts = kafka_consumer_ConsumerRecords_partitions(self->records);  // owned, owns its TPs
    if (parts != NULL) {
        int32_t n = kafka_List_size(parts);
        for (int32_t i = 0; i < n; i++) {
            const kafka_common_TopicPartition_t* tp = (const kafka_common_TopicPartition_t*)kafka_List_get(parts, i);
            kafka_List_t* recs = kafka_consumer_ConsumerRecords_records_with_partition(self->records, tp);
            if (append_record_list(self, recs, out) < 0) { Py_DECREF(out); kafka_List_destroy(parts); return NULL; }
        }
        kafka_List_destroy(parts);
    }
    return out;
}

// ConsumerRecords.partitions() -> list[(topic, partition)]
static PyObject* ConsumerRecords_partitions(ConsumerRecordsObject* self, PyObject* args) {
    return tp_list_to_py(kafka_consumer_ConsumerRecords_partitions(self->records));
}

// ConsumerRecords.records_with_partition(topic, partition) -> list[ConsumerRecord]
static PyObject* ConsumerRecords_records_with_partition(ConsumerRecordsObject* self, PyObject* args) {
    const char* topic; int partition;
    if (!PyArg_ParseTuple(args, "si", &topic, &partition)) return NULL;
    PyObject* out = PyList_New(0);
    if (out == NULL) return NULL;
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition);
    kafka_List_t* recs = kafka_consumer_ConsumerRecords_records_with_partition(self->records, tp);
    kafka_common_TopicPartition_destroy(tp);
    if (append_record_list(self, recs, out) < 0) { Py_DECREF(out); return NULL; }
    return out;
}

// ConsumerRecords.records_with_topic(topic) -> list[ConsumerRecord]
static PyObject* ConsumerRecords_records_with_topic(ConsumerRecordsObject* self, PyObject* args) {
    const char* topic;
    if (!PyArg_ParseTuple(args, "s", &topic)) return NULL;
    PyObject* out = PyList_New(0);
    if (out == NULL) return NULL;
    kafka_List_t* recs = kafka_consumer_ConsumerRecords_records_with_topic(self->records, topic);
    if (append_record_list(self, recs, out) < 0) { Py_DECREF(out); return NULL; }
    return out;
}

// ConsumerRecords.next_offsets() -> {(topic, partition): (offset, metadata, leader_epoch | None)}
static PyObject* ConsumerRecords_next_offsets(ConsumerRecordsObject* self, PyObject* args) {
    return oam_map_to_py(kafka_consumer_ConsumerRecords_next_offsets(self->records));
}

static PyObject* ConsumerRecords_count(ConsumerRecordsObject* self, PyObject* args) {
    return PyLong_FromLong(kafka_consumer_ConsumerRecords_count(self->records));
}

static PyObject* ConsumerRecords_is_empty(ConsumerRecordsObject* self, PyObject* args) {
    return PyBool_FromLong(kafka_consumer_ConsumerRecords_is_empty(self->records) ? 1 : 0);
}

static PyMethodDef ConsumerRecords_methods[] = {
    {"count", (PyCFunction)ConsumerRecords_count, METH_NOARGS, "Number of records"},
    {"is_empty", (PyCFunction)ConsumerRecords_is_empty, METH_NOARGS, "Whether the batch is empty"},
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

// Wrap an OWNED records handle into a ConsumerRecordsObject (NULL -> None).
static PyObject* wrap_records(kafka_consumer_ConsumerRecords_t* records) {
    if (records == NULL) Py_RETURN_NONE;
    ConsumerRecordsObject* obj = PyObject_New(ConsumerRecordsObject, &ConsumerRecordsType);
    if (obj == NULL) {
        kafka_consumer_ConsumerRecords_destroy(records);
        return NULL;
    }
    obj->records = records;
    return (PyObject*)obj;
}

// ---- the consumer handle ---------------------------------------------------

// The `void *self` registered with kafka_consumer_ConsumerRebalanceListener_new.
// Owns one reference to the Python adapter (consumer.py's _ListenerAdapter).
// Lives until the registration that copied it is replaced by a later
// subscribe*, or until the consumer is destroyed (an unsubscribe leaves the
// Java listener registered, so the context survives it too).
typedef struct {
    PyObject* adapter;
    kafka_consumer_Consumer_t* consumer;  // reports through set_callback_result
} ListenerCtx;

// The `void *self` registered with kafka_consumer_OffsetCommitCallback_new.
// Lives until `onComplete` fired. When the commit that took it returned Err
// the callback may never fire, so the context is parked on the consumer's
// orphan list and reclaimed at destroy (after `_destroy` ran every pending
// callback): a late `onComplete` finds it alive, a never-firing one is freed
// with the consumer.
typedef struct CommitCbCtx {
    PyObject* adapter;                    // callable(offsets_dict, err_tuple | None), or NULL to discard
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
// Consumer_execute_callbacks (`_cb` entry point), both with the GIL released by
// the wrapper. The adapter method receives the partitions as
// list[(topic, partition)] plus the callback id and returns:
//   * None  -> the listener returned: report success now;
//   * True  -> the adapter deferred (coroutine scheduled on the loop) and will
//              report through Consumer_set_callback_result itself;
//   * raise -> report the exception's message as a KafkaException, like a Java
//              listener throwing out of onPartitions*.
static void listener_fire(const char* method, void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    ListenerCtx* ctx = (ListenerCtx*)self_;
    PyGILState_STATE g = PyGILState_Ensure();
    kafka_common_Error_t* err = NULL;
    int deferred = 0;
    PyObject* py_parts = tp_list_to_py_borrowed(partitions);
    if (py_parts == NULL) {
        err = error_from_py_exception("failed to convert the rebalance partitions");
    } else {
        PyObject* r = PyObject_CallMethod(ctx->adapter, method, "OL", py_parts, (long long)callback_id);
        Py_DECREF(py_parts);
        if (r == NULL) {
            err = error_from_py_exception("rebalance listener raised an exception");
        } else {
            deferred = (r == Py_True);
            Py_DECREF(r);
        }
    }
    if (!deferred) kafka_consumer_Consumer_set_callback_result(ctx->consumer, callback_id, err);
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
    *out_ctx = NULL;
    *out_listener = NULL;
    if (adapter == Py_None) return 0;
    ListenerCtx* ctx = listener_ctx_new(adapter, c->consumer);
    if (ctx == NULL) return -1;
    *out_ctx = ctx;
    *out_listener = kafka_consumer_ConsumerRebalanceListener_new(
        ctx, listener_on_revoked, listener_on_assigned, listener_on_lost);
    return 0;
}

// ---- commit-callback trampoline --------------------------------------------
//
// Java's OffsetCommitCallback.onComplete returns void, so the report carries
// no result: it is made right after the Python callable returned (an exception
// is only printed), and the context is released -- unless orphaned, see above.
static void commit_on_complete(void* self_, const kafka_Map_t* offsets,
                               const kafka_common_Error_t* error, int64_t callback_id) {
    CommitCbCtx* ctx = (CommitCbCtx*)self_;
    PyGILState_STATE g = PyGILState_Ensure();
    if (ctx->adapter != NULL) {
        PyObject* py_offsets = offset_map_to_py_borrowed(offsets);
        PyObject* py_err = error ? error_to_py(error) : (Py_INCREF(Py_None), Py_None);
        if (py_offsets != NULL && py_err != NULL) {
            PyObject* r = PyObject_CallFunctionObjArgs(ctx->adapter, py_offsets, py_err, NULL);
            if (r == NULL) PyErr_WriteUnraisable(ctx->adapter); else Py_DECREF(r);
        } else {
            PyErr_WriteUnraisable(ctx->adapter);
        }
        Py_XDECREF(py_offsets);
        Py_XDECREF(py_err);
    }
    kafka_consumer_Consumer_set_callback_result(ctx->consumer, callback_id, NULL);
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
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallNoArgs(c->notify_cb);
    if (r == NULL) PyErr_WriteUnraisable(c->notify_cb); else Py_DECREF(r);
    PyGILState_Release(g);
}

// Consumer_set_callbacks_notify(handle, callable | None): registers the Python
// callable the notify hook invokes. Call before the first `_cb` operation.
static PyObject* py_Consumer_set_callbacks_notify(PyObject* self, PyObject* args) {
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

// Consumer_execute_callbacks(handle) -> int: runs the queued callbacks on this
// thread (GIL released around the drain; each callback re-acquires it).
static PyObject* py_Consumer_execute_callbacks(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    int32_t n;
    Py_BEGIN_ALLOW_THREADS
    n = kafka_consumer_Consumer_execute_callbacks(s);
    Py_END_ALLOW_THREADS
    return PyLong_FromLong((long)n);
}

// Consumer_set_callback_result(handle, callback_id, message | None): the
// deferred report of a listener invocation (None = success, str = the
// exception message, reported as a KafkaException).
static PyObject* py_Consumer_set_callback_result(PyObject* self, PyObject* args) {
    unsigned long long h; long long callback_id; PyObject* message;
    if (!PyArg_ParseTuple(args, "KLO", &h, &callback_id, &message)) return NULL;
    kafka_common_Error_t* err = NULL;
    if (message != Py_None) {
        const char* msg = PyUnicode_AsUTF8(message);
        if (msg == NULL) return NULL;
        err = kafka_common_Error_kafka_message(msg);
    }
    kafka_consumer_Consumer_set_callback_result(consumer_self(h), (int64_t)callback_id, err);
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
    const char* auto_offset_reset;
    if (!PyArg_ParseTuple(args, "s", &auto_offset_reset)) return NULL;
    Consumer* c = consumer_alloc();
    if (c == NULL) return NULL;
    kafka_common_Error_t* err = kafka_consumer_MockConsumer_new(auto_offset_reset, &c->mc);
    if (err != NULL) {
        const char* msg = kafka_common_Error_message(err);
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create MockConsumer");
        kafka_common_Error_destroy(err);
        PyMem_Free(c);
        return NULL;
    }
    c->is_mock = 1;
    c->consumer = kafka_consumer_MockConsumer__as_Consumer(c->mc);  // borrowed view
    kafka_consumer_Consumer_set_callbacks_notify(c->consumer, consumer_callbacks_notify, c);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)c);
}

// Consumer_KafkaConsumer_new(config: dict[str, str]) -> handle. NULL
// deserializers: records carry `kafka_Bytes_t *` keys and values. Raises
// RuntimeError with the Rust error's message when the config is rejected.
static PyObject* py_Consumer_KafkaConsumer_new(PyObject* self, PyObject* args) {
    PyObject* config_dict;
    if (!PyArg_ParseTuple(args, "O", &config_dict)) return NULL;
    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }
    // A C-built kafka_Map_t holds BORROWED char* -> char*: the strings stay
    // owned by the dict's str objects, which outlive this call.
    kafka_Map_t* props = kafka_Map_new();
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_Check(key) ? PyUnicode_AsUTF8(key) : NULL;
        const char* v = PyUnicode_Check(value) ? PyUnicode_AsUTF8(value) : NULL;
        if (k == NULL || v == NULL) {
            kafka_Map_destroy(props);
            if (!PyErr_Occurred())
                PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
        kafka_Map_put(props, (void*)k, (void*)v);
    }
    kafka_consumer_ConsumerConfig_t* config = NULL;
    kafka_common_Error_t* err = kafka_consumer_ConsumerConfig_new(props, &config);
    kafka_Map_destroy(props);
    kafka_consumer_Consumer_t* consumer = NULL;
    if (err == NULL) {
        err = kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &consumer);  // config copied
        kafka_consumer_ConsumerConfig_destroy(config);
    }
    if (err != NULL) {
        const char* msg = kafka_common_Error_message(err);
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create KafkaConsumer");
        kafka_common_Error_destroy(err);
        return NULL;
    }
    Consumer* c = consumer_alloc();
    if (c == NULL) { kafka_consumer_Consumer_destroy(consumer); return NULL; }
    c->consumer = consumer;  // owned
    kafka_consumer_Consumer_set_callbacks_notify(c->consumer, consumer_callbacks_notify, c);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)c);
}

// Consumer_destroy(handle): destroys the Rust consumer (which runs every still
// pending callback, hence the released GIL), then reclaims the C contexts no
// callback can reference any more.
static PyObject* py_Consumer_destroy(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    Consumer* c = consumer_from_handle(h);
    Py_BEGIN_ALLOW_THREADS
    if (c->is_mock) kafka_consumer_MockConsumer_destroy(c->mc);
    else kafka_consumer_Consumer_destroy(c->consumer);
    Py_END_ALLOW_THREADS
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
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_Consumer_wakeup(consumer_self(h));
    Py_RETURN_NONE;
}

// ---- sync state reads (return Python objects directly) ---------------------

static PyObject* py_Consumer_assignment(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return tp_list_to_py(kafka_consumer_Consumer_assignment(consumer_self(h)));
}

static PyObject* py_Consumer_paused(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return tp_list_to_py(kafka_consumer_Consumer_paused(consumer_self(h)));
}

static PyObject* py_Consumer_subscription(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return string_list_to_py(kafka_consumer_Consumer_subscription(consumer_self(h)));
}

// Consumer_metrics -> list[dict] (name/group/description/tags/value/kind), the
// same shape as Producer_metrics; empty while an operation is in flight.
static PyObject* py_Consumer_metrics(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return metrics_map_to_py(kafka_consumer_Consumer_metrics(consumer_self(h)));
}

// Returns a ConsumerGroupMetadata object that RETAINS the owned Rust handle
// (freed in its tp_dealloc), or None while an operation is in flight.
static PyObject* py_Consumer_group_metadata(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerGroupMetadata_t* m = kafka_consumer_Consumer_group_metadata(consumer_self(h));
    if (m == NULL) Py_RETURN_NONE;
    ConsumerGroupMetadataObject* obj =
        PyObject_New(ConsumerGroupMetadataObject, &ConsumerGroupMetadataType);
    if (obj == NULL) {
        kafka_consumer_ConsumerGroupMetadata_destroy(m);
        return NULL;
    }
    obj->handle = m;
    return (PyObject*)obj;
}

static PyObject* py_Consumer_client_id(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    const char* s = kafka_consumer_Consumer_client_id(consumer_self(h));  // borrowed
    if (s == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(s);
}

// Consumer_current_lag(handle, topic, partition) -> int | None (-1 = empty)
static PyObject* py_Consumer_current_lag(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition;
    if (!PyArg_ParseTuple(args, "Ksi", &h, &topic, &partition)) return NULL;
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition);
    int64_t lag = kafka_consumer_Consumer_current_lag(consumer_self(h), tp);
    kafka_common_TopicPartition_destroy(tp);
    if (lag < 0) Py_RETURN_NONE;
    return PyLong_FromLongLong((long long)lag);
}

// ---- `_cb` completion trampolines ------------------------------------------
//
// Each runs from Consumer_execute_callbacks (GIL released by that wrapper), or
// inline from a ConsumerHandle `_cb` whose consumer is already destroyed, and
// hands copied Python values to the registered callable. The void shape reuses
// the producer's VoidCbCtx / producer_void_cb.

// Delivers cb(value, err_tuple | None); `value` is a new reference (or NULL
// with an exception set), `err` an owned error or NULL.
static void deliver_value_cb(PyObject* cb, PyObject* value, kafka_common_Error_t* err) {
    PyObject* py_err = err ? owned_error_to_py(err) : (Py_INCREF(Py_None), Py_None);
    if (value != NULL && py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(cb, value, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(cb); else Py_DECREF(r);
    } else {
        PyErr_WriteUnraisable(cb);
    }
    Py_XDECREF(value);
    Py_XDECREF(py_err);
}

// poll: cb(ConsumerRecords | None, err_tuple | None)
static void consumer_poll_cb(kafka_consumer_ConsumerRecords_t* records, kafka_common_Error_t* error, void* opaque) {
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
    deliver_value_cb(ctx->cb, wrap_records(records), error);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// position: cb(int, err_tuple | None)   (-1 beside an error)
static void consumer_i64_cb(int64_t value, kafka_common_Error_t* error, void* opaque) {
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
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

// map results: cb(dict | None, err_tuple | None); the owned map is converted
// and freed here.
static void consumer_map_cb(kafka_Map_t* value, kafka_common_Error_t* error, void* opaque) {
    ValueCbCtx* ctx = (ValueCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* py_value = value ? ctx->conv_map(value) : (Py_INCREF(Py_None), Py_None);
    deliver_value_cb(ctx->cb, py_value, error);
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

// list results: cb(list | None, err_tuple | None); the owned list is converted
// and freed here.
static void consumer_list_cb(kafka_List_t* value, kafka_common_Error_t* error, void* opaque) {
    ValueCbCtx* ctx = (ValueCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
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
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    err = OP(s);                                                                        \
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
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    err = OP(s, l.list);                                                                \
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
    unsigned long long h; PyObject* tps; long long ms;                                  \
    if (!PyArg_ParseTuple(args, "KOL", &h, &tps, &ms)) return NULL;                     \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) err = OP_T(s, l.list, (int64_t)ms, &out);                              \
    else err = OP(s, l.list, &out);                                                     \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    if (err != NULL) { if (out) kafka_Map_destroy(out); return build_value_error(Py_None, err); } \
    PyObject* value = CONV(out);                                                        \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = build_value_error(value, NULL);                                     \
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
    unsigned long long h; PyObject* spec; long long ms;                                 \
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &ms)) return NULL;                    \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) err = OP_T(s, m.map, (int64_t)ms, &out);                               \
    else err = OP(s, m.map, &out);                                                      \
    Py_END_ALLOW_THREADS                                                                \
    tp_i64_map_free(&m);                                                                \
    if (err != NULL) { if (out) kafka_Map_destroy(out); return build_value_error(Py_None, err); } \
    PyObject* value = oat_map_to_py(out);                                               \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = build_value_error(value, NULL);                                     \
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
    unsigned long long h; const char* topic; int partition; long long offset;           \
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &offset)) return NULL;  \
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition); \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    err = OP(s, tp, (int64_t)offset);                                                   \
    Py_END_ALLOW_THREADS                                                                \
    kafka_common_TopicPartition_destroy(tp);                                            \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##prefix##_seek_cb(PyObject* self, PyObject* args) {                \
    unsigned long long h; const char* topic; int partition; long long offset; PyObject* cb; \
    if (!PyArg_ParseTuple(args, "KsiLO", &h, &topic, &partition, &offset, &cb)) return NULL; \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) return NULL;                                                       \
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition); \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_CB(s, tp, (int64_t)offset, producer_void_cb, ctx);                               \
    Py_END_ALLOW_THREADS                                                                \
    kafka_common_TopicPartition_destroy(tp);                                            \
    Py_RETURN_NONE;                                                                     \
}                                                                                       \
static PyObject* py_##prefix##_seek_with_metadata(PyObject* self, PyObject* args) {     \
    unsigned long long h; const char* topic; int partition; long long offset; int epoch; const char* metadata; \
    if (!PyArg_ParseTuple(args, "KsiLiz", &h, &topic, &partition, &offset, &epoch, &metadata)) return NULL; \
    kafka_consumer_OffsetAndMetadata_t* oam = NULL;                                     \
    kafka_common_Error_t* err = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata( \
        (int64_t)offset, (int32_t)epoch, metadata, &oam);                               \
    if (err != NULL) return owned_error_to_py(err);                                     \
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition); \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    err = OP_M(s, tp, oam);                                                             \
    Py_END_ALLOW_THREADS                                                                \
    kafka_common_TopicPartition_destroy(tp);                                            \
    kafka_consumer_OffsetAndMetadata_destroy(oam);                                      \
    return err_result(err);                                                             \
}                                                                                       \
static PyObject* py_##prefix##_seek_with_metadata_cb(PyObject* self, PyObject* args) {  \
    unsigned long long h; const char* topic; int partition; long long offset; int epoch; const char* metadata; PyObject* cb; \
    if (!PyArg_ParseTuple(args, "KsiLizO", &h, &topic, &partition, &offset, &epoch, &metadata, &cb)) return NULL; \
    kafka_consumer_OffsetAndMetadata_t* oam = NULL;                                     \
    kafka_common_Error_t* err = kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata( \
        (int64_t)offset, (int32_t)epoch, metadata, &oam);                               \
    if (err != NULL) {                                                                  \
        /* deliver the construction error through the callback like any other */       \
        PyObject* py_err = owned_error_to_py(err);                                      \
        if (py_err == NULL) return NULL;                                                \
        PyObject* r = PyObject_CallFunctionObjArgs(cb, py_err, NULL);                   \
        Py_DECREF(py_err);                                                              \
        if (r == NULL) return NULL;                                                     \
        Py_DECREF(r);                                                                   \
        Py_RETURN_NONE;                                                                 \
    }                                                                                   \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) { kafka_consumer_OffsetAndMetadata_destroy(oam); return NULL; }    \
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition); \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    OP_M_CB(s, tp, oam, producer_void_cb, ctx);                                         \
    Py_END_ALLOW_THREADS                                                                \
    kafka_common_TopicPartition_destroy(tp);                                            \
    kafka_consumer_OffsetAndMetadata_destroy(oam);                                      \
    Py_RETURN_NONE;                                                                     \
}

// Shape: position(self, TopicPartition[, timeout_ms]) -> int64.
// py(h, topic, partition, ms) -> (int, err) / py_cb(h, topic, partition, ms, cb)
#define DEF_POSITION_OPS(pyname, SELF_T, SELF_FN, OP, OP_T, OP_CB, OP_T_CB)             \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h; const char* topic; int partition; long long ms;               \
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &ms)) return NULL;      \
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition); \
    SELF_T s = SELF_FN(h);                                                              \
    int64_t pos = -1;                                                                   \
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) err = OP_T(s, tp, (int64_t)ms, &pos);                                  \
    else err = OP(s, tp, &pos);                                                         \
    Py_END_ALLOW_THREADS                                                                \
    kafka_common_TopicPartition_destroy(tp);                                            \
    PyObject* value = PyLong_FromLongLong((long long)(err ? -1 : pos));                 \
    if (value == NULL) { if (err) kafka_common_Error_destroy(err); return NULL; }       \
    PyObject* ret = build_value_error(value, err);                                      \
    Py_DECREF(value);                                                                   \
    return ret;                                                                         \
}                                                                                       \
static PyObject* py_##pyname##_cb(PyObject* self, PyObject* args) {                     \
    unsigned long long h; const char* topic; int partition; long long ms; PyObject* cb; \
    if (!PyArg_ParseTuple(args, "KsiLO", &h, &topic, &partition, &ms, &cb)) return NULL; \
    VoidCbCtx* ctx = void_cb_ctx_new(cb);                                               \
    if (ctx == NULL) return NULL;                                                       \
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition); \
    SELF_T s = SELF_FN(h);                                                              \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (ms >= 0) OP_T_CB(s, tp, (int64_t)ms, consumer_i64_cb, ctx);                     \
    else OP_CB(s, tp, consumer_i64_cb, ctx);                                            \
    Py_END_ALLOW_THREADS                                                                \
    kafka_common_TopicPartition_destroy(tp);                                            \
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
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    if (with_offsets) {                                                                 \
        if (ms >= 0) err = OP_OT(s, m.map, (int64_t)ms);                                \
        else err = OP_O(s, m.map);                                                      \
    } else {                                                                            \
        if (ms >= 0) err = OP_T(s, (int64_t)ms);                                        \
        else err = OP(s);                                                               \
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
        if (ms >= 0) OP_OT_CB(s, m.map, (int64_t)ms, producer_void_cb, ctx);           \
        else OP_O_CB(s, m.map, producer_void_cb, ctx);                                  \
    } else {                                                                            \
        if (ms >= 0) OP_T_CB(s, (int64_t)ms, producer_void_cb, ctx);                   \
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
    unsigned long long h; const char* reason;
    if (!PyArg_ParseTuple(args, "Ks", &h, &reason)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_consumer_Consumer_enforce_rebalance_with_reason(s, reason);
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
    unsigned long long h; long long ms;
    if (!PyArg_ParseTuple(args, "KL", &h, &ms)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_consumer_ConsumerRecords_t* records = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_consumer_Consumer_poll(s, (int64_t)ms, &records);
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (records) kafka_consumer_ConsumerRecords_destroy(records);
        return build_value_error(Py_None, err);
    }
    PyObject* value = wrap_records(records);
    if (value == NULL) return NULL;
    PyObject* ret = build_value_error(value, NULL);
    Py_DECREF(value);
    return ret;
}

static PyObject* py_Consumer_poll_cb(PyObject* self, PyObject* args) {
    unsigned long long h; long long ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &ms, &cb)) return NULL;
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    Py_BEGIN_ALLOW_THREADS
    kafka_consumer_Consumer_poll_cb(s, (int64_t)ms, consumer_poll_cb, ctx);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// Consumer_close(h, timeout_ms) -> err | None. A negative timeout selects
// Java's close() (default CloseOptions); otherwise close(CloseOptions.timeout).
static PyObject* py_Consumer_close(PyObject* self, PyObject* args) {
    unsigned long long h; long long ms;
    if (!PyArg_ParseTuple(args, "KL", &h, &ms)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_consumer_CloseOptions_t* opts = ms >= 0 ? kafka_consumer_CloseOptions_new_with_timeout((int64_t)ms) : NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = opts ? kafka_consumer_Consumer_close_with_options(s, opts) : kafka_consumer_Consumer_close(s);
    Py_END_ALLOW_THREADS
    if (opts) kafka_consumer_CloseOptions_destroy(opts);
    return err_result(err);
}

static PyObject* py_Consumer_close_cb(PyObject* self, PyObject* args) {
    unsigned long long h; long long ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &ms, &cb)) return NULL;
    VoidCbCtx* ctx = void_cb_ctx_new(cb);
    if (ctx == NULL) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_consumer_CloseOptions_t* opts = ms >= 0 ? kafka_consumer_CloseOptions_new_with_timeout((int64_t)ms) : NULL;
    Py_BEGIN_ALLOW_THREADS
    if (opts) kafka_consumer_Consumer_close_with_options_cb(s, opts, producer_void_cb, ctx);
    else kafka_consumer_Consumer_close_cb(s, producer_void_cb, ctx);
    Py_END_ALLOW_THREADS
    if (opts) kafka_consumer_CloseOptions_destroy(opts);  // copied during the call
    Py_RETURN_NONE;
}

// ---- subscribe -------------------------------------------------------------

// Completion of a `_cb` subscribe: settles the listener registration (see
// consumer_listener_commit) before delivering cb(err_tuple | None).
typedef struct {
    PyObject* cb;
    Consumer* c;
    ListenerCtx* lctx;
} SubscribeCbCtx;

static void consumer_subscribe_cb(kafka_common_Error_t* error, void* opaque) {
    SubscribeCbCtx* ctx = (SubscribeCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
    consumer_listener_commit(ctx->c, ctx->lctx, error != NULL);
    PyObject* py_err = error ? owned_error_to_py(error) : (Py_INCREF(Py_None), Py_None);
    if (py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
        Py_DECREF(py_err);
    } else {
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
    err = listener ? kafka_consumer_Consumer_subscribe_with_topics_listener(s, tl.list, listener)
                   : kafka_consumer_Consumer_subscribe_with_topics(s, tl.list);
    Py_END_ALLOW_THREADS
    if (listener) kafka_consumer_ConsumerRebalanceListener_destroy(listener);  // registration copied
    str_list_free(&tl);
    consumer_listener_commit(c, lctx, err != NULL);
    return err_result(err);
}

static PyObject* py_Consumer_subscribe_cb(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* topics; PyObject* adapter; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOOO", &h, &topics, &adapter, &cb)) return NULL;
    Consumer* c = consumer_from_handle(h);
    str_list_t tl;
    if (str_list_build(topics, &tl) < 0) return NULL;
    ListenerCtx* lctx; kafka_consumer_ConsumerRebalanceListener_t* listener;
    if (listener_build(c, adapter, &lctx, &listener) < 0) { str_list_free(&tl); return NULL; }
    SubscribeCbCtx* ctx = subscribe_cb_ctx_new(cb, c, lctx);
    if (ctx == NULL) {
        if (listener) kafka_consumer_ConsumerRebalanceListener_destroy(listener);
        listener_ctx_free(lctx); str_list_free(&tl);
        return NULL;
    }
    kafka_consumer_Consumer_t* s = c->consumer;
    Py_BEGIN_ALLOW_THREADS
    if (listener) kafka_consumer_Consumer_subscribe_with_topics_listener_cb(s, tl.list, listener, consumer_subscribe_cb, ctx);
    else kafka_consumer_Consumer_subscribe_with_topics_cb(s, tl.list, consumer_subscribe_cb, ctx);
    Py_END_ALLOW_THREADS
    if (listener) kafka_consumer_ConsumerRebalanceListener_destroy(listener);  // registration copied
    str_list_free(&tl);
    Py_RETURN_NONE;
}

// Consumer_subscribe_pattern(h, pattern: str, adapter | None) -> err | None
// (Java's subscribe(SubscriptionPattern[, listener]), evaluated broker-side).
static PyObject* py_Consumer_subscribe_pattern(PyObject* self, PyObject* args) {
    unsigned long long h; const char* pattern; PyObject* adapter;
    if (!PyArg_ParseTuple(args, "KsO", &h, &pattern, &adapter)) return NULL;
    Consumer* c = consumer_from_handle(h);
    ListenerCtx* lctx; kafka_consumer_ConsumerRebalanceListener_t* listener;
    if (listener_build(c, adapter, &lctx, &listener) < 0) return NULL;
    kafka_consumer_SubscriptionPattern_t* sp = kafka_consumer_SubscriptionPattern_new(pattern);
    kafka_consumer_Consumer_t* s = c->consumer;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = listener ? kafka_consumer_Consumer_subscribe_with_pattern_listener(s, sp, listener)
                   : kafka_consumer_Consumer_subscribe_with_pattern(s, sp);
    Py_END_ALLOW_THREADS
    kafka_consumer_SubscriptionPattern_destroy(sp);
    if (listener) kafka_consumer_ConsumerRebalanceListener_destroy(listener);
    consumer_listener_commit(c, lctx, err != NULL);
    return err_result(err);
}

static PyObject* py_Consumer_subscribe_pattern_cb(PyObject* self, PyObject* args) {
    unsigned long long h; const char* pattern; PyObject* adapter; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsOO", &h, &pattern, &adapter, &cb)) return NULL;
    Consumer* c = consumer_from_handle(h);
    ListenerCtx* lctx; kafka_consumer_ConsumerRebalanceListener_t* listener;
    if (listener_build(c, adapter, &lctx, &listener) < 0) return NULL;
    SubscribeCbCtx* ctx = subscribe_cb_ctx_new(cb, c, lctx);
    if (ctx == NULL) {
        if (listener) kafka_consumer_ConsumerRebalanceListener_destroy(listener);
        listener_ctx_free(lctx);
        return NULL;
    }
    kafka_consumer_SubscriptionPattern_t* sp = kafka_consumer_SubscriptionPattern_new(pattern);
    kafka_consumer_Consumer_t* s = c->consumer;
    Py_BEGIN_ALLOW_THREADS
    if (listener) kafka_consumer_Consumer_subscribe_with_pattern_listener_cb(s, sp, listener, consumer_subscribe_cb, ctx);
    else kafka_consumer_Consumer_subscribe_with_pattern_cb(s, sp, consumer_subscribe_cb, ctx);
    Py_END_ALLOW_THREADS
    kafka_consumer_SubscriptionPattern_destroy(sp);
    if (listener) kafka_consumer_ConsumerRebalanceListener_destroy(listener);
    Py_RETURN_NONE;
}

// ---- commit_async ----------------------------------------------------------

// Consumer_commit_async(h, spec | None, adapter | None) -> err | None
//
// Java's commitAsync(), commitAsync(callback) and commitAsync(offsets, callback).
// The FFI's offsets form always takes a callback, so `offsets` without a user
// callback registers a discarding one (Java's commitAsync(offsets, null)).
static PyObject* py_Consumer_commit_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; PyObject* adapter;
    if (!PyArg_ParseTuple(args, "KOO", &h, &spec, &adapter)) return NULL;
    Consumer* c = consumer_from_handle(h);
    kafka_consumer_Consumer_t* s = c->consumer;
    kafka_common_Error_t* err;
    if (spec == Py_None && adapter == Py_None) {
        Py_BEGIN_ALLOW_THREADS
        err = kafka_consumer_Consumer_commit_async(s);
        Py_END_ALLOW_THREADS
        return err_result(err);
    }
    offsets_map_t m; memset(&m, 0, sizeof(m));
    int with_offsets = spec != Py_None;
    if (with_offsets && offsets_map_build(spec, &m) < 0) return NULL;
    CommitCbCtx* ctx = commit_ctx_new(c, adapter == Py_None ? NULL : adapter);
    if (ctx == NULL) { if (with_offsets) offsets_map_free(&m); return NULL; }
    kafka_consumer_OffsetCommitCallback_t* cbh = kafka_consumer_OffsetCommitCallback_new(ctx, commit_on_complete);
    Py_BEGIN_ALLOW_THREADS
    err = with_offsets ? kafka_consumer_Consumer_commit_async_with_offsets_callback(s, m.map, cbh)
                       : kafka_consumer_Consumer_commit_async_with_callback(s, cbh);
    Py_END_ALLOW_THREADS
    kafka_consumer_OffsetCommitCallback_destroy(cbh);  // registration copied
    if (with_offsets) offsets_map_free(&m);
    if (err != NULL) consumer_orphan_commit(c, ctx);
    return err_result(err);
}

// Completion of a `_cb` commit_async: an Err'd commit orphans its callback
// context (see CommitCbCtx) before delivering cb(err_tuple | None).
typedef struct {
    PyObject* cb;
    Consumer* c;
    CommitCbCtx* cctx;
} CommitAsyncCbCtx;

static void consumer_commit_async_cb(kafka_common_Error_t* error, void* opaque) {
    CommitAsyncCbCtx* ctx = (CommitAsyncCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
    if (error != NULL) consumer_orphan_commit(ctx->c, ctx->cctx);
    PyObject* py_err = error ? owned_error_to_py(error) : (Py_INCREF(Py_None), Py_None);
    if (py_err != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(ctx->cb, py_err, NULL);
        if (r == NULL) PyErr_WriteUnraisable(ctx->cb); else Py_DECREF(r);
        Py_DECREF(py_err);
    } else {
        PyErr_WriteUnraisable(ctx->cb);
    }
    Py_DECREF(ctx->cb);
    PyMem_Free(ctx);
    PyGILState_Release(g);
}

static PyObject* py_Consumer_commit_async_cb(PyObject* self, PyObject* args) {
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
    kafka_consumer_OffsetCommitCallback_t* cbh = kafka_consumer_OffsetCommitCallback_new(cctx, commit_on_complete);
    Py_BEGIN_ALLOW_THREADS
    if (with_offsets) kafka_consumer_Consumer_commit_async_with_offsets_callback_cb(s, m.map, cbh, consumer_commit_async_cb, ctx);
    else kafka_consumer_Consumer_commit_async_with_callback_cb(s, cbh, consumer_commit_async_cb, ctx);
    Py_END_ALLOW_THREADS
    kafka_consumer_OffsetCommitCallback_destroy(cbh);  // registration copied
    if (with_offsets) offsets_map_free(&m);
    Py_RETURN_NONE;
}

// Consumer_partitions_for(h, topic, timeout_ms) -> (list | None, err | None)
static PyObject* py_Consumer_partitions_for(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; long long ms;
    if (!PyArg_ParseTuple(args, "KsL", &h, &topic, &ms)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_List_t* infos = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (ms >= 0) err = kafka_consumer_Consumer_partitions_for_with_timeout(s, topic, (int64_t)ms, &infos);
    else err = kafka_consumer_Consumer_partitions_for(s, topic, &infos);
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (infos) kafka_List_destroy(infos);
        return build_value_error(Py_None, err);
    }
    PyObject* value = partition_info_list_to_py(infos);
    if (value == NULL) return NULL;
    PyObject* ret = build_value_error(value, NULL);
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
    unsigned long long h; long long ms;
    if (!PyArg_ParseTuple(args, "KL", &h, &ms)) return NULL;
    kafka_consumer_Consumer_t* s = consumer_self(h);
    kafka_Map_t* out = NULL;
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    if (ms >= 0) err = kafka_consumer_Consumer_list_topics_with_timeout(s, (int64_t)ms, &out);
    else err = kafka_consumer_Consumer_list_topics(s, &out);
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        if (out) kafka_Map_destroy(out);
        return build_value_error(Py_None, err);
    }
    PyObject* value = topics_map_to_py(out);
    if (value == NULL) return NULL;
    PyObject* ret = build_value_error(value, NULL);
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
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerHandle_t* hh = kafka_consumer_Consumer_handle(consumer_self(h));
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)hh);
}

static PyObject* py_ConsumerHandle_destroy(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerHandle_destroy((kafka_consumer_ConsumerHandle_t*)(uintptr_t)h);
    Py_RETURN_NONE;
}

static PyObject* py_ConsumerHandle_wakeup(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerHandle_wakeup(handle_self(h));
    Py_RETURN_NONE;
}

static PyObject* py_ConsumerHandle_assignment(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return tp_list_to_py(kafka_consumer_ConsumerHandle_assignment(handle_self(h)));
}

static PyObject* py_ConsumerHandle_paused(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return tp_list_to_py(kafka_consumer_ConsumerHandle_paused(handle_self(h)));
}

static PyObject* py_ConsumerHandle_subscription(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return string_list_to_py(kafka_consumer_ConsumerHandle_subscription(handle_self(h)));
}

// No-timeout variants of the map-returning shapes (the ConsumerHandle's Java
// methods have no Duration overload). py(h, tps) / py_cb(h, tps, cb)
#define DEF_TPLIST_MAP_OPS_NT(pyname, SELF_T, SELF_FN, OP, OP_CB, CONV)                 \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h; PyObject* tps;                                                \
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;                           \
    tp_list_t l;                                                                        \
    if (tp_list_build(tps, &l) < 0) return NULL;                                        \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    err = OP(s, l.list, &out);                                                          \
    Py_END_ALLOW_THREADS                                                                \
    tp_list_free(&l);                                                                   \
    if (err != NULL) { if (out) kafka_Map_destroy(out); return build_value_error(Py_None, err); } \
    PyObject* value = CONV(out);                                                        \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = build_value_error(value, NULL);                                     \
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
    unsigned long long h; PyObject* spec;                                               \
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;                          \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    SELF_T s = SELF_FN(h);                                                              \
    kafka_Map_t* out = NULL;                                                            \
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    err = OP(s, m.map, &out);                                                           \
    Py_END_ALLOW_THREADS                                                                \
    tp_i64_map_free(&m);                                                                \
    if (err != NULL) { if (out) kafka_Map_destroy(out); return build_value_error(Py_None, err); } \
    PyObject* value = oat_map_to_py(out);                                               \
    if (value == NULL) return NULL;                                                     \
    PyObject* ret = build_value_error(value, NULL);                                     \
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
    kafka_common_Error_t* err;                                                          \
    Py_BEGIN_ALLOW_THREADS                                                              \
    err = with_offsets ? OP_O(s, m.map) : OP(s);                                        \
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

#undef HANDLE_T

// ---- MockConsumer drivers --------------------------------------------------

static kafka_consumer_MockConsumer_t* mock_self(unsigned long long h) {
    return consumer_from_handle(h)->mc;
}

// MockConsumer_rebalance(h, [(topic, partition)]) -> err | None. Blocking: it
// invokes the registered listener on this thread. `_cb` queues it instead.
static PyObject* py_MockConsumer_rebalance(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps;
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;
    tp_list_t l;
    if (tp_list_build(tps, &l) < 0) return NULL;
    kafka_consumer_MockConsumer_t* mc = mock_self(h);
    kafka_common_Error_t* err;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_consumer_MockConsumer_rebalance(mc, l.list);
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
    PinnedBytes* pb = (PinnedBytes*)PyMem_Malloc(sizeof(PinnedBytes) + (size_t)view.len);
    if (pb == NULL) { PyBuffer_Release(&view); PyErr_NoMemory(); return -1; }
    if (view.len > 0) memcpy(pb->data, view.buf, (size_t)view.len);
    pb->bytes.data = pb->data;
    pb->bytes.len = (int32_t)view.len;
    PyBuffer_Release(&view);
    pb->next = c->pinned;
    c->pinned = pb;
    *out = &pb->bytes;
    return 0;
}

// MockConsumer_add_record(h, topic, partition, offset, key | None, value | None) -> err | None
static PyObject* py_MockConsumer_add_record(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset; PyObject* key; PyObject* value;
    if (!PyArg_ParseTuple(args, "KsiLOO", &h, &topic, &partition, &offset, &key, &value)) return NULL;
    Consumer* c = consumer_from_handle(h);
    kafka_Bytes_t* k; kafka_Bytes_t* v;
    if (pin_bytes(c, key, &k) < 0) return NULL;
    if (pin_bytes(c, value, &v) < 0) return NULL;
    kafka_consumer_ConsumerRecord_t* rec =
        kafka_consumer_ConsumerRecord_new(topic, (int32_t)partition, (int64_t)offset, k, v);
    kafka_common_Error_t* err = kafka_consumer_MockConsumer_add_record(c->mc, rec);  // copied
    kafka_consumer_ConsumerRecord_destroy(rec);
    return err_result(err);
}

// MockConsumer_update_{beginning,end,duration}_offsets(h, [(topic, partition, offset)])
#define DEF_MOCK_UPDATE_OFFSETS(pyname, OP)                                             \
static PyObject* py_##pyname(PyObject* self, PyObject* args) {                          \
    unsigned long long h; PyObject* spec;                                               \
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;                          \
    tp_i64_map_t m;                                                                     \
    if (tp_i64_map_build(spec, &m) < 0) return NULL;                                    \
    OP(mock_self(h), m.map);                                                            \
    tp_i64_map_free(&m);                                                                \
    Py_RETURN_NONE;                                                                     \
}

DEF_MOCK_UPDATE_OFFSETS(MockConsumer_update_beginning_offsets, kafka_consumer_MockConsumer_update_beginning_offsets)
DEF_MOCK_UPDATE_OFFSETS(MockConsumer_update_end_offsets, kafka_consumer_MockConsumer_update_end_offsets)
DEF_MOCK_UPDATE_OFFSETS(MockConsumer_update_duration_offsets, kafka_consumer_MockConsumer_update_duration_offsets)

// MockConsumer_update_partitions(h, topic, [(partition, leader_id, host, port)]) -> err | None
// Each PartitionInfo gets the leader as its only replica / ISR member.
static PyObject* py_MockConsumer_update_partitions(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KsO", &h, &topic, &spec)) return NULL;
    PyObject* seq = PySequence_Fast(spec, "partitions must be a sequence");
    if (seq == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
    // C-built lists hold borrowed elements: the Node_t* and PartitionInfo_t*
    // are owned here and freed after the (copying) call.
    kafka_List_t* infos = kafka_List_new();
    kafka_List_t* nodes = kafka_List_new();
    kafka_List_t* replicas = kafka_List_new();  // one single-leader list per PartitionInfo
    kafka_common_Error_t* err = NULL;
    int failed = 0;
    for (Py_ssize_t i = 0; i < n; i++) {
        int partition, leader_id, port; const char* host;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(seq, i), "iisi", &partition, &leader_id, &host, &port)) {
            failed = 1;
            break;
        }
        kafka_common_Node_t* leader = kafka_common_Node_new((int32_t)leader_id, host, (int32_t)port);
        kafka_List_add(nodes, leader);
        kafka_List_t* replica = kafka_List_new();
        kafka_List_add(replica, leader);
        kafka_List_add(replicas, replica);
        kafka_List_add(infos, kafka_common_PartitionInfo_new(topic, (int32_t)partition, leader, replica, replica));
    }
    if (!failed) {
        Py_BEGIN_ALLOW_THREADS
        err = kafka_consumer_MockConsumer_update_partitions(mock_self(h), topic, infos);  // copied
        Py_END_ALLOW_THREADS
    }
    for (int32_t i = 0; i < kafka_List_size(infos); i++)
        kafka_common_PartitionInfo_destroy((kafka_common_PartitionInfo_t*)kafka_List_get(infos, i));
    for (int32_t i = 0; i < kafka_List_size(replicas); i++)
        kafka_List_destroy((kafka_List_t*)kafka_List_get(replicas, i));
    for (int32_t i = 0; i < kafka_List_size(nodes); i++)
        kafka_common_Node_destroy((kafka_common_Node_t*)kafka_List_get(nodes, i));
    kafka_List_destroy(infos);
    kafka_List_destroy(replicas);
    kafka_List_destroy(nodes);
    Py_DECREF(seq);
    if (failed) return NULL;
    return err_result(err);
}

// Builds the error for set_poll_error / set_offsets_error: a code selects the
// matching Error variant (mock_error_from_code), None a plain KafkaException
// with the message.
static kafka_common_Error_t* mock_error_build(const char* message, PyObject* code) {
    if (code == Py_None) return kafka_common_Error_kafka_message(message);
    long c = PyLong_AsLong(code);
    if (c == -1 && PyErr_Occurred()) return NULL;
    return mock_error_from_code((int)c, message);
}

// MockConsumer_set_poll_error(h, message, code | None)
static PyObject* py_MockConsumer_set_poll_error(PyObject* self, PyObject* args) {
    unsigned long long h; const char* message; PyObject* code;
    if (!PyArg_ParseTuple(args, "KsO", &h, &message, &code)) return NULL;
    kafka_common_Error_t* err = mock_error_build(message, code);
    if (err == NULL) return NULL;
    kafka_consumer_MockConsumer_set_poll_error(mock_self(h), err);  // ownership moves
    Py_RETURN_NONE;
}

// MockConsumer_set_offsets_error(h, message, code | None)
static PyObject* py_MockConsumer_set_offsets_error(PyObject* self, PyObject* args) {
    unsigned long long h; const char* message; PyObject* code;
    if (!PyArg_ParseTuple(args, "KsO", &h, &message, &code)) return NULL;
    kafka_common_Error_t* err = mock_error_build(message, code);
    if (err == NULL) return NULL;
    kafka_consumer_MockConsumer_set_offsets_error(mock_self(h), err);  // ownership moves
    Py_RETURN_NONE;
}

// MockConsumer_set_max_poll_records(h, n) -> err | None
static PyObject* py_MockConsumer_set_max_poll_records(PyObject* self, PyObject* args) {
    unsigned long long h; long long n;
    if (!PyArg_ParseTuple(args, "KL", &h, &n)) return NULL;
    return err_result(kafka_consumer_MockConsumer_set_max_poll_records(mock_self(h), (int64_t)n));
}

static PyObject* py_MockConsumer_closed(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return PyBool_FromLong(kafka_consumer_MockConsumer_closed(mock_self(h)));
}

static PyObject* py_MockConsumer_should_rebalance(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return PyBool_FromLong(kafka_consumer_MockConsumer_should_rebalance(mock_self(h)));
}

static PyObject* py_MockConsumer_reset_should_rebalance(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_MockConsumer_reset_should_rebalance(mock_self(h));
    Py_RETURN_NONE;
}

static PyObject* py_MockConsumer_last_poll_timeout(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return PyLong_FromLongLong((long long)kafka_consumer_MockConsumer_last_poll_timeout(mock_self(h)));
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
//     `kafka_common_KafkaFuture_get_cb` on every future; the callbacks arrive
//     through `Admin_execute_callbacks`, which the Python side runs on the
//     thread that waits for them. The asyncio client drains on the event-loop
//     thread when the `Admin_set_callbacks_notify` hook fires
//     (`loop.call_soon_threadsafe`); the sync client's hook only sets a
//     `threading.Event` that the calling thread waits on in 100 ms slices
//     (`_sync_wait.SyncWaiter`), so a pending `KeyboardInterrupt` is raised
//     promptly instead of once a native block returned. A future that belongs
//     to no client (refinements, `all_of`) fires inline, which the same code
//     handles. `Admin_close_cb` queues `cb()` on the same vector so `close`
//     waits the same way.
//   * `Admin_resolve(job)` and `Admin_close(handle, timeout_ms)` are the
//     blocking forms: `kafka_common_KafkaFuture_get` on every future / the
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
    PyGILState_STATE g = PyGILState_Ensure();
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
    PyObject* config_dict;
    if (!PyArg_ParseTuple(args, "O", &config_dict)) return NULL;
    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }
    // A C-built kafka_Map_t holds BORROWED char* -> char*: the strings stay
    // owned by the dict's str objects, which outlive this call.
    kafka_Map_t* props = kafka_Map_new();
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_Check(key) ? PyUnicode_AsUTF8(key) : NULL;
        const char* v = PyUnicode_Check(value) ? PyUnicode_AsUTF8(value) : NULL;
        if (k == NULL || v == NULL) {
            kafka_Map_destroy(props);
            if (!PyErr_Occurred())
                PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
        kafka_Map_put(props, (void*)k, (void*)v);
    }
    kafka_admin_AdminClientConfig_t* config = NULL;
    kafka_common_Error_t* err = kafka_admin_AdminClientConfig_new(props, &config);
    kafka_Map_destroy(props);
    kafka_admin_Admin_t* client = NULL;
    if (err == NULL) {
        Py_BEGIN_ALLOW_THREADS
        err = kafka_admin_AdminClient_create(config, &client);
        Py_END_ALLOW_THREADS
        kafka_admin_AdminClientConfig_destroy(config);
    }
    if (err != NULL) {
        const char* msg = kafka_common_Error_message(err);
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create AdminClient");
        kafka_common_Error_destroy(err);
        return NULL;
    }
    AdminHandle* a = admin_alloc();
    if (a == NULL) { kafka_admin_Admin_destroy(client); return NULL; }
    a->owned = client;
    a->admin = client;
    kafka_admin_Admin_set_callbacks_notify(a->admin, admin_callbacks_notify, a);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)a);
}

// Admin_MockAdminClient_new(num_brokers: int) -> handle
static PyObject* py_Admin_MockAdminClient_new(PyObject* self, PyObject* args) {
    int num_brokers;
    if (!PyArg_ParseTuple(args, "i", &num_brokers)) return NULL;
    kafka_admin_MockAdminClient_t* mock = NULL;
    kafka_common_Error_t* err = kafka_admin_MockAdminClient_create((int32_t)num_brokers, &mock);
    if (err != NULL) {
        const char* msg = kafka_common_Error_message(err);
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create MockAdminClient");
        kafka_common_Error_destroy(err);
        return NULL;
    }
    AdminHandle* a = admin_alloc();
    if (a == NULL) { kafka_admin_MockAdminClient_destroy(mock); return NULL; }
    a->mock = mock;
    a->admin = kafka_admin_MockAdminClient__as_Admin(mock);  // borrowed view
    kafka_admin_Admin_set_callbacks_notify(a->admin, admin_callbacks_notify, a);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)a);
}

// Admin_close(handle, timeout_ms): Java close() / close(Duration). Blocks
// (GIL released); -1 is the no-argument form. Does not free the handle. The
// Python clients use `Admin_close_cb` instead; this stays for callers that
// want a native block.
static PyObject* py_Admin_close(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KL", &h, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    Py_BEGIN_ALLOW_THREADS
    if (timeout_ms < 0) kafka_admin_Admin_close(a->admin);
    else kafka_admin_Admin_close_with_timeout(a->admin, (int64_t)timeout_ms);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

// Completion of `Admin_close_cb`: `cb()` runs from `Admin_execute_callbacks`
// (or from `Admin_destroy`, which runs the still-pending callbacks). The C
// API's close callback carries no error (`kafka_admin_Admin_close_cb_t` takes
// only the opaque), so the Python callback takes no arguments. Reuses the
// producer's `VoidCbCtx` for the callable reference.
static void admin_close_cb(void* opaque) {
    VoidCbCtx* ctx = (VoidCbCtx*)opaque;
    PyGILState_STATE g = PyGILState_Ensure();
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
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    // Silence the notify hook first: `_destroy` runs the pending callbacks
    // itself, and a scheduled drain racing the teardown would touch a freed
    // client. The callable stays alive until `_destroy` has returned.
    PyObject* notify_cb = a->notify_cb;
    a->notify_cb = NULL;
    Py_BEGIN_ALLOW_THREADS
    if (a->owned) kafka_admin_Admin_destroy(a->owned);
    else if (a->mock) kafka_admin_MockAdminClient_destroy(a->mock);
    Py_END_ALLOW_THREADS
    Py_XDECREF(notify_cb);
    PyMem_Free(a);
    Py_RETURN_NONE;
}

// Admin_set_callbacks_notify(handle, callable | None): the Python callable the
// notify hook invokes (from a Rust task). Call once, right after creation and
// before the first `_cb` resolve: swapping it while a notification is in
// flight is not synchronized against the hook.
static PyObject* py_Admin_set_callbacks_notify(PyObject* self, PyObject* args) {
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

// Admin_execute_callbacks(handle) -> int: runs the queued callbacks on this
// thread (GIL released around the drain; each callback re-acquires it).
static PyObject* py_Admin_execute_callbacks(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    int32_t n;
    Py_BEGIN_ALLOW_THREADS
    n = kafka_admin_Admin_execute_callbacks(a->admin);
    Py_END_ALLOW_THREADS
    return PyLong_FromLong((long)n);
}

// ---- error helpers -------------------------------------------------------------

// (code, message | None, is_retriable, is_fatal) -- the 4-tuple
// `admin._to_error` turns into a KafkaError; None for a NULL error.
static PyObject* borrowed_error_to_py(const kafka_common_Error_t* e) {
    if (e == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(izii)", kafka_common_Error_code(e),
                         kafka_common_Error_message(e),
                         kafka_common_Error_is_retriable_error(e) ? 1 : 0,
                         error_is_fatal(e) ? 1 : 0);
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
    const char* msg = kafka_common_Error_message(err);
    if (msg == NULL) msg = "";
    size_t len = strlen(prefix) + 2 + strlen(msg) + 1;
    char* buf = (char*)PyMem_Malloc(len);
    kafka_common_Error_t* out;
    if (buf == NULL) {
        out = kafka_common_Error_local_illegal_argument(msg);
    } else {
        snprintf(buf, len, "%s: %s", prefix, msg);
        out = kafka_common_Error_local_illegal_argument(buf);
        PyMem_Free(buf);
    }
    kafka_common_Error_destroy(err);
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
    kafka_List_t** lists;                // owned lists destroyed at job end
    int n_lists, lists_cap;
    void* result;
    void (*result_destroy)(void*);
    admin_assemble_fn assemble;
    void* ctx;                           // result-specific, freed by ctx_free
    void (*ctx_free)(void*);
    PyObject* callback;                  // asyncio: cb(payload, exc); NULL when sync
    int is_async;
    int outstanding;                     // async: futures not yet delivered
    PyObject *exc_type, *exc_value, *exc_tb;  // first conversion failure
} AdminJob;

static AdminJob* job_new(void* result, void (*result_destroy)(void*), admin_assemble_fn assemble) {
    AdminJob* job = (AdminJob*)PyMem_Calloc(1, sizeof(AdminJob));
    if (job == NULL) { PyErr_NoMemory(); return NULL; }
    job->result = result;
    job->result_destroy = result_destroy;
    job->assemble = assemble;
    return job;
}

static void job_free(AdminJob* job) {
    for (int i = 0; i < job->n; i++) {
        AdminSlot* s = job->slots[i];
        if (s->owned && s->future) kafka_common_KafkaFuture_destroy(s->future);
        Py_XDECREF(s->key); Py_XDECREF(s->value); Py_XDECREF(s->error);
        PyMem_Free(s);
    }
    PyMem_Free(job->slots);
    for (int i = 0; i < job->n_maps; i++) kafka_Map_destroy(job->maps[i]);
    PyMem_Free(job->maps);
    for (int i = 0; i < job->n_lists; i++) kafka_List_destroy(job->lists[i]);
    PyMem_Free(job->lists);
    if (job->result && job->result_destroy) job->result_destroy(job->result);
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
            if (owned) kafka_common_KafkaFuture_destroy(future);
            PyErr_NoMemory();
            return NULL;
        }
        job->slots = grown; job->cap = cap;
    }
    AdminSlot* s = (AdminSlot*)PyMem_Calloc(1, sizeof(AdminSlot));
    if (s == NULL) {
        Py_XDECREF(key);
        if (owned) kafka_common_KafkaFuture_destroy(future);
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
    if (map == NULL) return 0;
    if (job->n_maps == job->maps_cap) {
        int cap = job->maps_cap ? job->maps_cap * 2 : 4;
        kafka_Map_t** grown = (kafka_Map_t**)PyMem_Realloc(job->maps, (size_t)cap * sizeof(*grown));
        if (grown == NULL) { kafka_Map_destroy(map); PyErr_NoMemory(); return -1; }
        job->maps = grown; job->maps_cap = cap;
    }
    job->maps[job->n_maps++] = map;
    return 0;
}

static int job_own_list(AdminJob* job, kafka_List_t* list) {
    if (list == NULL) return 0;
    if (job->n_lists == job->lists_cap) {
        int cap = job->lists_cap ? job->lists_cap * 2 : 4;
        kafka_List_t** grown = (kafka_List_t**)PyMem_Realloc(job->lists, (size_t)cap * sizeof(*grown));
        if (grown == NULL) { kafka_List_destroy(list); PyErr_NoMemory(); return -1; }
        job->lists = grown; job->lists_cap = cap;
    }
    job->lists[job->n_lists++] = list;
    return 0;
}

static void job_dispatch_range(AdminJob* job, int from, int to);

// Resolves one slot: `get` (immediate when the future already completed, as
// it has in the `_cb` path) and conversion while the value is still borrowed
// from the live future. GIL held on entry (the sync path releases it around
// the blocking `get`).
static void slot_resolve(AdminJob* job, AdminSlot* slot) {
    void* value = NULL;
    kafka_common_Error_t* err;
    if (job->is_async) {
        err = kafka_common_KafkaFuture_get(slot->future, &value);
    } else {
        Py_BEGIN_ALLOW_THREADS
        err = kafka_common_KafkaFuture_get(slot->future, &value);
        Py_END_ALLOW_THREADS
    }
    slot->done = 1;
    if (err != NULL) {
        slot->error = borrowed_error_to_py(err);
        kafka_common_Error_destroy(err);
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

// `kafka_common_KafkaFuture_get_cb` callback. The value and error delivered
// here are not used: the error is released and the slot re-reads the (now
// completed) future with `get`, whose value stays valid until the future is
// destroyed -- the same lifetime rule the sync path relies on. Runs on the
// thread draining `Admin_execute_callbacks` (GIL released there), or inline
// inside `get_cb` for a client-less future (GIL held); PyGILState_Ensure
// covers both.
static void admin_slot_cb(void* value, kafka_common_Error_t* error, void* opaque) {
    (void)value;
    AdminSlot* slot = (AdminSlot*)opaque;
    if (error) kafka_common_Error_destroy(error);
    PyGILState_STATE g = PyGILState_Ensure();
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
// Admin_execute_callbacks (or inline, for a job with no futures or
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
    if (id == NULL) Py_RETURN_NONE;
    char* s = kafka_common_Uuid_to_string(id);
    PyObject* out = PyUnicode_FromString(s ? s : "");
    if (s) kafka_string_destroy(s);
    return out;
}

// An owned Uuid (TopicDescription/TopicListing topic_id getters) -> str; frees it.
static PyObject* owned_uuid_to_py(kafka_common_Uuid_t* id) {
    PyObject* out = uuid_to_py(id);
    if (id) kafka_common_Uuid_destroy(id);
    return out;
}

// An owned enum `toString()` -> str (None for a NULL singleton).
static PyObject* owned_string_to_py(char* s) {
    if (s == NULL) Py_RETURN_NONE;
    PyObject* out = PyUnicode_FromString(s);
    kafka_string_destroy(s);
    return out;
}

static PyObject* group_state_to_py(const kafka_common_GroupState_t* s) {
    if (s == NULL) Py_RETURN_NONE;
    return owned_string_to_py(kafka_common_GroupState_to_string(s));
}

static PyObject* group_type_to_py(const kafka_common_GroupType_t* t) {
    if (t == NULL) Py_RETURN_NONE;
    return owned_string_to_py(kafka_common_GroupType_to_string(t));
}

static PyObject* classic_group_state_to_py(const kafka_common_ClassicGroupState_t* s) {
    if (s == NULL) Py_RETURN_NONE;
    return owned_string_to_py(kafka_common_ClassicGroupState_to_string(s));
}

static PyObject* transaction_state_to_py(const kafka_admin_TransactionState_t* s) {
    if (s == NULL) Py_RETURN_NONE;
    return owned_string_to_py(kafka_admin_TransactionState_to_string(s));
}

// Java constant names, which the Python `ConfigSource` surface exposes.
static const char* config_source_name(const kafka_admin_ConfigEntry_ConfigSource_t* s) {
    if (s == NULL) return "UNKNOWN";
    switch (kafka_admin_ConfigEntry_ConfigSource__enum(s)) {
        case kafka_admin_ConfigEntry_ConfigSource_e_dynamic_topic_config: return "DYNAMIC_TOPIC_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_dynamic_broker_logger_config: return "DYNAMIC_BROKER_LOGGER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_dynamic_broker_config: return "DYNAMIC_BROKER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_dynamic_default_broker_config: return "DYNAMIC_DEFAULT_BROKER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_dynamic_client_metrics_config: return "DYNAMIC_CLIENT_METRICS_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_dynamic_group_config: return "DYNAMIC_GROUP_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_static_broker_config: return "STATIC_BROKER_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_default_config: return "DEFAULT_CONFIG";
        case kafka_admin_ConfigEntry_ConfigSource_e_unknown: return "UNKNOWN";
    }
    return "UNKNOWN";
}

static const char* config_type_name(const kafka_admin_ConfigEntry_ConfigType_t* t) {
    if (t == NULL) return "UNKNOWN";
    switch (kafka_admin_ConfigEntry_ConfigType__enum(t)) {
        case kafka_admin_ConfigEntry_ConfigType_e_boolean: return "BOOLEAN";
        case kafka_admin_ConfigEntry_ConfigType_e_string: return "STRING";
        case kafka_admin_ConfigEntry_ConfigType_e_int_: return "INT";
        case kafka_admin_ConfigEntry_ConfigType_e_short_: return "SHORT";
        case kafka_admin_ConfigEntry_ConfigType_e_long_: return "LONG";
        case kafka_admin_ConfigEntry_ConfigType_e_double_: return "DOUBLE";
        case kafka_admin_ConfigEntry_ConfigType_e_list: return "LIST";
        case kafka_admin_ConfigEntry_ConfigType_e_class_: return "CLASS";
        case kafka_admin_ConfigEntry_ConfigType_e_password: return "PASSWORD";
        case kafka_admin_ConfigEntry_ConfigType_e_unknown: return "UNKNOWN";
    }
    return "UNKNOWN";
}

// An OWNED list of borrowed AclOperation singletons -> [code, ...], or None
// when the broker did not report the set (NULL list, Java's null). Frees it.
static PyObject* acl_operations_to_py(kafka_List_t* ops) {
    if (ops == NULL) Py_RETURN_NONE;
    int32_t n = kafka_List_size(ops);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_List_destroy(ops); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_common_acl_AclOperation_t* op =
            (const kafka_common_acl_AclOperation_t*)kafka_List_get(ops, i);
        PyObject* code = PyLong_FromLong(kafka_common_acl_AclOperation_code(op));
        if (code == NULL) { Py_DECREF(out); kafka_List_destroy(ops); return NULL; }
        PyList_SET_ITEM(out, i, code);
    }
    kafka_List_destroy(ops);
    return out;
}

// An OWNED list of owned int32_t* -> [int, ...]; frees it. NULL -> [].
static PyObject* int32_list_to_py(kafka_List_t* list) {
    if (list == NULL) return PyList_New(0);
    int32_t n = kafka_List_size(list);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_List_destroy(list); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* v = PyLong_FromLong(*(const int32_t*)kafka_List_get(list, i));
        if (v == NULL) { Py_DECREF(out); kafka_List_destroy(list); return NULL; }
        PyList_SET_ITEM(out, i, v);
    }
    kafka_List_destroy(list);
    return out;
}

static PyObject* tp_replica_to_py(const kafka_common_TopicPartitionReplica_t* r) {
    const char* topic = kafka_common_TopicPartitionReplica_topic(r);
    return Py_BuildValue("(sii)", topic ? topic : "",
                         (int)kafka_common_TopicPartitionReplica_partition(r),
                         (int)kafka_common_TopicPartitionReplica_broker_id(r));
}

// (type_id, name) -- the key `_to_describe_configs` rebuilds a ConfigResource from.
static PyObject* config_resource_to_py(const kafka_common_config_ConfigResource_t* r) {
    const char* name = kafka_common_config_ConfigResource_name(r);
    return Py_BuildValue("(is)",
                         (int)kafka_common_config_ConfigResource_Type_id(
                             kafka_common_config_ConfigResource_type(r)),
                         name ? name : "");
}

// (resource_type, name, pattern_type, principal, host, operation, permission)
// -- the four enums as Java's `code()`.
static PyObject* acl_binding_to_py(const kafka_common_acl_AclBinding_t* b) {
    if (b == NULL) Py_RETURN_NONE;
    const kafka_common_resource_ResourcePattern_t* pattern = kafka_common_acl_AclBinding_pattern(b);
    const kafka_common_acl_AccessControlEntry_t* entry = kafka_common_acl_AclBinding_entry(b);
    return Py_BuildValue(
        "(isissii)",
        (int)kafka_common_resource_ResourceType_code(
            kafka_common_resource_ResourcePattern_resource_type(pattern)),
        kafka_common_resource_ResourcePattern_name(pattern),
        (int)kafka_common_resource_PatternType_code(
            kafka_common_resource_ResourcePattern_pattern_type(pattern)),
        kafka_common_acl_AccessControlEntry_principal(entry),
        kafka_common_acl_AccessControlEntry_host(entry),
        (int)kafka_common_acl_AclOperation_code(kafka_common_acl_AccessControlEntry_operation(entry)),
        (int)kafka_common_acl_AclPermissionType_code(
            kafka_common_acl_AccessControlEntry_permission_type(entry)));
}

// Same seven fields, but the three strings use 'z' so a NULL (Java's "match
// any") becomes None.
static PyObject* acl_binding_filter_to_py(const kafka_common_acl_AclBindingFilter_t* f) {
    if (f == NULL) Py_RETURN_NONE;
    const kafka_common_resource_ResourcePatternFilter_t* pattern =
        kafka_common_acl_AclBindingFilter_pattern_filter(f);
    const kafka_common_acl_AccessControlEntryFilter_t* entry =
        kafka_common_acl_AclBindingFilter_entry_filter(f);
    return Py_BuildValue(
        "(izizzii)",
        (int)kafka_common_resource_ResourceType_code(
            kafka_common_resource_ResourcePatternFilter_resource_type(pattern)),
        kafka_common_resource_ResourcePatternFilter_name(pattern),
        (int)kafka_common_resource_PatternType_code(
            kafka_common_resource_ResourcePatternFilter_pattern_type(pattern)),
        kafka_common_acl_AccessControlEntryFilter_principal(entry),
        kafka_common_acl_AccessControlEntryFilter_host(entry),
        (int)kafka_common_acl_AclOperation_code(kafka_common_acl_AccessControlEntryFilter_operation(entry)),
        (int)kafka_common_acl_AclPermissionType_code(
            kafka_common_acl_AccessControlEntryFilter_permission_type(entry)));
}

// ((entity_type, entity_name_or_None), ...) -- a None name is Java's null map
// value, the built-in default entity, which is not the empty name.
static PyObject* client_quota_entity_to_py(const kafka_common_quota_ClientQuotaEntity_t* e) {
    if (e == NULL) Py_RETURN_NONE;
    // `entries()` is an owned map of entity type -> entity name (NULL for the
    // default entity), sorted by type; destroyed once copied out.
    kafka_Map_t* entries = kafka_common_quota_ClientQuotaEntity_entries(e);
    int32_t n = entries ? kafka_Map_size(entries) : 0;
    PyObject* pairs = PyTuple_New(n < 0 ? 0 : n);
    if (pairs == NULL) { if (entries) kafka_Map_destroy(entries); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* pair = Py_BuildValue("(sz)",
                                       (const char*)kafka_Map_key(entries, i),
                                       (const char*)kafka_Map_value(entries, i));
        if (pair == NULL) { Py_DECREF(pairs); kafka_Map_destroy(entries); return NULL; }
        PyTuple_SET_ITEM(pairs, i, pair);
    }
    if (entries) kafka_Map_destroy(entries);
    return pairs;
}

static PyObject* kafka_principal_to_py(const kafka_common_security_auth_KafkaPrincipal_t* p) {
    if (p == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(ssO)", kafka_common_security_auth_KafkaPrincipal_principal_type(p),
                         kafka_common_security_auth_KafkaPrincipal_name(p),
                         kafka_common_security_auth_KafkaPrincipal_token_authenticated(p) ? Py_True : Py_False);
}

// ---- options -----------------------------------------------------------------

// Every `<Rpc>Options_t` has `_set_timeout_ms(int32_t)`; -1 leaves Java's
// timeoutMs null so `default.api.timeout.ms` applies.
#define ADMIN_SET_TIMEOUT(Rpc, options, timeout_ms) \
    do { if ((timeout_ms) >= 0) kafka_admin_##Rpc##Options_set_timeout_ms((options), (int32_t)(timeout_ms)); } while (0)

// ---- Python -> C input builders ------------------------------------------------
//
// C-built containers hold BORROWED elements; every builder below owns what it
// put in and frees it after the RPC call (the RPC copies what it keeps).

// list[str] -> kafka_List_t of borrowed char*. Like str_list_build but with a
// caller-supplied error message.
static int str_list_build_msg(PyObject* seq, str_list_t* l, const char* what) {
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, what);
    if (fast == NULL) return -1;
    l->seq = fast;
    l->list = kafka_List_new();
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast, i);  // borrowed
        const char* s = PyUnicode_Check(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (s == NULL) {
            if (!PyErr_Occurred()) PyErr_SetString(PyExc_TypeError, what);
            str_list_free(l);
            return -1;
        }
        kafka_List_add(l->list, (void*)s);
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
    PyMem_Free(l->vals);
    if (l->list) kafka_List_destroy(l->list);
    memset(l, 0, sizeof(*l));
}

static int i32_list_build(PyObject* seq, i32_list_t* l, const char* what) {
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, what);
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    l->list = kafka_List_new();
    l->vals = n > 0 ? PyMem_Calloc(n, sizeof(*l->vals)) : NULL;
    if (n > 0 && l->vals == NULL) { Py_DECREF(fast); i32_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        long v = PyLong_AsLong(PySequence_Fast_GET_ITEM(fast, i));
        if (v == -1 && PyErr_Occurred()) { Py_DECREF(fast); i32_list_free(l); return -1; }
        l->vals[i] = (int32_t)v;
        kafka_List_add(l->list, &l->vals[i]);
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
    PyMem_Free(l->vals);
    if (l->list) kafka_List_destroy(l->list);
    memset(l, 0, sizeof(*l));
}

static int i64_list_build(PyObject* seq, i64_list_t* l, const char* what) {
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, what);
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    l->list = kafka_List_new();
    l->vals = n > 0 ? PyMem_Calloc(n, sizeof(*l->vals)) : NULL;
    if (n > 0 && l->vals == NULL) { Py_DECREF(fast); i64_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        long long v = PyLong_AsLongLong(PySequence_Fast_GET_ITEM(fast, i));
        if (v == -1 && PyErr_Occurred()) { Py_DECREF(fast); i64_list_free(l); return -1; }
        l->vals[i] = (int64_t)v;
        kafka_List_add(l->list, &l->vals[i]);
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
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->replicas && l->replicas[i]) kafka_common_TopicPartitionReplica_destroy(l->replicas[i]);
    }
    PyMem_Free(l->replicas);
    if (l->list) kafka_List_destroy(l->list);
    memset(l, 0, sizeof(*l));
}

// Each row is (topic, partition, broker_id) or, with `with_dir`, (topic,
// partition, broker_id, log_dir): the fourth column is returned through
// `dirs` (borrowed char*, owned by `fast`, which the caller must keep alive).
static int tpr_list_build(PyObject* seq, tpr_list_t* l, int with_dir, PyObject** fast_out,
                          const char*** dirs_out) {
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "expected a sequence of (topic, partition, broker_id) rows");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    l->list = kafka_List_new();
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
        l->replicas[i] = kafka_common_TopicPartitionReplica_new(t, (int32_t)p, (int32_t)b);
        kafka_List_add(l->list, l->replicas[i]);
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
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->ids && l->ids[i]) kafka_common_Uuid_destroy(l->ids[i]);
    }
    PyMem_Free(l->ids);
    if (l->list) kafka_List_destroy(l->list);
    memset(l, 0, sizeof(*l));
}

static int uuid_list_build(PyObject* seq, uuid_list_t* l, kafka_common_Error_t** err_out) {
    memset(l, 0, sizeof(*l));
    *err_out = NULL;
    PyObject* fast = PySequence_Fast(seq, "topic ids must be a sequence of str");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    l->list = kafka_List_new();
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
        kafka_common_Error_t* err = kafka_common_Uuid_from_string(s, &l->ids[i]);
        if (err != NULL) {
            *err_out = prefixed_error("invalid topic id", err);
            Py_DECREF(fast); uuid_list_free(l); return -1;
        }
        kafka_List_add(l->list, l->ids[i]);
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
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->principals && l->principals[i])
            kafka_common_security_auth_KafkaPrincipal_destroy(l->principals[i]);
    }
    PyMem_Free(l->principals);
    if (l->list) kafka_List_destroy(l->list);
    memset(l, 0, sizeof(*l));
}

static int principal_list_build(PyObject* seq, principal_list_t* l) {
    memset(l, 0, sizeof(*l));
    PyObject* fast = PySequence_Fast(seq, "principals must be a sequence of (type, name)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    l->list = kafka_List_new();
    l->principals = n > 0 ? PyMem_Calloc(n, sizeof(*l->principals)) : NULL;
    if (n > 0 && l->principals == NULL) { Py_DECREF(fast); principal_list_free(l); PyErr_NoMemory(); return -1; }
    l->n = n;
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* type = NULL; const char* name = NULL;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "ss", &type, &name)) {
            Py_DECREF(fast); principal_list_free(l); return -1;
        }
        l->principals[i] = kafka_common_security_auth_KafkaPrincipal_new(type, name);
        kafka_List_add(l->list, l->principals[i]);
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
    if (nodes == NULL) Py_RETURN_NONE;
    int32_t n = kafka_List_size(nodes);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* item = node_to_py((const kafka_common_Node_t*)kafka_List_get(nodes, i));
        if (item == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, item);
    }
    return out;
}

// BORROWED list of AclOperation singletons -> [code]; NULL -> None.
static PyObject* acl_operations_to_py_borrowed(const kafka_List_t* ops) {
    if (ops == NULL) Py_RETURN_NONE;
    int32_t n = kafka_List_size(ops);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* code = PyLong_FromLong(kafka_common_acl_AclOperation_code(
            (const kafka_common_acl_AclOperation_t*)kafka_List_get(ops, i)));
        if (code == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, code);
    }
    return out;
}

// (partition, leader, [replicas], [isr], [elr] | None, [last_known_elr] | None)
static PyObject* topic_partition_info_to_py(const kafka_common_TopicPartitionInfo_t* info) {
    PyObject* leader = node_to_py(kafka_common_TopicPartitionInfo_leader(info));
    PyObject* replicas = node_list_to_py(kafka_common_TopicPartitionInfo_replicas(info));
    PyObject* isr = node_list_to_py(kafka_common_TopicPartitionInfo_isr(info));
    PyObject* elr = node_list_to_py(kafka_common_TopicPartitionInfo_elr(info));
    PyObject* last_known_elr = node_list_to_py(kafka_common_TopicPartitionInfo_last_known_elr(info));
    if (!leader || !replicas || !isr || !elr || !last_known_elr) {
        Py_XDECREF(leader); Py_XDECREF(replicas); Py_XDECREF(isr); Py_XDECREF(elr); Py_XDECREF(last_known_elr);
        return NULL;
    }
    // Java's replicas/isr are never null; the FFI gives an empty list, which
    // node_list_to_py would also map to [] (only NULL becomes None).
    return Py_BuildValue("(iNNNNN)", (int)kafka_common_TopicPartitionInfo_partition(info),
                         leader, replicas, isr, elr, last_known_elr);
}

// (name, topic_id, is_internal, [partition_info], [acl_op] | None)
static PyObject* topic_description_to_py(const kafka_admin_TopicDescription_t* d) {
    kafka_List_t* parts = kafka_admin_TopicDescription_partitions(d);  // owned
    int32_t n = parts ? kafka_List_size(parts) : 0;
    PyObject* partitions = PyList_New(n < 0 ? 0 : n);
    if (partitions == NULL) { if (parts) kafka_List_destroy(parts); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* p = topic_partition_info_to_py(
            (const kafka_common_TopicPartitionInfo_t*)kafka_List_get(parts, i));
        if (p == NULL) { Py_DECREF(partitions); kafka_List_destroy(parts); return NULL; }
        PyList_SET_ITEM(partitions, i, p);
    }
    if (parts) kafka_List_destroy(parts);
    PyObject* topic_id = owned_uuid_to_py(kafka_admin_TopicDescription_topic_id(d));
    PyObject* ops = acl_operations_to_py(kafka_admin_TopicDescription_authorized_operations(d));
    if (topic_id == NULL || ops == NULL) {
        Py_XDECREF(topic_id); Py_XDECREF(ops); Py_DECREF(partitions); return NULL;
    }
    const char* name = kafka_admin_TopicDescription_name(d);
    return Py_BuildValue("(sNiNN)", name ? name : "", topic_id,
                         kafka_admin_TopicDescription_is_internal(d) ? 1 : 0, partitions, ops);
}

// (name, topic_id, is_internal)
static PyObject* topic_listing_to_py(const kafka_admin_TopicListing_t* l) {
    PyObject* topic_id = owned_uuid_to_py(kafka_admin_TopicListing_topic_id(l));
    if (topic_id == NULL) return NULL;
    const char* name = kafka_admin_TopicListing_name(l);
    return Py_BuildValue("(sNi)", name ? name : "", topic_id,
                         kafka_admin_TopicListing_is_internal(l) ? 1 : 0);
}

// The 5-tuple `_to_config_entry` expects (createTopics' metadata).
static PyObject* config_entry_short_to_py(const kafka_admin_ConfigEntry_t* e) {
    const char* name = kafka_admin_ConfigEntry_name(e);
    return Py_BuildValue("(sziii)", name ? name : "", kafka_admin_ConfigEntry_value(e),
                         kafka_admin_ConfigEntry_is_default(e) ? 1 : 0,
                         kafka_admin_ConfigEntry_is_sensitive(e) ? 1 : 0,
                         kafka_admin_ConfigEntry_is_read_only(e) ? 1 : 0);
}

// The 9-tuple `_to_full_config_entry` expects: (name, value, is_default,
// is_sensitive, is_read_only, source_name, type_name, documentation | None,
// [(name, value, source_name)]).
static PyObject* config_entry_to_py(const kafka_admin_ConfigEntry_t* e) {
    kafka_List_t* syns = kafka_admin_ConfigEntry_synonyms(e);  // owned
    int32_t n = syns ? kafka_List_size(syns) : 0;
    PyObject* synonyms = PyList_New(n < 0 ? 0 : n);
    if (synonyms == NULL) { if (syns) kafka_List_destroy(syns); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_ConfigEntry_ConfigSynonym_t* s =
            (const kafka_admin_ConfigEntry_ConfigSynonym_t*)kafka_List_get(syns, i);
        const char* sname = kafka_admin_ConfigEntry_ConfigSynonym_name(s);
        PyObject* row = Py_BuildValue("(szs)", sname ? sname : "",
                                      kafka_admin_ConfigEntry_ConfigSynonym_value(s),
                                      config_source_name(kafka_admin_ConfigEntry_ConfigSynonym_source(s)));
        if (row == NULL) { Py_DECREF(synonyms); kafka_List_destroy(syns); return NULL; }
        PyList_SET_ITEM(synonyms, i, row);
    }
    if (syns) kafka_List_destroy(syns);
    const char* name = kafka_admin_ConfigEntry_name(e);
    return Py_BuildValue("(sziiisszN)", name ? name : "", kafka_admin_ConfigEntry_value(e),
                         kafka_admin_ConfigEntry_is_default(e) ? 1 : 0,
                         kafka_admin_ConfigEntry_is_sensitive(e) ? 1 : 0,
                         kafka_admin_ConfigEntry_is_read_only(e) ? 1 : 0,
                         config_source_name(kafka_admin_ConfigEntry_source(e)),
                         config_type_name(kafka_admin_ConfigEntry_type(e)),
                         kafka_admin_ConfigEntry_documentation(e), synonyms);
}

// Config_t -> [entry]; `full` picks the 9-tuple over the 5-tuple.
static PyObject* config_to_py(const kafka_admin_Config_t* c, int full) {
    kafka_List_t* entries = kafka_admin_Config_entries(c);  // owned
    int32_t n = entries ? kafka_List_size(entries) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { if (entries) kafka_List_destroy(entries); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_ConfigEntry_t* e = (const kafka_admin_ConfigEntry_t*)kafka_List_get(entries, i);
        PyObject* row = full ? config_entry_to_py(e) : config_entry_short_to_py(e);
        if (row == NULL) { Py_DECREF(out); kafka_List_destroy(entries); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    if (entries) kafka_List_destroy(entries);
    return out;
}

// (error | None, total_bytes, usable_bytes, [(topic, partition, size, offset_lag, is_future)])
// -1 bytes are Java's empty OptionalLong; admin.py maps them to None.
static PyObject* log_dir_description_to_py(const kafka_admin_LogDirDescription_t* d) {
    kafka_Map_t* infos = kafka_admin_LogDirDescription_replica_infos(d);  // owned TP* -> ReplicaInfo*
    int32_t n = infos ? kafka_Map_size(infos) : 0;
    PyObject* replicas = PyList_New(n < 0 ? 0 : n);
    if (replicas == NULL) { if (infos) kafka_Map_destroy(infos); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_common_TopicPartition_t* tp = (const kafka_common_TopicPartition_t*)kafka_Map_key(infos, i);
        const kafka_admin_ReplicaInfo_t* r = (const kafka_admin_ReplicaInfo_t*)kafka_Map_value(infos, i);
        const char* topic = kafka_common_TopicPartition_topic(tp);
        PyObject* row = Py_BuildValue("(siLLi)", topic ? topic : "",
                                      (int)kafka_common_TopicPartition_partition(tp),
                                      (long long)kafka_admin_ReplicaInfo_size(r),
                                      (long long)kafka_admin_ReplicaInfo_offset_lag(r),
                                      kafka_admin_ReplicaInfo_is_future(r) ? 1 : 0);
        if (row == NULL) { Py_DECREF(replicas); kafka_Map_destroy(infos); return NULL; }
        PyList_SET_ITEM(replicas, i, row);
    }
    if (infos) kafka_Map_destroy(infos);
    PyObject* err = borrowed_error_to_py(kafka_admin_LogDirDescription_error(d));
    if (err == NULL) { Py_DECREF(replicas); return NULL; }
    return Py_BuildValue("(NLLN)", err, (long long)kafka_admin_LogDirDescription_total_bytes(d),
                         (long long)kafka_admin_LogDirDescription_usable_bytes(d), replicas);
}

// MemberAssignment -> [(topic, partition)]; NULL (Java's empty Optional) -> None.
static PyObject* member_assignment_to_py(const kafka_admin_MemberAssignment_t* a) {
    if (a == NULL) Py_RETURN_NONE;
    return tp_list_to_py(kafka_admin_MemberAssignment_topic_partitions(a));
}

// (consumer_id, group_instance_id | None, rack_id | None, client_id, host,
//  assignment, target_assignment | None, member_epoch | None, upgraded | None)
static PyObject* member_description_to_py(const kafka_admin_MemberDescription_t* m) {
    PyObject* assignment = member_assignment_to_py(kafka_admin_MemberDescription_assignment(m));
    PyObject* target = member_assignment_to_py(kafka_admin_MemberDescription_target_assignment(m));
    if (assignment == NULL || target == NULL) { Py_XDECREF(assignment); Py_XDECREF(target); return NULL; }
    int32_t epoch = kafka_admin_MemberDescription_member_epoch(m);
    int8_t upgraded = kafka_admin_MemberDescription_upgraded(m);
    PyObject* py_epoch = epoch < 0 ? (Py_INCREF(Py_None), Py_None) : PyLong_FromLong(epoch);
    PyObject* py_upgraded = upgraded < 0 ? (Py_INCREF(Py_None), Py_None) : PyBool_FromLong(upgraded);
    if (py_epoch == NULL || py_upgraded == NULL) {
        Py_XDECREF(py_epoch); Py_XDECREF(py_upgraded); Py_DECREF(assignment); Py_DECREF(target); return NULL;
    }
    const char* consumer_id = kafka_admin_MemberDescription_consumer_id(m);
    const char* client_id = kafka_admin_MemberDescription_client_id(m);
    const char* host = kafka_admin_MemberDescription_host(m);
    return Py_BuildValue("(szzssNNNN)", consumer_id ? consumer_id : "",
                         kafka_admin_MemberDescription_group_instance_id(m),
                         kafka_admin_MemberDescription_rack_id(m),
                         client_id ? client_id : "", host ? host : "",
                         assignment, target, py_epoch, py_upgraded);
}

// OWNED list of MemberDescription* -> [member]; frees it.
static PyObject* member_list_to_py(kafka_List_t* members) {
    int32_t n = members ? kafka_List_size(members) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { if (members) kafka_List_destroy(members); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* m = member_description_to_py((const kafka_admin_MemberDescription_t*)kafka_List_get(members, i));
        if (m == NULL) { Py_DECREF(out); kafka_List_destroy(members); return NULL; }
        PyList_SET_ITEM(out, i, m);
    }
    if (members) kafka_List_destroy(members);
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
    PyObject* members = member_list_to_py(kafka_admin_ConsumerGroupDescription_members(d));
    PyObject* group_type = group_type_to_py(kafka_admin_ConsumerGroupDescription_type(d));
    PyObject* group_state = group_state_to_py(kafka_admin_ConsumerGroupDescription_group_state(d));
    PyObject* coordinator = node_to_py(kafka_admin_ConsumerGroupDescription_coordinator(d));
    PyObject* ops = acl_operations_to_py(kafka_admin_ConsumerGroupDescription_authorized_operations(d));
    PyObject* group_epoch = optional_i32_to_py(kafka_admin_ConsumerGroupDescription_group_epoch(d));
    PyObject* target_epoch = optional_i32_to_py(kafka_admin_ConsumerGroupDescription_target_assignment_epoch(d));
    if (!members || !group_type || !group_state || !coordinator || !ops || !group_epoch || !target_epoch) {
        Py_XDECREF(members); Py_XDECREF(group_type); Py_XDECREF(group_state); Py_XDECREF(coordinator);
        Py_XDECREF(ops); Py_XDECREF(group_epoch); Py_XDECREF(target_epoch);
        return NULL;
    }
    const char* group_id = kafka_admin_ConsumerGroupDescription_group_id(d);
    const char* assignor = kafka_admin_ConsumerGroupDescription_partition_assignor(d);
    return Py_BuildValue("(sONsNNNNNN)", group_id ? group_id : "",
                         kafka_admin_ConsumerGroupDescription_is_simple_consumer_group(d) ? Py_True : Py_False,
                         members, assignor ? assignor : "", group_type, group_state, coordinator, ops,
                         group_epoch, target_epoch);
}

// (group_id, protocol, protocol_data, is_simple, [member], state_name,
//  coordinator, [acl_op] | None)
static PyObject* classic_group_description_to_py(const kafka_admin_ClassicGroupDescription_t* d) {
    PyObject* members = member_list_to_py(kafka_admin_ClassicGroupDescription_members(d));
    PyObject* state = classic_group_state_to_py(kafka_admin_ClassicGroupDescription_state(d));
    PyObject* coordinator = node_to_py(kafka_admin_ClassicGroupDescription_coordinator(d));
    PyObject* ops = acl_operations_to_py(kafka_admin_ClassicGroupDescription_authorized_operations(d));
    if (!members || !state || !coordinator || !ops) {
        Py_XDECREF(members); Py_XDECREF(state); Py_XDECREF(coordinator); Py_XDECREF(ops);
        return NULL;
    }
    const char* group_id = kafka_admin_ClassicGroupDescription_group_id(d);
    const char* protocol = kafka_admin_ClassicGroupDescription_protocol(d);
    const char* protocol_data = kafka_admin_ClassicGroupDescription_protocol_data(d);
    return Py_BuildValue("(sssONNNN)", group_id ? group_id : "", protocol ? protocol : "",
                         protocol_data ? protocol_data : "",
                         kafka_admin_ClassicGroupDescription_is_simple_consumer_group(d) ? Py_True : Py_False,
                         members, state, coordinator, ops);
}

// (group_id, group_type_name | None, protocol, group_state_name | None, is_simple)
static PyObject* group_listing_to_py(const kafka_admin_GroupListing_t* g) {
    PyObject* type = group_type_to_py(kafka_admin_GroupListing_type(g));
    PyObject* state = group_state_to_py(kafka_admin_GroupListing_group_state(g));
    if (type == NULL || state == NULL) { Py_XDECREF(type); Py_XDECREF(state); return NULL; }
    const char* group_id = kafka_admin_GroupListing_group_id(g);
    const char* protocol = kafka_admin_GroupListing_protocol(g);
    return Py_BuildValue("(sNsNO)", group_id ? group_id : "", type, protocol ? protocol : "", state,
                         kafka_admin_GroupListing_is_simple_consumer_group(g) ? Py_True : Py_False);
}

// (offset, metadata, leader_epoch | None); NULL (Java null) -> None.
static PyObject* offset_and_metadata_to_py(const kafka_consumer_OffsetAndMetadata_t* om) {
    if (om == NULL) Py_RETURN_NONE;
    PyObject* epoch = optional_i32_to_py(kafka_consumer_OffsetAndMetadata_leader_epoch(om));
    if (epoch == NULL) return NULL;
    const char* metadata = kafka_consumer_OffsetAndMetadata_metadata(om);
    return Py_BuildValue("(LsN)", (long long)kafka_consumer_OffsetAndMetadata_offset(om),
                         metadata ? metadata : "", epoch);
}

// (token_id, owner, requester, [renewer], issue_ts, expiry_ts, max_ts, hmac, hmac_base64)
static PyObject* delegation_token_to_py(const kafka_common_security_token_delegation_DelegationToken_t* t) {
    if (t == NULL) Py_RETURN_NONE;
    const kafka_common_security_token_delegation_TokenInformation_t* info =
        kafka_common_security_token_delegation_DelegationToken_token_info(t);
    kafka_List_t* renewer_list = kafka_common_security_token_delegation_TokenInformation_renewers(info);  // owned
    int32_t n = renewer_list ? kafka_List_size(renewer_list) : 0;
    PyObject* renewers = PyList_New(n < 0 ? 0 : n);
    if (renewers == NULL) { if (renewer_list) kafka_List_destroy(renewer_list); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* p = kafka_principal_to_py(
            (const kafka_common_security_auth_KafkaPrincipal_t*)kafka_List_get(renewer_list, i));
        if (p == NULL) { Py_DECREF(renewers); kafka_List_destroy(renewer_list); return NULL; }
        PyList_SET_ITEM(renewers, i, p);
    }
    if (renewer_list) kafka_List_destroy(renewer_list);
    PyObject* owner = kafka_principal_to_py(kafka_common_security_token_delegation_TokenInformation_owner(info));
    PyObject* requester = kafka_principal_to_py(
        kafka_common_security_token_delegation_TokenInformation_token_requester(info));
    kafka_Bytes_t hmac = kafka_common_security_token_delegation_DelegationToken_hmac(t);  // borrowed bytes
    PyObject* py_hmac = PyBytes_FromStringAndSize((const char*)hmac.data, hmac.data ? (Py_ssize_t)hmac.len : 0);
    char* b64 = kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string(t);  // owned
    PyObject* py_b64 = PyUnicode_FromString(b64 ? b64 : "");
    if (b64) kafka_string_destroy(b64);
    if (!owner || !requester || !py_hmac || !py_b64) {
        Py_XDECREF(owner); Py_XDECREF(requester); Py_XDECREF(py_hmac); Py_XDECREF(py_b64); Py_DECREF(renewers);
        return NULL;
    }
    const char* token_id = kafka_common_security_token_delegation_TokenInformation_token_id(info);
    return Py_BuildValue("(sNNNLLLNN)", token_id ? token_id : "", owner, requester, renewers,
                         (long long)kafka_common_security_token_delegation_TokenInformation_issue_timestamp(info),
                         (long long)kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(info),
                         (long long)kafka_common_security_token_delegation_TokenInformation_max_timestamp(info),
                         py_hmac, py_b64);
}

// (producer_id, producer_epoch, last_sequence, last_timestamp,
//  coordinator_epoch | None, current_transaction_start_offset | None)
static PyObject* producer_state_to_py(const kafka_admin_ProducerState_t* s) {
    PyObject* coord = optional_i32_to_py(kafka_admin_ProducerState_coordinator_epoch(s));
    PyObject* start = optional_i64_to_py(kafka_admin_ProducerState_current_transaction_start_offset(s));
    if (coord == NULL || start == NULL) { Py_XDECREF(coord); Py_XDECREF(start); return NULL; }
    return Py_BuildValue("(LiiLNN)", (long long)kafka_admin_ProducerState_producer_id(s),
                         (int)kafka_admin_ProducerState_producer_epoch(s),
                         (int)kafka_admin_ProducerState_last_sequence(s),
                         (long long)kafka_admin_ProducerState_last_timestamp(s), coord, start);
}

// (coordinator_id, state_name, producer_id, producer_epoch, timeout_ms,
//  start_time_ms | None, [(topic, partition)])
static PyObject* transaction_description_to_py(const kafka_admin_TransactionDescription_t* d) {
    PyObject* state = transaction_state_to_py(kafka_admin_TransactionDescription_state(d));
    PyObject* start = optional_i64_to_py(kafka_admin_TransactionDescription_transaction_start_time_ms(d));
    PyObject* partitions = tp_list_to_py(kafka_admin_TransactionDescription_topic_partitions(d));
    if (!state || !start || !partitions) {
        Py_XDECREF(state); Py_XDECREF(start); Py_XDECREF(partitions); return NULL;
    }
    return Py_BuildValue("(iNLiLNN)", (int)kafka_admin_TransactionDescription_coordinator_id(d), state,
                         (long long)kafka_admin_TransactionDescription_producer_id(d),
                         (int)kafka_admin_TransactionDescription_producer_epoch(d),
                         (long long)kafka_admin_TransactionDescription_transaction_timeout_ms(d),
                         start, partitions);
}

// (transactional_id, producer_id, state_name)
static PyObject* transaction_listing_to_py(const kafka_admin_TransactionListing_t* l) {
    PyObject* state = transaction_state_to_py(kafka_admin_TransactionListing_state(l));
    if (state == NULL) return NULL;
    const char* tid = kafka_admin_TransactionListing_transactional_id(l);
    return Py_BuildValue("(sLN)", tid ? tid : "", (long long)kafka_admin_TransactionListing_producer_id(l), state);
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
    const kafka_List_t* list = (const kafka_List_t*)v;
    int32_t n = list ? kafka_List_size(list) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        const char* s = (const char*)kafka_List_get(list, i);
        PyObject* item = PyUnicode_FromString(s ? s : "");
        if (item == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, item);
    }
    return out;
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
    const kafka_Map_t* map = (const kafka_Map_t*)v;
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int32_t n = map ? kafka_Map_size(map) : 0;
    for (int32_t i = 0; i < n; i++) {
        const char* name = (const char*)kafka_Map_key(map, i);
        PyObject* row = topic_listing_to_py((const kafka_admin_TopicListing_t*)kafka_Map_value(map, i));
        if (row == NULL || PyDict_SetItemString(d, name ? name : "", row) < 0) { Py_XDECREF(row); Py_DECREF(d); return NULL; }
        Py_DECREF(row);
    }
    return d;
}

static PyObject* conv_deleted_records(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) return PyLong_FromLong(-1);
    return PyLong_FromLongLong((long long)kafka_admin_DeletedRecords_low_watermark((const kafka_admin_DeletedRecords_t*)v));
}

// [(type_id, name)]
static PyObject* conv_config_resources(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_List_t* list = (const kafka_List_t*)v;
    int32_t n = list ? kafka_List_size(list) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* row = config_resource_to_py((const kafka_common_config_ConfigResource_t*)kafka_List_get(list, i));
        if (row == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
}

// {log_dir: (error, total_bytes, usable_bytes, [replica])}
static PyObject* conv_log_dir_descriptions(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_Map_t* map = (const kafka_Map_t*)v;
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int32_t n = map ? kafka_Map_size(map) : 0;
    for (int32_t i = 0; i < n; i++) {
        const char* dir = (const char*)kafka_Map_key(map, i);
        PyObject* row = log_dir_description_to_py((const kafka_admin_LogDirDescription_t*)kafka_Map_value(map, i));
        if (row == NULL || PyDict_SetItemString(d, dir ? dir : "", row) < 0) { Py_XDECREF(row); Py_DECREF(d); return NULL; }
        Py_DECREF(row);
    }
    return d;
}

// (current_dir | None, current_lag, future_dir | None, future_lag)
static PyObject* conv_replica_log_dir_info(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t* info =
        (const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t*)v;
    if (info == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(zLzL)",
                         kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir(info),
                         (long long)kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_offset_lag(info),
                         kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir(info),
                         (long long)kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_offset_lag(info));
}

// {(topic, partition): ([replicas], [adding], [removing])}
static PyObject* conv_partition_reassignments(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_Map_t* map = (const kafka_Map_t*)v;
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int32_t n = map ? kafka_Map_size(map) : 0;
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_PartitionReassignment_t* r = (const kafka_admin_PartitionReassignment_t*)kafka_Map_value(map, i);
        PyObject* key = tp_to_py((const kafka_common_TopicPartition_t*)kafka_Map_key(map, i));
        PyObject* replicas = int32_list_to_py(kafka_admin_PartitionReassignment_replicas(r));
        PyObject* adding = int32_list_to_py(kafka_admin_PartitionReassignment_adding_replicas(r));
        PyObject* removing = int32_list_to_py(kafka_admin_PartitionReassignment_removing_replicas(r));
        if (!key || !replicas || !adding || !removing) {
            Py_XDECREF(key); Py_XDECREF(replicas); Py_XDECREF(adding); Py_XDECREF(removing); Py_DECREF(d); return NULL;
        }
        PyObject* value = Py_BuildValue("(NNN)", replicas, adding, removing);
        if (value == NULL || PyDict_SetItem(d, key, value) < 0) { Py_XDECREF(value); Py_DECREF(key); Py_DECREF(d); return NULL; }
        Py_DECREF(key); Py_DECREF(value);
    }
    return d;
}

// {(topic, partition): error4 | None}
static PyObject* conv_elect_leaders(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_Map_t* map = (const kafka_Map_t*)v;
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int32_t n = map ? kafka_Map_size(map) : 0;
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = tp_to_py((const kafka_common_TopicPartition_t*)kafka_Map_key(map, i));
        PyObject* err = borrowed_error_to_py((const kafka_common_Error_t*)kafka_Map_value(map, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) { Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d); return NULL; }
        Py_DECREF(key); Py_DECREF(err);
    }
    return d;
}

// (offset, timestamp, leader_epoch | None)
static PyObject* conv_list_offsets_info(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t* info =
        (const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t*)v;
    if (info == NULL) Py_RETURN_NONE;
    PyObject* epoch = optional_i32_to_py(kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch(info));
    if (epoch == NULL) return NULL;
    return Py_BuildValue("(LLN)", (long long)kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(info),
                         (long long)kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp(info), epoch);
}

// [(group_id, type, protocol, state, is_simple)]
static PyObject* conv_group_listings(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_List_t* list = (const kafka_List_t*)v;
    int32_t n = list ? kafka_List_size(list) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* row = group_listing_to_py((const kafka_admin_GroupListing_t*)kafka_List_get(list, i));
        if (row == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
}

// [error4]
static PyObject* conv_error_list(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_List_t* list = (const kafka_List_t*)v;
    int32_t n = list ? kafka_List_size(list) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* row = borrowed_error_to_py((const kafka_common_Error_t*)kafka_List_get(list, i));
        if (row == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
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
    const kafka_Map_t* map = (const kafka_Map_t*)v;
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int32_t n = map ? kafka_Map_size(map) : 0;
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = tp_to_py((const kafka_common_TopicPartition_t*)kafka_Map_key(map, i));
        PyObject* value = offset_and_metadata_to_py((const kafka_consumer_OffsetAndMetadata_t*)kafka_Map_value(map, i));
        if (!key || !value || PyDict_SetItem(d, key, value) < 0) { Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d); return NULL; }
        Py_DECREF(key); Py_DECREF(value);
    }
    return d;
}

// [binding7]
static PyObject* conv_acl_bindings(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_List_t* list = (const kafka_List_t*)v;
    int32_t n = list ? kafka_List_size(list) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* row = acl_binding_to_py((const kafka_common_acl_AclBinding_t*)kafka_List_get(list, i));
        if (row == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
}

// FilterResults -> [(error4 | None, binding7 | None)]
static PyObject* conv_filter_results(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_admin_DeleteAclsResult_FilterResults_t* results = (const kafka_admin_DeleteAclsResult_FilterResults_t*)v;
    if (results == NULL) return PyList_New(0);
    kafka_List_t* values = kafka_admin_DeleteAclsResult_FilterResults_values(results);  // owned
    int32_t n = values ? kafka_List_size(values) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { if (values) kafka_List_destroy(values); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_DeleteAclsResult_FilterResult_t* r =
            (const kafka_admin_DeleteAclsResult_FilterResult_t*)kafka_List_get(values, i);
        PyObject* err = borrowed_error_to_py(kafka_admin_DeleteAclsResult_FilterResult_error(r));
        PyObject* binding = acl_binding_to_py(kafka_admin_DeleteAclsResult_FilterResult_binding(r));
        PyObject* row = error_value_pair(err, binding);
        if (row == NULL) { Py_DECREF(out); kafka_List_destroy(values); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    if (values) kafka_List_destroy(values);
    return out;
}

// {entity_pairs: [(quota_key, value)]}
static PyObject* conv_quota_entities(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_Map_t* map = (const kafka_Map_t*)v;
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    int32_t n = map ? kafka_Map_size(map) : 0;
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = client_quota_entity_to_py((const kafka_common_quota_ClientQuotaEntity_t*)kafka_Map_key(map, i));
        if (key == NULL) { Py_DECREF(d); return NULL; }
        const kafka_Map_t* quotas = (const kafka_Map_t*)kafka_Map_value(map, i);
        int32_t m = quotas ? kafka_Map_size(quotas) : 0;
        PyObject* rows = PyList_New(m < 0 ? 0 : m);
        if (rows == NULL) { Py_DECREF(key); Py_DECREF(d); return NULL; }
        for (int32_t j = 0; j < m; j++) {
            const char* qk = (const char*)kafka_Map_key(quotas, j);
            const double* qv = (const double*)kafka_Map_value(quotas, j);
            PyObject* row = Py_BuildValue("(sd)", qk ? qk : "", qv ? *qv : 0.0);
            if (row == NULL) { Py_DECREF(rows); Py_DECREF(key); Py_DECREF(d); return NULL; }
            PyList_SET_ITEM(rows, j, row);
        }
        if (PyDict_SetItem(d, key, rows) < 0) { Py_DECREF(rows); Py_DECREF(key); Py_DECREF(d); return NULL; }
        Py_DECREF(key); Py_DECREF(rows);
    }
    return d;
}

static PyObject* conv_delegation_token(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    return delegation_token_to_py((const kafka_common_security_token_delegation_DelegationToken_t*)v);
}

static PyObject* conv_delegation_tokens(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_List_t* list = (const kafka_List_t*)v;
    int32_t n = list ? kafka_List_size(list) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* row = delegation_token_to_py(
            (const kafka_common_security_token_delegation_DelegationToken_t*)kafka_List_get(list, i));
        if (row == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
}

// UserScramCredentialsDescription -> [(mechanism_type, iterations)]
static PyObject* conv_scram_description(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_admin_UserScramCredentialsDescription_t* d = (const kafka_admin_UserScramCredentialsDescription_t*)v;
    if (d == NULL) return PyList_New(0);
    kafka_List_t* infos = kafka_admin_UserScramCredentialsDescription_credential_infos(d);  // owned
    int32_t n = infos ? kafka_List_size(infos) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { if (infos) kafka_List_destroy(infos); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_ScramCredentialInfo_t* info = (const kafka_admin_ScramCredentialInfo_t*)kafka_List_get(infos, i);
        PyObject* row = Py_BuildValue("(ii)",
                                      (int)kafka_admin_ScramMechanism_type(kafka_admin_ScramCredentialInfo_mechanism(info)),
                                      (int)kafka_admin_ScramCredentialInfo_iterations(info));
        if (row == NULL) { Py_DECREF(out); kafka_List_destroy(infos); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    if (infos) kafka_List_destroy(infos);
    return out;
}

// ([(feature, min, max)], epoch | None, [(feature, min, max)])
static PyObject* conv_feature_metadata(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_admin_FeatureMetadata_t* m = (const kafka_admin_FeatureMetadata_t*)v;
    if (m == NULL) Py_RETURN_NONE;
    kafka_Map_t* finalized = kafka_admin_FeatureMetadata_finalized_features(m);  // owned
    int32_t n = finalized ? kafka_Map_size(finalized) : 0;
    PyObject* py_finalized = PyList_New(n < 0 ? 0 : n);
    if (py_finalized == NULL) { if (finalized) kafka_Map_destroy(finalized); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_FinalizedVersionRange_t* r = (const kafka_admin_FinalizedVersionRange_t*)kafka_Map_value(finalized, i);
        const char* name = (const char*)kafka_Map_key(finalized, i);
        PyObject* row = Py_BuildValue("(sii)", name ? name : "",
                                      (int)kafka_admin_FinalizedVersionRange_min_version_level(r),
                                      (int)kafka_admin_FinalizedVersionRange_max_version_level(r));
        if (row == NULL) { Py_DECREF(py_finalized); kafka_Map_destroy(finalized); return NULL; }
        PyList_SET_ITEM(py_finalized, i, row);
    }
    if (finalized) kafka_Map_destroy(finalized);
    kafka_Map_t* supported = kafka_admin_FeatureMetadata_supported_features(m);  // owned
    n = supported ? kafka_Map_size(supported) : 0;
    PyObject* py_supported = PyList_New(n < 0 ? 0 : n);
    if (py_supported == NULL) { if (supported) kafka_Map_destroy(supported); Py_DECREF(py_finalized); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_SupportedVersionRange_t* r = (const kafka_admin_SupportedVersionRange_t*)kafka_Map_value(supported, i);
        const char* name = (const char*)kafka_Map_key(supported, i);
        PyObject* row = Py_BuildValue("(sii)", name ? name : "",
                                      (int)kafka_admin_SupportedVersionRange_min_version(r),
                                      (int)kafka_admin_SupportedVersionRange_max_version(r));
        if (row == NULL) { Py_DECREF(py_supported); Py_DECREF(py_finalized); kafka_Map_destroy(supported); return NULL; }
        PyList_SET_ITEM(py_supported, i, row);
    }
    if (supported) kafka_Map_destroy(supported);
    PyObject* epoch = optional_i64_to_py(kafka_admin_FeatureMetadata_finalized_features_epoch(m));
    if (epoch == NULL) { Py_DECREF(py_supported); Py_DECREF(py_finalized); return NULL; }
    return Py_BuildValue("(NNN)", py_finalized, epoch, py_supported);
}

// PartitionProducerState -> [producer_state]
static PyObject* conv_partition_producer_state(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_admin_DescribeProducersResult_PartitionProducerState_t* s =
        (const kafka_admin_DescribeProducersResult_PartitionProducerState_t*)v;
    if (s == NULL) return PyList_New(0);
    kafka_List_t* producers = kafka_admin_DescribeProducersResult_PartitionProducerState_active_producers(s);  // owned
    int32_t n = producers ? kafka_List_size(producers) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { if (producers) kafka_List_destroy(producers); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* row = producer_state_to_py((const kafka_admin_ProducerState_t*)kafka_List_get(producers, i));
        if (row == NULL) { Py_DECREF(out); kafka_List_destroy(producers); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    if (producers) kafka_List_destroy(producers);
    return out;
}

static PyObject* conv_transaction_description(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    if (v == NULL) Py_RETURN_NONE;
    return transaction_description_to_py((const kafka_admin_TransactionDescription_t*)v);
}

// [(transactional_id, producer_id, state)]
static PyObject* conv_transaction_listings(AdminJob* job, AdminSlot* slot, void* v) {
    (void)job; (void)slot;
    const kafka_List_t* list = (const kafka_List_t*)v;
    int32_t n = list ? kafka_List_size(list) : 0;
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* row = transaction_listing_to_py((const kafka_admin_TransactionListing_t*)kafka_List_get(list, i));
        if (row == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, row);
    }
    return out;
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
    if (job_own_map(job, map) < 0) return -1;
    if (map == NULL) return 0;
    int32_t n = kafka_Map_size(map);
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = key_fn(kafka_Map_key(map, i));
        if (key == NULL) return -1;
        kafka_common_KafkaFuture_t* f = (kafka_common_KafkaFuture_t*)kafka_Map_value(map, i);
        if (job_add_slot(job, f, 0, key, conv, NULL, tag) == NULL) return -1;
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
#define JOB_RESULT_DESTROY(Rpc) ((void (*)(void*))kafka_admin_##Rpc##Result_destroy)

// ---- owned-handle containers --------------------------------------------------
//
// A C-built kafka_List_t / kafka_Map_t holds BORROWED elements, so the typed
// handles built from Python rows are tracked beside the container and
// destroyed with it after the RPC (which copies what it keeps).

typedef void (*handle_destroy_fn)(void*);

typedef struct {
    kafka_List_t* list;
    void** items;
    Py_ssize_t n, cap;
    handle_destroy_fn destroy;   // NULL: elements are borrowed
} obj_list_t;

static int obj_list_init(obj_list_t* l, Py_ssize_t cap, handle_destroy_fn destroy) {
    memset(l, 0, sizeof(*l));
    l->destroy = destroy;
    l->list = kafka_List_new();
    l->items = cap > 0 ? (void**)PyMem_Calloc((size_t)cap, sizeof(void*)) : NULL;
    if (cap > 0 && l->items == NULL) {
        kafka_List_destroy(l->list); l->list = NULL; PyErr_NoMemory(); return -1;
    }
    l->cap = cap;
    return 0;
}

static void obj_list_add(obj_list_t* l, void* item) {
    l->items[l->n++] = item;
    kafka_List_add(l->list, item);
}

static void obj_list_free(obj_list_t* l) {
    for (Py_ssize_t i = 0; i < l->n; i++) {
        if (l->destroy && l->items[i]) l->destroy(l->items[i]);
    }
    PyMem_Free(l->items);
    if (l->list) kafka_List_destroy(l->list);
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
    memset(m, 0, sizeof(*m));
    m->key_destroy = key_destroy;
    m->val_destroy = val_destroy;
    m->map = kafka_Map_new();
    m->keys = cap > 0 ? (void**)PyMem_Calloc((size_t)cap, sizeof(void*)) : NULL;
    m->vals = cap > 0 ? (void**)PyMem_Calloc((size_t)cap, sizeof(void*)) : NULL;
    if (cap > 0 && (m->keys == NULL || m->vals == NULL)) {
        PyMem_Free(m->keys); PyMem_Free(m->vals);
        kafka_Map_destroy(m->map); m->map = NULL; PyErr_NoMemory(); return -1;
    }
    m->cap = cap;
    return 0;
}

// `value` may be NULL where the RPC reads NULL as Java's null.
static void obj_map_put(obj_map_t* m, void* key, void* value) {
    m->keys[m->n] = key;
    m->vals[m->n] = value;
    m->n++;
    kafka_Map_put(m->map, key, value);
}

static void obj_map_free(obj_map_t* m) {
    for (Py_ssize_t i = 0; i < m->n; i++) {
        if (m->key_destroy && m->keys[i]) m->key_destroy(m->keys[i]);
        if (m->val_destroy && m->vals[i]) m->val_destroy(m->vals[i]);
    }
    PyMem_Free(m->keys); PyMem_Free(m->vals);
    if (m->map) kafka_Map_destroy(m->map);
    memset(m, 0, sizeof(*m));
}

// Boxes an int32 for the `const int32_t *` scalars the containers carry.
static int32_t* box_i32(int32_t v) {
    int32_t* p = (int32_t*)PyMem_Malloc(sizeof(*p));
    if (p == NULL) { PyErr_NoMemory(); return NULL; }
    *p = v;
    return p;
}

static int16_t* box_i16(int16_t v) {
    int16_t* p = (int16_t*)PyMem_Malloc(sizeof(*p));
    if (p == NULL) { PyErr_NoMemory(); return NULL; }
    *p = v;
    return p;
}

static int64_t* box_i64(int64_t v) {
    int64_t* p = (int64_t*)PyMem_Malloc(sizeof(*p));
    if (p == NULL) { PyErr_NoMemory(); return NULL; }
    *p = v;
    return p;
}

static void pymem_free(void* p) { PyMem_Free(p); }
static void destroy_list(void* l) { kafka_List_destroy((kafka_List_t*)l); }

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
        if (b == NULL) { Py_DECREF(fast); obj_list_free(&l); return NULL; }
        obj_list_add(&l, b);
    }
    Py_DECREF(fast);
    // Hand the list out; i32_list_boxed_free frees the boxed elements with it.
    kafka_List_t* out = l.list;
    PyMem_Free(l.items);
    return out;
}

// Frees a list built by i32_list_boxed (elements then container).
static void i32_list_boxed_free(kafka_List_t* list) {
    if (list == NULL) return;
    int32_t n = kafka_List_size(list);
    for (int32_t i = 0; i < n; i++) PyMem_Free(kafka_List_get(list, i));
    kafka_List_destroy(list);
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

#define ADMIN_OPTIONS(Rpc, var, timeout_ms) \
    kafka_admin_##Rpc##Options_t* var = kafka_admin_##Rpc##Options_new(); \
    ADMIN_SET_TIMEOUT(Rpc, var, timeout_ms)

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
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only, retry;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &spec, &timeout_ms, &validate_only, &retry)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "new_topics must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t topics;
    if (obj_list_init(&topics, n, (handle_destroy_fn)kafka_admin_NewTopic_destroy) < 0) { Py_DECREF(fast); return NULL; }
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
            if (obj_map_init(&m, n_assign, pymem_free, (handle_destroy_fn)i32_list_boxed_free) < 0) { Py_DECREF(afast); goto fail; }
            int ok = 1;
            for (Py_ssize_t j = 0; j < n_assign && ok; j++) {
                int partition; PyObject* brokers;
                if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(afast, j), "iO", &partition, &brokers)) { ok = 0; break; }
                int32_t* key = box_i32((int32_t)partition);
                if (key == NULL) { ok = 0; break; }
                kafka_List_t* blist = i32_list_boxed(brokers, "broker ids must be a sequence of int");
                if (blist == NULL) { PyMem_Free(key); ok = 0; break; }
                obj_map_put(&m, key, blist);
            }
            Py_DECREF(afast);
            if (ok) t = kafka_admin_NewTopic_with_replicas_assignments(name, m.map);
            obj_map_free(&m);
            if (!ok) goto fail;
        } else {
            t = kafka_admin_NewTopic_with_num_partitions_replication_factor(name, (int32_t)np, (int16_t)rf);
        }
        obj_list_add(&topics, t);
        Py_ssize_t n_conf = PySequence_Size(configs);
        if (n_conf < 0) goto fail;
        if (n_conf > 0) {
            PyObject* cfast = PySequence_Fast(configs, "configs must be a sequence of (key, value)");
            if (cfast == NULL) goto fail;
            kafka_Map_t* cmap = kafka_Map_new();   // borrowed char* -> char* (owned by cfast's str objects)
            int ok = 1;
            for (Py_ssize_t j = 0; j < n_conf; j++) {
                const char *k, *v;
                if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(cfast, j), "ss", &k, &v)) { ok = 0; break; }
                kafka_Map_put(cmap, (void*)k, (void*)v);
            }
            if (ok) kafka_admin_NewTopic_set_configs(t, cmap);
            kafka_Map_destroy(cmap);
            Py_DECREF(cfast);
            if (!ok) goto fail;
        }
    }
    {
        ADMIN_OPTIONS(CreateTopics, o, timeout_ms);
        kafka_admin_CreateTopicsOptions_set_validate_only(o, (int8_t)validate_only);
        kafka_admin_CreateTopicsOptions_set_retry_on_quota_violation(o, (int8_t)retry);
        kafka_admin_CreateTopicsResult_t* r = kafka_admin_Admin_create_topics_with_options(a->admin, topics.list, o);
        kafka_admin_CreateTopicsOptions_destroy(o);
        obj_list_free(&topics);
        Py_DECREF(fast);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreateTopics), assemble_create_topics);
        if (job == NULL) { kafka_admin_CreateTopicsResult_destroy(r); return NULL; }
        // Five slots per topic, in CT_* order: the Void future from values()
        // and the four refinements (owned, client-less, so they fire inline).
        kafka_Map_t* values = kafka_admin_CreateTopicsResult_values(r);
        JOB_TRY(job, job_own_map(job, values));
        int32_t nv = values ? kafka_Map_size(values) : 0;
        for (int32_t i = 0; i < nv; i++) {
            const char* name = (const char*)kafka_Map_key(values, i);
            kafka_common_KafkaFuture_t* f = (kafka_common_KafkaFuture_t*)kafka_Map_value(values, i);
            PyObject* key = key_str(name);
            if (key == NULL) { job_free(job); return NULL; }
            if (job_add_slot(job, f, 0, key, NULL, NULL, CT_VALUES) == NULL) { job_free(job); return NULL; }
            JOB_TRY(job, job_add_single(job, kafka_admin_CreateTopicsResult_config(r, name), 1, conv_config_short, CT_CONFIG));
            JOB_TRY(job, job_add_single(job, kafka_admin_CreateTopicsResult_topic_id(r, name), 1, conv_uuid, CT_TOPIC_ID));
            JOB_TRY(job, job_add_single(job, kafka_admin_CreateTopicsResult_num_partitions(r, name), 1, conv_i32_ptr, CT_NUM_PARTITIONS));
            JOB_TRY(job, job_add_single(job, kafka_admin_CreateTopicsResult_replication_factor(r, name), 1, conv_i32_ptr, CT_REPLICATION_FACTOR));
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
    *out = NULL; *err_out = NULL;
    if (by_ids) {
        uuid_list_t ids;
        if (uuid_list_build(names, &ids, err_out) < 0) return -1;
        *out = kafka_common_TopicCollection_of_topic_ids(ids.list);
        uuid_list_free(&ids);
    } else {
        str_list_t l;
        if (str_list_build(names, &l) < 0) return -1;
        *out = kafka_common_TopicCollection_of_topic_names(l.list);
        str_list_free(&l);
    }
    return 0;
}

// Admin_delete_topics(h, [name|id], timeout_ms, retry_on_quota_violation, by_ids)
// -> (job, err). Payload: {name|id: error}.
static PyObject* py_Admin_delete_topics(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* names; long long timeout_ms; int retry, by_ids;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &names, &timeout_ms, &retry, &by_ids)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_TopicCollection_t* topics; kafka_common_Error_t* err;
    if (topic_collection_build(names, by_ids, &topics, &err) < 0) return err ? job_or_error(NULL, err) : NULL;
    ADMIN_OPTIONS(DeleteTopics, o, timeout_ms);
    kafka_admin_DeleteTopicsOptions_set_retry_on_quota_violation(o, (int8_t)retry);
    kafka_admin_DeleteTopicsResult_t* r = kafka_admin_Admin_delete_topics_with_options(a->admin, topics, o);
    kafka_admin_DeleteTopicsOptions_destroy(o);
    kafka_common_TopicCollection_destroy(topics);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteTopics), assemble_keyed_errors);
    if (job == NULL) { kafka_admin_DeleteTopicsResult_destroy(r); return NULL; }
    if (by_ids) JOB_TRY(job, job_add_map(job, kafka_admin_DeleteTopicsResult_topic_id_values(r), key_uuid, NULL, 0));
    else JOB_TRY(job, job_add_map(job, kafka_admin_DeleteTopicsResult_topic_name_values(r), key_str, NULL, 0));
    return job_or_error(job, NULL);
}

// Admin_list_topics(h, timeout_ms, list_internal) -> (job, err).
// Payload: (error, {name: (name, topic_id, is_internal)}).
static PyObject* py_Admin_list_topics(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms; int list_internal;
    if (!PyArg_ParseTuple(args, "KLp", &h, &timeout_ms, &list_internal)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(ListTopics, o, timeout_ms);
    kafka_admin_ListTopicsOptions_set_list_internal(o, (int8_t)list_internal);
    kafka_admin_ListTopicsResult_t* r = kafka_admin_Admin_list_topics_with_options(a->admin, o);
    kafka_admin_ListTopicsOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListTopics), assemble_single);
    if (job == NULL) { kafka_admin_ListTopicsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_ListTopicsResult_names_to_listings(r), 1, conv_topic_listings, 0));
    return job_or_error(job, NULL);
}

// Admin_describe_topics(h, [name|id], timeout_ms, include_authorized_operations,
//                       partition_size_limit (-1: unset), by_ids) -> (job, err).
// Payload: {name|id: (error, description5)}.
static PyObject* py_Admin_describe_topics(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* names; long long timeout_ms; int include_ops, limit, by_ids;
    if (!PyArg_ParseTuple(args, "KOLpip", &h, &names, &timeout_ms, &include_ops, &limit, &by_ids)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_TopicCollection_t* topics; kafka_common_Error_t* err;
    if (topic_collection_build(names, by_ids, &topics, &err) < 0) return err ? job_or_error(NULL, err) : NULL;
    ADMIN_OPTIONS(DescribeTopics, o, timeout_ms);
    kafka_admin_DescribeTopicsOptions_set_include_authorized_operations(o, (int8_t)include_ops);
    if (limit >= 0) kafka_admin_DescribeTopicsOptions_set_partition_size_limit_per_response(o, (int32_t)limit);
    kafka_admin_DescribeTopicsResult_t* r = kafka_admin_Admin_describe_topics_with_topics_options(a->admin, topics, o);
    kafka_admin_DescribeTopicsOptions_destroy(o);
    kafka_common_TopicCollection_destroy(topics);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeTopics), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DescribeTopicsResult_destroy(r); return NULL; }
    if (by_ids) JOB_TRY(job, job_add_map(job, kafka_admin_DescribeTopicsResult_topic_id_values(r), key_uuid, conv_topic_description, 0));
    else JOB_TRY(job, job_add_map(job, kafka_admin_DescribeTopicsResult_topic_name_values(r), key_str, conv_topic_description, 0));
    return job_or_error(job, NULL);
}

// Admin_create_partitions(h, [(topic, total_count, [[broker]] | None)],
//                         timeout_ms, validate_only, retry_on_quota_violation)
// -> (job, err). Payload: {topic: error}. A None assignment is
// increaseTo(int); a list -- even an empty one -- is increaseTo(int, List).
static PyObject* py_Admin_create_partitions(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only, retry;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &spec, &timeout_ms, &validate_only, &retry)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "new_partitions must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // borrowed char* (owned by fast) -> owned NewPartitions*
    if (obj_map_init(&m, n, NULL, (handle_destroy_fn)kafka_admin_NewPartitions_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* topic; int total; PyObject* assignments;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siO", &topic, &total, &assignments)) goto fail;
        kafka_admin_NewPartitions_t* np;
        if (assignments == Py_None) {
            np = kafka_admin_NewPartitions_increase_to((int32_t)total);
        } else {
            PyObject* afast = PySequence_Fast(assignments, "new_assignments must be a sequence of broker lists");
            if (afast == NULL) goto fail;
            Py_ssize_t na = PySequence_Fast_GET_SIZE(afast);
            obj_list_t outer;
            if (obj_list_init(&outer, na, (handle_destroy_fn)i32_list_boxed_free) < 0) { Py_DECREF(afast); goto fail; }
            int ok = 1;
            for (Py_ssize_t j = 0; j < na; j++) {
                kafka_List_t* inner = i32_list_boxed(PySequence_Fast_GET_ITEM(afast, j), "broker ids must be a sequence of int");
                if (inner == NULL) { ok = 0; break; }
                obj_list_add(&outer, inner);
            }
            Py_DECREF(afast);
            np = ok ? kafka_admin_NewPartitions_increase_to_with_new_assignments((int32_t)total, outer.list) : NULL;
            obj_list_free(&outer);
            if (!ok) goto fail;
        }
        obj_map_put(&m, (void*)topic, np);
    }
    {
        ADMIN_OPTIONS(CreatePartitions, o, timeout_ms);
        kafka_admin_CreatePartitionsOptions_set_validate_only(o, (int8_t)validate_only);
        kafka_admin_CreatePartitionsOptions_set_retry_on_quota_violation(o, (int8_t)retry);
        kafka_admin_CreatePartitionsResult_t* r = kafka_admin_Admin_create_partitions_with_options(a->admin, m.map, o);
        kafka_admin_CreatePartitionsOptions_destroy(o);
        obj_map_free(&m);
        Py_DECREF(fast);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreatePartitions), assemble_keyed_errors);
        if (job == NULL) { kafka_admin_CreatePartitionsResult_destroy(r); return NULL; }
        JOB_TRY(job, job_add_map(job, kafka_admin_CreatePartitionsResult_values(r), key_str, NULL, 0));
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
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "records_to_delete must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy,
                     (handle_destroy_fn)kafka_admin_RecordsToDelete_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* t; int p; long long before;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siL", &t, &p, &before)) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
        obj_map_put(&m, kafka_common_TopicPartition_new(t, (int32_t)p), kafka_admin_RecordsToDelete_with_offset((int64_t)before));
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(DeleteRecords, o, timeout_ms);
    kafka_admin_DeleteRecordsResult_t* r = kafka_admin_Admin_delete_records_with_options(a->admin, m.map, o);
    kafka_admin_DeleteRecordsOptions_destroy(o);
    obj_map_free(&m);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteRecords), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DeleteRecordsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DeleteRecordsResult_low_watermarks(r), key_tp, conv_deleted_records, 0));
    return job_or_error(job, NULL);
}

// ---- cluster and configs --------------------------------------------------------------

// Admin_describe_cluster(h, timeout_ms, include_authorized_operations,
//                        include_fenced_brokers) -> (job, err).
// Payload: (error, (cluster_id, [node], controller, [op] | None)) -- Java's
// four independent futures; the first failure wins.
static PyObject* py_Admin_describe_cluster(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms; int include_ops, include_fenced;
    if (!PyArg_ParseTuple(args, "KLpp", &h, &timeout_ms, &include_ops, &include_fenced)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(DescribeCluster, o, timeout_ms);
    kafka_admin_DescribeClusterOptions_set_include_authorized_operations(o, (int8_t)include_ops);
    kafka_admin_DescribeClusterOptions_set_include_fenced_brokers(o, (int8_t)include_fenced);
    kafka_admin_DescribeClusterResult_t* r = kafka_admin_Admin_describe_cluster_with_options(a->admin, o);
    kafka_admin_DescribeClusterOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeCluster), assemble_multi);
    if (job == NULL) { kafka_admin_DescribeClusterResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_DescribeClusterResult_cluster_id(r), 1, conv_str, 0));
    JOB_TRY(job, job_add_single(job, kafka_admin_DescribeClusterResult_nodes(r), 1, conv_node_list, 1));
    JOB_TRY(job, job_add_single(job, kafka_admin_DescribeClusterResult_controller(r), 1, conv_node, 2));
    JOB_TRY(job, job_add_single(job, kafka_admin_DescribeClusterResult_authorized_operations(r), 1, conv_acl_operations, 3));
    return job_or_error(job, NULL);
}

// (type_id, name) rows -> owned ConfigResource handles. 0 / -1.
static int config_resource_list_build(PyObject* seq, obj_list_t* l) {
    PyObject* fast = PySequence_Fast(seq, "resources must be a sequence of (type, name)");
    if (fast == NULL) return -1;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    if (obj_list_init(l, n, (handle_destroy_fn)kafka_common_config_ConfigResource_destroy) < 0) { Py_DECREF(fast); return -1; }
    for (Py_ssize_t i = 0; i < n; i++) {
        int type; const char* name;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "is", &type, &name)) { Py_DECREF(fast); obj_list_free(l); return -1; }
        const kafka_common_config_ConfigResource_Type_t* rt = kafka_common_config_ConfigResource_Type_for_id((int8_t)type);
        if (rt == NULL) {
            PyErr_Format(PyExc_ValueError, "unknown ConfigResource type id %d", type);
            Py_DECREF(fast); obj_list_free(l); return -1;
        }
        obj_list_add(l, kafka_common_config_ConfigResource_new(rt, name));
    }
    Py_DECREF(fast);
    return 0;
}

// Admin_describe_configs(h, [(type, name)], timeout_ms, include_synonyms,
//                        include_documentation) -> (job, err).
// Payload: {(type, name): (error, [entry9])}.
static PyObject* py_Admin_describe_configs(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int synonyms, documentation;
    if (!PyArg_ParseTuple(args, "KOLpp", &h, &spec, &timeout_ms, &synonyms, &documentation)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    obj_list_t resources;
    if (config_resource_list_build(spec, &resources) < 0) return NULL;
    ADMIN_OPTIONS(DescribeConfigs, o, timeout_ms);
    kafka_admin_DescribeConfigsOptions_set_include_synonyms(o, (int8_t)synonyms);
    kafka_admin_DescribeConfigsOptions_set_include_documentation(o, (int8_t)documentation);
    kafka_admin_DescribeConfigsResult_t* r = kafka_admin_Admin_describe_configs_with_options(a->admin, resources.list, o);
    kafka_admin_DescribeConfigsOptions_destroy(o);
    obj_list_free(&resources);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeConfigs), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DescribeConfigsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DescribeConfigsResult_values(r), key_config_resource, conv_config_full, 0));
    return job_or_error(job, NULL);
}

static void alter_config_op_list_free(void* l) {
    kafka_List_t* list = (kafka_List_t*)l;
    int32_t n = kafka_List_size(list);
    for (int32_t i = 0; i < n; i++) kafka_admin_AlterConfigOp_destroy((kafka_admin_AlterConfigOp_t*)kafka_List_get(list, i));
    kafka_List_destroy(list);
}

// Admin_incremental_alter_configs(h, [(type, name, entry_name, value | None, op_type)],
//                                 timeout_ms, validate_only) -> (job, err).
// Payload: {(type, name): error}. Rows are one per operation, grouped by
// resource (consecutive rows with the same (type, name) share one
// ConfigResource key, as the Python side emits them).
static PyObject* py_Admin_incremental_alter_configs(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &validate_only)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "configs must be a sequence of rows");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // owned ConfigResource* -> owned kafka_List_t of owned AlterConfigOp*
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_config_ConfigResource_destroy, alter_config_op_list_free) < 0) {
        Py_DECREF(fast); return NULL;
    }
    int last_type = -1; const char* last_name = NULL; kafka_List_t* ops = NULL;
    for (Py_ssize_t i = 0; i < n; i++) {
        int type; const char *name, *entry_name, *value; int op;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "isszi", &type, &name, &entry_name, &value, &op)) goto fail;
        if (ops == NULL || type != last_type || strcmp(name, last_name) != 0) {
            const kafka_common_config_ConfigResource_Type_t* rt = kafka_common_config_ConfigResource_Type_for_id((int8_t)type);
            if (rt == NULL) { PyErr_Format(PyExc_ValueError, "unknown ConfigResource type id %d", type); goto fail; }
            ops = kafka_List_new();
            obj_map_put(&m, kafka_common_config_ConfigResource_new(rt, name), ops);
            last_type = type; last_name = name;
        }
        const kafka_admin_AlterConfigOp_OpType_t* op_type = kafka_admin_AlterConfigOp_OpType_for_id((int8_t)op);
        if (op_type == NULL) { PyErr_Format(PyExc_ValueError, "unknown AlterConfigOp type id %d", op); goto fail; }
        kafka_admin_ConfigEntry_t* entry = kafka_admin_ConfigEntry_new(entry_name, value);
        kafka_List_add(ops, kafka_admin_AlterConfigOp_new(entry, op_type));
        kafka_admin_ConfigEntry_destroy(entry);   // copied by AlterConfigOp_new
    }
    {
        ADMIN_OPTIONS(AlterConfigs, o, timeout_ms);
        kafka_admin_AlterConfigsOptions_set_validate_only(o, (int8_t)validate_only);
        kafka_admin_AlterConfigsResult_t* r = kafka_admin_Admin_incremental_alter_configs_with_options(a->admin, m.map, o);
        kafka_admin_AlterConfigsOptions_destroy(o);
        obj_map_free(&m);
        Py_DECREF(fast);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterConfigs), assemble_keyed_errors);
        if (job == NULL) { kafka_admin_AlterConfigsResult_destroy(r); return NULL; }
        JOB_TRY(job, job_add_map(job, kafka_admin_AlterConfigsResult_values(r), key_config_resource, NULL, 0));
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
    unsigned long long h; PyObject* types; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &types, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(types, "resource_types must be a sequence of int");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    kafka_List_t* list = kafka_List_new();   // borrowed Type singletons
    for (Py_ssize_t i = 0; i < n; i++) {
        long v = PyLong_AsLong(PySequence_Fast_GET_ITEM(fast, i));
        if (v == -1 && PyErr_Occurred()) { kafka_List_destroy(list); Py_DECREF(fast); return NULL; }
        const kafka_common_config_ConfigResource_Type_t* rt = kafka_common_config_ConfigResource_Type_for_id((int8_t)v);
        if (rt == NULL) {
            PyErr_Format(PyExc_ValueError, "unknown ConfigResource type id %ld", v);
            kafka_List_destroy(list); Py_DECREF(fast); return NULL;
        }
        kafka_List_add(list, (void*)rt);
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(ListConfigResources, o, timeout_ms);
    kafka_admin_ListConfigResourcesResult_t* r = kafka_admin_Admin_list_config_resources_with_options(a->admin, list, o);
    kafka_admin_ListConfigResourcesOptions_destroy(o);
    kafka_List_destroy(list);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListConfigResources), assemble_single);
    if (job == NULL) { kafka_admin_ListConfigResourcesResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_ListConfigResourcesResult_all(r), 1, conv_config_resources, 0));
    return job_or_error(job, NULL);
}

// ---- log dirs ------------------------------------------------------------------------

// Admin_describe_log_dirs(h, [broker_id], timeout_ms) -> (job, err).
// Payload: {broker_id: (error, {log_dir: description4})}.
static PyObject* py_Admin_describe_log_dirs(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* brokers; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &brokers, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    i32_list_t ids;
    if (i32_list_build(brokers, &ids, "brokers must be a sequence of int") < 0) return NULL;
    ADMIN_OPTIONS(DescribeLogDirs, o, timeout_ms);
    kafka_admin_DescribeLogDirsResult_t* r = kafka_admin_Admin_describe_log_dirs_with_options(a->admin, ids.list, o);
    kafka_admin_DescribeLogDirsOptions_destroy(o);
    i32_list_free(&ids);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeLogDirs), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DescribeLogDirsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DescribeLogDirsResult_descriptions(r), key_i32, conv_log_dir_descriptions, 0));
    return job_or_error(job, NULL);
}

// Admin_alter_replica_log_dirs(h, [(topic, partition, broker_id, log_dir)], timeout_ms)
// -> (job, err). Payload: {(topic, partition, broker_id): error}.
static PyObject* py_Admin_alter_replica_log_dirs(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tpr_list_t replicas; PyObject* fast = NULL; const char** dirs = NULL;
    if (tpr_list_build(spec, &replicas, 1, &fast, &dirs) < 0) return NULL;
    kafka_Map_t* m = kafka_Map_new();   // borrowed TPReplica* -> borrowed char* (owned by fast)
    for (Py_ssize_t i = 0; i < replicas.n; i++) kafka_Map_put(m, replicas.replicas[i], (void*)dirs[i]);
    ADMIN_OPTIONS(AlterReplicaLogDirs, o, timeout_ms);
    kafka_admin_AlterReplicaLogDirsResult_t* r = kafka_admin_Admin_alter_replica_log_dirs_with_options(a->admin, m, o);
    kafka_admin_AlterReplicaLogDirsOptions_destroy(o);
    kafka_Map_destroy(m);
    tpr_list_free(&replicas);
    PyMem_Free(dirs);
    Py_XDECREF(fast);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterReplicaLogDirs), assemble_keyed_errors);
    if (job == NULL) { kafka_admin_AlterReplicaLogDirsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_AlterReplicaLogDirsResult_values(r), key_tpr, NULL, 0));
    return job_or_error(job, NULL);
}

// Admin_describe_replica_log_dirs(h, [(topic, partition, broker_id)], timeout_ms)
// -> (job, err). Payload: {(topic, partition, broker_id): (error, info4)}.
static PyObject* py_Admin_describe_replica_log_dirs(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tpr_list_t replicas;
    if (tpr_list_build(spec, &replicas, 0, NULL, NULL) < 0) return NULL;
    ADMIN_OPTIONS(DescribeReplicaLogDirs, o, timeout_ms);
    kafka_admin_DescribeReplicaLogDirsResult_t* r = kafka_admin_Admin_describe_replica_log_dirs_with_options(a->admin, replicas.list, o);
    kafka_admin_DescribeReplicaLogDirsOptions_destroy(o);
    tpr_list_free(&replicas);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeReplicaLogDirs), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DescribeReplicaLogDirsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DescribeReplicaLogDirsResult_values(r), key_tpr, conv_replica_log_dir_info, 0));
    return job_or_error(job, NULL);
}

// ---- elections, reassignments, offsets --------------------------------------------------

// Admin_elect_leaders(h, election_type, all_partitions, [(topic, partition)], timeout_ms)
// -> (job, err). Payload: (error, {(topic, partition): error | None}).
// `all_partitions` is Java's null Set (every partition in the cluster).
static PyObject* py_Admin_elect_leaders(PyObject* self, PyObject* args) {
    unsigned long long h; int election_type, all_partitions; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KipOL", &h, &election_type, &all_partitions, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    const kafka_common_ElectionType_t* et = NULL;
    kafka_common_Error_t* err = kafka_common_ElectionType_value_of((int8_t)election_type, &et);
    if (err != NULL) return job_or_error(NULL, err);
    tp_list_t tps;
    memset(&tps, 0, sizeof(tps));
    if (!all_partitions && tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(ElectLeaders, o, timeout_ms);
    kafka_admin_ElectLeadersResult_t* r = kafka_admin_Admin_elect_leaders_with_options(
        a->admin, et, all_partitions ? NULL : tps.list, o);
    kafka_admin_ElectLeadersOptions_destroy(o);
    tp_list_free(&tps);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ElectLeaders), assemble_single);
    if (job == NULL) { kafka_admin_ElectLeadersResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_ElectLeadersResult_partitions(r), 1, conv_elect_leaders, 0));
    return job_or_error(job, NULL);
}

// Admin_alter_partition_reassignments(h, [(topic, partition, is_none, [replica])],
//                                     timeout_ms, allow_replication_factor_change)
// -> (job, err). Payload: {(topic, partition): error}. `is_none` is Java's
// empty Optional (cancel the reassignment), which crosses as a NULL value.
static PyObject* py_Admin_alter_partition_reassignments(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int allow_rf;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &allow_rf)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "reassignments must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy,
                     (handle_destroy_fn)kafka_admin_NewPartitionReassignment_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* t; int p, is_none; PyObject* replicas;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "sipO", &t, &p, &is_none, &replicas)) goto fail;
        kafka_admin_NewPartitionReassignment_t* npr = NULL;
        if (!is_none) {
            kafka_List_t* list = i32_list_boxed(replicas, "target_replicas must be a sequence of int");
            if (list == NULL) goto fail;
            kafka_common_Error_t* err = kafka_admin_NewPartitionReassignment_new(list, &npr);
            i32_list_boxed_free(list);
            if (err != NULL) {
                char prefix[192];
                snprintf(prefix, sizeof(prefix), "reassignment for %s-%d at index %zd", t, p, i);
                err = prefixed_error(prefix, err);
                obj_map_free(&m); Py_DECREF(fast);
                return job_or_error(NULL, err);
            }
        }
        obj_map_put(&m, kafka_common_TopicPartition_new(t, (int32_t)p), npr);
    }
    {
        ADMIN_OPTIONS(AlterPartitionReassignments, o, timeout_ms);
        kafka_admin_AlterPartitionReassignmentsOptions_set_allow_replication_factor_change(o, (int8_t)allow_rf);
        kafka_admin_AlterPartitionReassignmentsResult_t* r =
            kafka_admin_Admin_alter_partition_reassignments_with_options(a->admin, m.map, o);
        kafka_admin_AlterPartitionReassignmentsOptions_destroy(o);
        obj_map_free(&m);
        Py_DECREF(fast);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterPartitionReassignments), assemble_keyed_errors);
        if (job == NULL) { kafka_admin_AlterPartitionReassignmentsResult_destroy(r); return NULL; }
        JOB_TRY(job, job_add_map(job, kafka_admin_AlterPartitionReassignmentsResult_values(r), key_tp, NULL, 0));
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
    unsigned long long h; int all_partitions; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KpOL", &h, &all_partitions, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tp_list_t tps;
    memset(&tps, 0, sizeof(tps));
    if (!all_partitions && tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(ListPartitionReassignments, o, timeout_ms);
    kafka_admin_ListPartitionReassignmentsResult_t* r = all_partitions
        ? kafka_admin_Admin_list_partition_reassignments_with_options(a->admin, o)
        : kafka_admin_Admin_list_partition_reassignments_with_partitions_options(a->admin, tps.list, o);
    kafka_admin_ListPartitionReassignmentsOptions_destroy(o);
    tp_list_free(&tps);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListPartitionReassignments), assemble_single);
    if (job == NULL) { kafka_admin_ListPartitionReassignmentsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_ListPartitionReassignmentsResult_reassignments(r), 1, conv_partition_reassignments, 0));
    return job_or_error(job, NULL);
}

// The Python OffsetSpec sentinels (admin.OffsetSpec._LATEST ...), carried in
// the `value` column of a non-timestamp row.
enum {
    OFFSET_SPEC_LATEST = -1, OFFSET_SPEC_EARLIEST = -2, OFFSET_SPEC_MAX_TIMESTAMP = -3,
    OFFSET_SPEC_EARLIEST_LOCAL = -4, OFFSET_SPEC_LATEST_TIERED = -5, OFFSET_SPEC_EARLIEST_PENDING_UPLOAD = -6,
};

static void offset_spec_destroy_if_owned(void* spec) {
    // Only the timestamp spec is an owned handle; the others are singletons
    // the map stores as borrowed pointers, which must not be freed. The owned
    // ones are tracked separately (see py_Admin_list_offsets).
    (void)spec;
}

// Admin_list_offsets(h, [(topic, partition, is_timestamp, value)], timeout_ms,
//                    isolation_level) -> (job, err).
// Payload: {(topic, partition): (error, (offset, timestamp, leader_epoch))}.
static PyObject* py_Admin_list_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int isolation_level;
    if (!PyArg_ParseTuple(args, "KOLi", &h, &spec, &timeout_ms, &isolation_level)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    const kafka_common_IsolationLevel_t* il = NULL;
    kafka_common_Error_t* err = kafka_common_IsolationLevel_for_id((int8_t)isolation_level, &il);
    if (err != NULL) return job_or_error(NULL, err);
    PyObject* fast = fast_rows(spec, "topic_partition_offsets must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // owned TP* -> OffsetSpec* (singleton, or owned timestamp spec tracked in `owned`)
    obj_list_t owned;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy, offset_spec_destroy_if_owned) < 0) {
        Py_DECREF(fast); return NULL;
    }
    if (obj_list_init(&owned, n, (handle_destroy_fn)kafka_admin_OffsetSpec_destroy) < 0) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* t; int p, is_timestamp; long long value;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "sipL", &t, &p, &is_timestamp, &value)) goto fail;
        const kafka_admin_OffsetSpec_t* os;
        if (is_timestamp) {
            kafka_admin_OffsetSpec_t* ts = kafka_admin_OffsetSpec_timestamp((int64_t)value);
            obj_list_add(&owned, ts);
            os = ts;
        } else {
            switch (value) {
                case OFFSET_SPEC_LATEST: os = kafka_admin_OffsetSpec_latest(); break;
                case OFFSET_SPEC_EARLIEST: os = kafka_admin_OffsetSpec_earliest(); break;
                case OFFSET_SPEC_MAX_TIMESTAMP: os = kafka_admin_OffsetSpec_max_timestamp(); break;
                case OFFSET_SPEC_EARLIEST_LOCAL: os = kafka_admin_OffsetSpec_earliest_local(); break;
                case OFFSET_SPEC_LATEST_TIERED: os = kafka_admin_OffsetSpec_latest_tiered(); break;
                case OFFSET_SPEC_EARLIEST_PENDING_UPLOAD: os = kafka_admin_OffsetSpec_earliest_pending_upload(); break;
                default:
                    PyErr_Format(PyExc_ValueError, "unknown OffsetSpec sentinel %lld", value);
                    goto fail;
            }
        }
        obj_map_put(&m, kafka_common_TopicPartition_new(t, (int32_t)p), (void*)os);
    }
    {
        // ListOffsetsOptions(IsolationLevel): the owned constructor form.
        kafka_admin_ListOffsetsOptions_t* o = kafka_admin_ListOffsetsOptions_with_isolation_level(il);
        ADMIN_SET_TIMEOUT(ListOffsets, o, timeout_ms);
        kafka_admin_ListOffsetsResult_t* r = kafka_admin_Admin_list_offsets_with_options(a->admin, m.map, o);
        kafka_admin_ListOffsetsOptions_destroy(o);
        obj_list_free(&owned);
        Py_DECREF(fast);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListOffsets), assemble_keyed_pairs);
        if (job == NULL) { obj_map_free(&m); kafka_admin_ListOffsetsResult_destroy(r); return NULL; }
        // One owned per-partition future per requested partition, keyed by it.
        for (Py_ssize_t i = 0; i < m.n; i++) {
            const kafka_common_TopicPartition_t* tp = (const kafka_common_TopicPartition_t*)m.keys[i];
            kafka_common_KafkaFuture_t* f = NULL;
            kafka_common_Error_t* perr = kafka_admin_ListOffsetsResult_partition_result(r, tp, &f);
            if (perr != NULL) { obj_map_free(&m); job_free(job); return job_or_error(NULL, perr); }
            PyObject* key = tp_to_py(tp);
            if (key == NULL) { kafka_common_KafkaFuture_destroy(f); obj_map_free(&m); job_free(job); return NULL; }
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
    unsigned long long h; PyObject *states, *protocols, *types; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOOOL", &h, &states, &protocols, &types, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t s, pr, ty;
    if (str_list_build_msg(states, &s, "group_states must be a sequence of str") < 0) return NULL;
    if (str_list_build_msg(protocols, &pr, "protocol_types must be a sequence of str") < 0) { str_list_free(&s); return NULL; }
    if (str_list_build_msg(types, &ty, "types must be a sequence of str") < 0) { str_list_free(&s); str_list_free(&pr); return NULL; }
    kafka_admin_ListGroupsOptions_t* o = kafka_admin_ListGroupsOptions_new();
    ADMIN_SET_TIMEOUT(ListGroups, o, timeout_ms);
    int32_t n;
    if ((n = kafka_List_size(s.list)) > 0) {
        kafka_List_t* singletons = kafka_List_new();
        for (int32_t i = 0; i < n; i++) kafka_List_add(singletons, (void*)kafka_common_GroupState_parse((const char*)kafka_List_get(s.list, i)));
        kafka_admin_ListGroupsOptions_in_group_states(o, singletons);
        kafka_List_destroy(singletons);
    }
    if (kafka_List_size(pr.list) > 0) kafka_admin_ListGroupsOptions_with_protocol_types(o, pr.list);
    if ((n = kafka_List_size(ty.list)) > 0) {
        kafka_List_t* singletons = kafka_List_new();
        for (int32_t i = 0; i < n; i++) kafka_List_add(singletons, (void*)kafka_common_GroupType_parse((const char*)kafka_List_get(ty.list, i)));
        kafka_admin_ListGroupsOptions_with_types(o, singletons);
        kafka_List_destroy(singletons);
    }
    kafka_admin_ListGroupsResult_t* r = kafka_admin_Admin_list_groups_with_options(a->admin, o);
    kafka_admin_ListGroupsOptions_destroy(o);
    str_list_free(&s); str_list_free(&pr); str_list_free(&ty);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListGroups), assemble_multi);
    if (job == NULL) { kafka_admin_ListGroupsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_ListGroupsResult_valid(r), 1, conv_group_listings, 0));
    JOB_TRY(job, job_add_single(job, kafka_admin_ListGroupsResult_errors(r), 1, conv_error_list, 1));
    return job_or_error(job, NULL);
}

// Admin_describe_consumer_groups(h, [group_id], timeout_ms, include_authorized_operations)
// -> (job, err). Payload: {group_id: (error, description10)}.
static PyObject* py_Admin_describe_consumer_groups(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids; long long timeout_ms; int include_ops;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &ids, &timeout_ms, &include_ops)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "group_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeConsumerGroups, o, timeout_ms);
    kafka_admin_DescribeConsumerGroupsOptions_set_include_authorized_operations(o, (int8_t)include_ops);
    kafka_admin_DescribeConsumerGroupsResult_t* r = kafka_admin_Admin_describe_consumer_groups_with_options(a->admin, l.list, o);
    kafka_admin_DescribeConsumerGroupsOptions_destroy(o);
    str_list_free(&l);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeConsumerGroups), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DescribeConsumerGroupsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DescribeConsumerGroupsResult_described_groups(r), key_str, conv_consumer_group_description, 0));
    return job_or_error(job, NULL);
}

// Admin_describe_classic_groups(h, [group_id], timeout_ms, include_authorized_operations)
// -> (job, err). Payload: {group_id: (error, description8)}.
static PyObject* py_Admin_describe_classic_groups(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids; long long timeout_ms; int include_ops;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &ids, &timeout_ms, &include_ops)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "group_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeClassicGroups, o, timeout_ms);
    kafka_admin_DescribeClassicGroupsOptions_set_include_authorized_operations(o, (int8_t)include_ops);
    kafka_admin_DescribeClassicGroupsResult_t* r = kafka_admin_Admin_describe_classic_groups_with_options(a->admin, l.list, o);
    kafka_admin_DescribeClassicGroupsOptions_destroy(o);
    str_list_free(&l);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeClassicGroups), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DescribeClassicGroupsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DescribeClassicGroupsResult_described_groups(r), key_str, conv_classic_group_description, 0));
    return job_or_error(job, NULL);
}

// Admin_list_consumer_group_offsets(h, [(group_id, all_partitions, [(topic, partition)])],
//                                   timeout_ms, require_stable) -> (job, err).
// Payload: {group_id: (error, {(topic, partition): (offset, metadata, epoch) | None})}.
// `all_partitions` is Java's unset collection: every partition the group has
// committed offsets for.
static PyObject* py_Admin_list_consumer_group_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int require_stable;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &require_stable)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "group_specs must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;   // borrowed char* (owned by fast) -> owned ListConsumerGroupOffsetsSpec*
    if (obj_map_init(&m, n, NULL, (handle_destroy_fn)kafka_admin_ListConsumerGroupOffsetsSpec_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* gid; int all_partitions; PyObject* rows;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "spO", &gid, &all_partitions, &rows)) goto fail;
        kafka_admin_ListConsumerGroupOffsetsSpec_t* s = kafka_admin_ListConsumerGroupOffsetsSpec_new();
        obj_map_put(&m, (void*)gid, s);
        if (!all_partitions) {
            tp_list_t tps;
            if (tp_list_build(rows, &tps) < 0) goto fail;
            kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions(s, tps.list);   // copied
            tp_list_free(&tps);
        }
    }
    {
        ADMIN_OPTIONS(ListConsumerGroupOffsets, o, timeout_ms);
        kafka_admin_ListConsumerGroupOffsetsOptions_set_require_stable(o, (int8_t)require_stable);
        kafka_admin_ListConsumerGroupOffsetsResult_t* r =
            kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options(a->admin, m.map, o);
        kafka_admin_ListConsumerGroupOffsetsOptions_destroy(o);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListConsumerGroupOffsets), assemble_keyed_pairs);
        if (job == NULL) { obj_map_free(&m); Py_DECREF(fast); kafka_admin_ListConsumerGroupOffsetsResult_destroy(r); return NULL; }
        // One owned future per requested group id, keyed by it.
        for (Py_ssize_t i = 0; i < m.n; i++) {
            const char* gid = (const char*)m.keys[i];
            kafka_common_KafkaFuture_t* f = NULL;
            kafka_common_Error_t* perr =
                kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id(r, gid, &f);
            if (perr != NULL) { obj_map_free(&m); Py_DECREF(fast); job_free(job); return job_or_error(NULL, perr); }
            PyObject* key = key_str(gid);
            if (key == NULL) { kafka_common_KafkaFuture_destroy(f); obj_map_free(&m); Py_DECREF(fast); job_free(job); return NULL; }
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
//     timeout_ms) -> (job, err). Payload: {(topic, partition): error}.
static PyObject* py_Admin_alter_consumer_group_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; const char* gid; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KsOL", &h, &gid, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "offsets must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;
    if (obj_map_init(&m, n, (handle_destroy_fn)kafka_common_TopicPartition_destroy,
                     (handle_destroy_fn)kafka_consumer_OffsetAndMetadata_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char *t, *metadata; int p, has_epoch, epoch; long long offset;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siLzpi", &t, &p, &offset, &metadata, &has_epoch, &epoch)) {
            obj_map_free(&m); Py_DECREF(fast); return NULL;
        }
        kafka_consumer_OffsetAndMetadata_t* om = NULL;
        kafka_common_Error_t* err = has_epoch
            ? kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata((int64_t)offset, (int32_t)epoch, metadata, &om)
            : kafka_consumer_OffsetAndMetadata_with_metadata((int64_t)offset, metadata, &om);
        if (err != NULL) {
            obj_map_free(&m); Py_DECREF(fast);
            return job_or_error(NULL, indexed_error("offset", i, err));
        }
        obj_map_put(&m, kafka_common_TopicPartition_new(t, (int32_t)p), om);
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(AlterConsumerGroupOffsets, o, timeout_ms);
    kafka_admin_AlterConsumerGroupOffsetsResult_t* r =
        kafka_admin_Admin_alter_consumer_group_offsets_with_options(a->admin, gid, m.map, o);
    kafka_admin_AlterConsumerGroupOffsetsOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterConsumerGroupOffsets), assemble_keyed_errors);
    if (job == NULL) { obj_map_free(&m); kafka_admin_AlterConsumerGroupOffsetsResult_destroy(r); return NULL; }
    for (Py_ssize_t i = 0; i < m.n; i++) {
        const kafka_common_TopicPartition_t* tp = (const kafka_common_TopicPartition_t*)m.keys[i];
        kafka_common_KafkaFuture_t* f = kafka_admin_AlterConsumerGroupOffsetsResult_partition_result(r, tp);
        PyObject* key = tp_to_py(tp);
        if (key == NULL) { if (f) kafka_common_KafkaFuture_destroy(f); obj_map_free(&m); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, NULL, NULL, 0) == NULL) { obj_map_free(&m); job_free(job); return NULL; }
    }
    obj_map_free(&m);
    return job_or_error(job, NULL);
}

// Admin_delete_consumer_group_offsets(h, group_id, [(topic, partition)], timeout_ms)
// -> (job, err). Payload: {(topic, partition): error}.
static PyObject* py_Admin_delete_consumer_group_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; const char* gid; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KsOL", &h, &gid, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tp_list_t tps;
    if (tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(DeleteConsumerGroupOffsets, o, timeout_ms);
    kafka_admin_DeleteConsumerGroupOffsetsResult_t* r =
        kafka_admin_Admin_delete_consumer_group_offsets_with_options(a->admin, gid, tps.list, o);
    kafka_admin_DeleteConsumerGroupOffsetsOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteConsumerGroupOffsets), assemble_keyed_errors);
    if (job == NULL) { tp_list_free(&tps); kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(r); return NULL; }
    for (Py_ssize_t i = 0; i < tps.n; i++) {
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result(r, tps.tps[i], &f);
        if (perr != NULL) { tp_list_free(&tps); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = tp_to_py(tps.tps[i]);
        if (key == NULL) { kafka_common_KafkaFuture_destroy(f); tp_list_free(&tps); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, NULL, NULL, 0) == NULL) { tp_list_free(&tps); job_free(job); return NULL; }
    }
    tp_list_free(&tps);
    return job_or_error(job, NULL);
}

// Admin_delete_consumer_groups(h, [group_id], timeout_ms) -> (job, err).
// Payload: {group_id: error}.
static PyObject* py_Admin_delete_consumer_groups(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &ids, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "group_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DeleteConsumerGroups, o, timeout_ms);
    kafka_admin_DeleteConsumerGroupsResult_t* r = kafka_admin_Admin_delete_consumer_groups_with_options(a->admin, l.list, o);
    kafka_admin_DeleteConsumerGroupsOptions_destroy(o);
    str_list_free(&l);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteConsumerGroups), assemble_keyed_errors);
    if (job == NULL) { kafka_admin_DeleteConsumerGroupsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DeleteConsumerGroupsResult_deleted_groups(r), key_str, NULL, 0));
    return job_or_error(job, NULL);
}

// Admin_remove_members_from_consumer_group(h, group_id, remove_all, [group_instance_id],
//                                          reason | None, timeout_ms) -> (job, err).
// Payload: {group_instance_id: error} for an explicit member list, or
// (error, None) -- Java's all() -- in remove-all mode (Java's no-argument
// options constructor), where memberResult() is not applicable.
static PyObject* py_Admin_remove_members_from_consumer_group(PyObject* self, PyObject* args) {
    unsigned long long h; const char *gid, *reason; int remove_all; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KspOzL", &h, &gid, &remove_all, &ids, &reason, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    obj_list_t members;
    if (remove_all) {
        if (obj_list_init(&members, 0, NULL) < 0) return NULL;
    } else {
        str_list_t l;
        if (str_list_build_msg(ids, &l, "members must be a sequence of group instance ids") < 0) return NULL;
        int32_t n = kafka_List_size(l.list);
        if (obj_list_init(&members, n, (handle_destroy_fn)kafka_admin_MemberToRemove_destroy) < 0) { str_list_free(&l); return NULL; }
        for (int32_t i = 0; i < n; i++) obj_list_add(&members, kafka_admin_MemberToRemove_new((const char*)kafka_List_get(l.list, i)));
        str_list_free(&l);
    }
    kafka_admin_RemoveMembersFromConsumerGroupOptions_t* o = NULL;
    if (remove_all) {
        o = kafka_admin_RemoveMembersFromConsumerGroupOptions_new();
    } else {
        kafka_common_Error_t* err = kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members(members.list, &o);
        if (err != NULL) { obj_list_free(&members); return job_or_error(NULL, err); }
    }
    ADMIN_SET_TIMEOUT(RemoveMembersFromConsumerGroup, o, timeout_ms);
    if (reason != NULL) kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason(o, reason);
    kafka_admin_RemoveMembersFromConsumerGroupResult_t* r =
        kafka_admin_Admin_remove_members_from_consumer_group_with_options(a->admin, gid, o);
    kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(RemoveMembersFromConsumerGroup),
                            remove_all ? assemble_single : assemble_keyed_errors);
    if (job == NULL) { obj_list_free(&members); kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(r); return NULL; }
    if (remove_all) {
        obj_list_free(&members);
        JOB_TRY(job, job_add_single(job, kafka_admin_RemoveMembersFromConsumerGroupResult_all(r), 1, NULL, 0));
        return job_or_error(job, NULL);
    }
    for (Py_ssize_t i = 0; i < members.n; i++) {
        const kafka_admin_MemberToRemove_t* member = (const kafka_admin_MemberToRemove_t*)members.items[i];
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = kafka_admin_RemoveMembersFromConsumerGroupResult_member_result(r, member, &f);
        if (perr != NULL) { obj_list_free(&members); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = key_str(kafka_admin_MemberToRemove_group_instance_id(member));
        if (key == NULL) { kafka_common_KafkaFuture_destroy(f); obj_list_free(&members); job_free(job); return NULL; }
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
    const char *name, *principal, *host; int rtype, ptype, op, perm;
    *err_out = NULL;
    if (!PyArg_ParseTuple(row, "isissii", &rtype, &name, &ptype, &principal, &host, &op, &perm)) return NULL;
    kafka_common_resource_ResourcePattern_t* pattern = NULL;
    kafka_common_Error_t* err = kafka_common_resource_ResourcePattern_new(
        kafka_common_resource_ResourceType_from_code((int8_t)rtype), name,
        kafka_common_resource_PatternType_from_code((int8_t)ptype), &pattern);
    if (err != NULL) { *err_out = indexed_error("acl", index, err); return NULL; }
    kafka_common_acl_AccessControlEntry_t* entry = NULL;
    err = kafka_common_acl_AccessControlEntry_new(
        principal, host, kafka_common_acl_AclOperation_from_code((int8_t)op),
        kafka_common_acl_AclPermissionType_from_code((int8_t)perm), &entry);
    if (err != NULL) {
        kafka_common_resource_ResourcePattern_destroy(pattern);
        *err_out = indexed_error("acl", index, err);
        return NULL;
    }
    kafka_common_acl_AclBinding_t* binding = kafka_common_acl_AclBinding_new(pattern, entry);  // copies both
    kafka_common_resource_ResourcePattern_destroy(pattern);
    kafka_common_acl_AccessControlEntry_destroy(entry);
    return binding;
}

// Builds one AclBindingFilter from a (resource_type, name | None, pattern_type,
// principal | None, host | None, operation, permission) row.
static kafka_common_acl_AclBindingFilter_t* acl_binding_filter_build(PyObject* row) {
    const char *name, *principal, *host; int rtype, ptype, op, perm;
    if (!PyArg_ParseTuple(row, "izizzii", &rtype, &name, &ptype, &principal, &host, &op, &perm)) return NULL;
    kafka_common_resource_ResourcePatternFilter_t* pattern = kafka_common_resource_ResourcePatternFilter_new(
        kafka_common_resource_ResourceType_from_code((int8_t)rtype), name,
        kafka_common_resource_PatternType_from_code((int8_t)ptype));
    kafka_common_acl_AccessControlEntryFilter_t* entry = kafka_common_acl_AccessControlEntryFilter_new(
        principal, host, kafka_common_acl_AclOperation_from_code((int8_t)op),
        kafka_common_acl_AclPermissionType_from_code((int8_t)perm));
    kafka_common_acl_AclBindingFilter_t* filter = kafka_common_acl_AclBindingFilter_new(pattern, entry);  // copies both
    kafka_common_resource_ResourcePatternFilter_destroy(pattern);
    kafka_common_acl_AccessControlEntryFilter_destroy(entry);
    return filter;
}

// Admin_create_acls(h, [binding7], timeout_ms) -> (job, err).
// Payload: {binding7: error}.
static PyObject* py_Admin_create_acls(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "acls must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_acl_AclBinding_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        kafka_common_Error_t* err = NULL;
        kafka_common_acl_AclBinding_t* b = acl_binding_build(PySequence_Fast_GET_ITEM(fast, i), i, &err);
        if (b == NULL) {
            obj_list_free(&l); Py_DECREF(fast);
            return err ? job_or_error(NULL, err) : NULL;
        }
        obj_list_add(&l, b);
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(CreateAcls, o, timeout_ms);
    kafka_admin_CreateAclsResult_t* r = kafka_admin_Admin_create_acls_with_options(a->admin, l.list, o);
    kafka_admin_CreateAclsOptions_destroy(o);
    obj_list_free(&l);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreateAcls), assemble_keyed_errors);
    if (job == NULL) { kafka_admin_CreateAclsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_CreateAclsResult_values(r), key_acl_binding, NULL, 0));
    return job_or_error(job, NULL);
}

// Admin_describe_acls(h, filter7, timeout_ms) -> (job, err).
// Payload: (error, [binding7]).
static PyObject* py_Admin_describe_acls(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* row; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &row, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_acl_AclBindingFilter_t* filter = acl_binding_filter_build(row);
    if (filter == NULL) return NULL;
    ADMIN_OPTIONS(DescribeAcls, o, timeout_ms);
    kafka_admin_DescribeAclsResult_t* r = kafka_admin_Admin_describe_acls_with_options(a->admin, filter, o);
    kafka_admin_DescribeAclsOptions_destroy(o);
    kafka_common_acl_AclBindingFilter_destroy(filter);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeAcls), assemble_single);
    if (job == NULL) { kafka_admin_DescribeAclsResult_destroy(r); return NULL; }
    // `values()` is BORROWED from the result: destroyed with it, never alone.
    JOB_TRY(job, job_add_single(job, (kafka_common_KafkaFuture_t*)kafka_admin_DescribeAclsResult_values(r), 0, conv_acl_bindings, 0));
    return job_or_error(job, NULL);
}

// Admin_delete_acls(h, [filter7], timeout_ms) -> (job, err).
// Payload: {filter7: (error, [(error, binding7)])}.
static PyObject* py_Admin_delete_acls(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "filters must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_acl_AclBindingFilter_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        kafka_common_acl_AclBindingFilter_t* f = acl_binding_filter_build(PySequence_Fast_GET_ITEM(fast, i));
        if (f == NULL) { obj_list_free(&l); Py_DECREF(fast); return NULL; }
        obj_list_add(&l, f);
    }
    Py_DECREF(fast);
    ADMIN_OPTIONS(DeleteAcls, o, timeout_ms);
    kafka_admin_DeleteAclsResult_t* r = kafka_admin_Admin_delete_acls_with_options(a->admin, l.list, o);
    kafka_admin_DeleteAclsOptions_destroy(o);
    obj_list_free(&l);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DeleteAcls), assemble_keyed_pairs);
    if (job == NULL) { kafka_admin_DeleteAclsResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_DeleteAclsResult_values(r), key_acl_binding_filter, conv_filter_results, 0));
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
    unsigned long long h; PyObject* spec; int strict; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOpL", &h, &spec, &strict, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "components must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_quota_ClientQuotaFilterComponent_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char *type, *name; int match;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siz", &type, &match, &name)) { obj_list_free(&l); Py_DECREF(fast); return NULL; }
        kafka_common_quota_ClientQuotaFilterComponent_t* c;
        switch (match) {
            case QUOTA_MATCH_EXACT:
                if (name == NULL) {
                    PyErr_SetString(PyExc_ValueError, "an exact-match quota filter component needs a name");
                    obj_list_free(&l); Py_DECREF(fast); return NULL;
                }
                c = kafka_common_quota_ClientQuotaFilterComponent_of_entity(type, name);
                break;
            case QUOTA_MATCH_DEFAULT: c = kafka_common_quota_ClientQuotaFilterComponent_of_default_entity(type); break;
            case QUOTA_MATCH_ANY: c = kafka_common_quota_ClientQuotaFilterComponent_of_entity_type(type); break;
            default:
                PyErr_Format(PyExc_ValueError, "unknown quota filter match type %d", match);
                obj_list_free(&l); Py_DECREF(fast); return NULL;
        }
        obj_list_add(&l, c);
    }
    Py_DECREF(fast);
    // ClientQuotaFilter.contains / containsOnly copy the components; `all()`
    // is containsOnly of nothing, so the explicit forms cover it.
    kafka_common_quota_ClientQuotaFilter_t* filter = strict
        ? kafka_common_quota_ClientQuotaFilter_contains_only(l.list)
        : kafka_common_quota_ClientQuotaFilter_contains(l.list);
    obj_list_free(&l);
    ADMIN_OPTIONS(DescribeClientQuotas, o, timeout_ms);
    kafka_admin_DescribeClientQuotasResult_t* r = kafka_admin_Admin_describe_client_quotas_with_options(a->admin, filter, o);
    kafka_admin_DescribeClientQuotasOptions_destroy(o);
    kafka_common_quota_ClientQuotaFilter_destroy(filter);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeClientQuotas), assemble_single);
    if (job == NULL) { kafka_admin_DescribeClientQuotasResult_destroy(r); return NULL; }
    // `entities()` is BORROWED from the result.
    JOB_TRY(job, job_add_single(job, (kafka_common_KafkaFuture_t*)kafka_admin_DescribeClientQuotasResult_entities(r), 0, conv_quota_entities, 0));
    return job_or_error(job, NULL);
}

// Admin_alter_client_quotas(h, [([(entity_type, name | None)], [(key, value | None)])],
//                           timeout_ms, validate_only) -> (job, err).
// Payload: {entity_pairs: error}. A None op value is a removal (Java's null
// Double), which crosses as NaN.
static PyObject* py_Admin_alter_client_quotas(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &validate_only)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "entries must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_common_quota_ClientQuotaAlteration_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *pairs, *ops;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "OO", &pairs, &ops)) goto fail;
        PyObject* fpairs = fast_rows(pairs, "entity entries must be a sequence");
        if (fpairs == NULL) goto fail;
        // Borrowed char* -> char* | NULL, owned by `fpairs` for the call.
        kafka_Map_t* entries = kafka_Map_new();
        Py_ssize_t np = PySequence_Fast_GET_SIZE(fpairs);
        for (Py_ssize_t j = 0; j < np; j++) {
            const char *type, *name;
            if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fpairs, j), "sz", &type, &name)) {
                kafka_Map_destroy(entries); Py_DECREF(fpairs); goto fail;
            }
            kafka_Map_put(entries, (void*)type, (void*)name);
        }
        kafka_common_quota_ClientQuotaEntity_t* entity = kafka_common_quota_ClientQuotaEntity_new(entries);  // copies
        kafka_Map_destroy(entries);
        Py_DECREF(fpairs);
        PyObject* fops = fast_rows(ops, "ops must be a sequence");
        if (fops == NULL) { kafka_common_quota_ClientQuotaEntity_destroy(entity); goto fail; }
        Py_ssize_t no = PySequence_Fast_GET_SIZE(fops);
        obj_list_t opl;
        if (obj_list_init(&opl, no, (handle_destroy_fn)kafka_common_quota_ClientQuotaAlteration_Op_destroy) < 0) {
            Py_DECREF(fops); kafka_common_quota_ClientQuotaEntity_destroy(entity); goto fail;
        }
        for (Py_ssize_t j = 0; j < no; j++) {
            const char* key; PyObject* value;
            if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fops, j), "sO", &key, &value)) {
                obj_list_free(&opl); Py_DECREF(fops); kafka_common_quota_ClientQuotaEntity_destroy(entity); goto fail;
            }
            double d = NAN;
            if (value != Py_None) {
                d = PyFloat_AsDouble(value);
                if (d == -1.0 && PyErr_Occurred()) {
                    obj_list_free(&opl); Py_DECREF(fops); kafka_common_quota_ClientQuotaEntity_destroy(entity); goto fail;
                }
            }
            obj_list_add(&opl, kafka_common_quota_ClientQuotaAlteration_Op_new(key, d));
        }
        Py_DECREF(fops);
        kafka_common_quota_ClientQuotaAlteration_t* alteration =
            kafka_common_quota_ClientQuotaAlteration_new(entity, opl.list);  // copies entity and ops
        obj_list_free(&opl);
        kafka_common_quota_ClientQuotaEntity_destroy(entity);
        obj_list_add(&l, alteration);
    }
    Py_DECREF(fast);
    {
        ADMIN_OPTIONS(AlterClientQuotas, o, timeout_ms);
        kafka_admin_AlterClientQuotasOptions_set_validate_only(o, (int8_t)validate_only);
        kafka_admin_AlterClientQuotasResult_t* r = kafka_admin_Admin_alter_client_quotas_with_options(a->admin, l.list, o);
        kafka_admin_AlterClientQuotasOptions_destroy(o);
        obj_list_free(&l);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterClientQuotas), assemble_keyed_errors);
        if (job == NULL) { kafka_admin_AlterClientQuotasResult_destroy(r); return NULL; }
        JOB_TRY(job, job_add_map(job, kafka_admin_AlterClientQuotasResult_values(r), key_quota_entity, NULL, 0));
        return job_or_error(job, NULL);
    }
fail:
    obj_list_free(&l);
    Py_DECREF(fast);
    return NULL;
}

// ---- SCRAM --------------------------------------------------------------------------

// Second stage of describeUserScramCredentials: once `users()` delivered, one
// owned `description(user)` future per user, keyed by it (tag 1).
static int cont_scram_users(AdminJob* job, AdminSlot* slot) {
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
        kafka_common_KafkaFuture_t* f = kafka_admin_DescribeUserScramCredentialsResult_description(r, name);
        Py_INCREF(user);
        if (job_add_slot(job, f, 1, user, conv_scram_description, NULL, 1) == NULL) return -1;
    }
    return 0;
}

// (users_error | None, {user: (error, [(mechanism, iterations)])})
static PyObject* assemble_scram(AdminJob* job) {
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

// Admin_describe_user_scram_credentials(h, [user], timeout_ms) -> (job, err).
// An empty list is Java's describe-all. Payload: see assemble_scram.
static PyObject* py_Admin_describe_user_scram_credentials(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* users; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &users, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(users, &l, "users must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeUserScramCredentials, o, timeout_ms);
    kafka_admin_DescribeUserScramCredentialsResult_t* r =
        kafka_admin_Admin_describe_user_scram_credentials_with_users_options(a->admin, l.list, o);
    kafka_admin_DescribeUserScramCredentialsOptions_destroy(o);
    str_list_free(&l);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeUserScramCredentials), assemble_scram);
    if (job == NULL) { kafka_admin_DescribeUserScramCredentialsResult_destroy(r); return NULL; }
    if (job_add_slot(job, kafka_admin_DescribeUserScramCredentialsResult_users(r), 1, NULL, conv_str_list, cont_scram_users, 0) == NULL) {
        job_free(job); return NULL;
    }
    return job_or_error(job, NULL);
}

// Admin_alter_user_scram_credentials(h,
//     [(user, is_deletion, mechanism_type, iterations, password | None, salt | None)],
//     timeout_ms) -> (job, err). Payload: {user: error}.
static PyObject* py_Admin_alter_user_scram_credentials(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &spec, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "alterations must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_list_t l;
    if (obj_list_init(&l, n, (handle_destroy_fn)kafka_admin_UserScramCredentialAlteration_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* user; int is_deletion, mech, iterations; PyObject *password, *salt;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "spiiOO", &user, &is_deletion, &mech, &iterations, &password, &salt)) goto fail;
        const kafka_admin_ScramMechanism_t* mechanism = kafka_admin_ScramMechanism_from_type((int8_t)mech);
        if (mechanism == NULL) {
            PyErr_Format(PyExc_ValueError, "unknown SCRAM mechanism type %d", mech);
            goto fail;
        }
        kafka_admin_UserScramCredentialAlteration_t* alteration;
        if (is_deletion) {
            kafka_admin_UserScramCredentialDeletion_t* del = kafka_admin_UserScramCredentialDeletion_new(user, mechanism);
            alteration = kafka_admin_UserScramCredentialAlteration_deletion(del);  // copies
            kafka_admin_UserScramCredentialDeletion_destroy(del);
        } else {
            Py_buffer pw, sa;
            if (PyObject_GetBuffer(password, &pw, PyBUF_SIMPLE) < 0) goto fail;
            kafka_Bytes_t pw_bytes = { (const uint8_t*)pw.buf, (int32_t)pw.len };
            kafka_admin_ScramCredentialInfo_t* info = kafka_admin_ScramCredentialInfo_new(mechanism, (int32_t)iterations);
            kafka_admin_UserScramCredentialUpsertion_t* up;
            if (salt == Py_None) {
                // Java's three-argument constructor: a salt is generated.
                up = kafka_admin_UserScramCredentialUpsertion_with_bytes(user, info, pw_bytes);
            } else {
                if (PyObject_GetBuffer(salt, &sa, PyBUF_SIMPLE) < 0) {
                    PyBuffer_Release(&pw); kafka_admin_ScramCredentialInfo_destroy(info); goto fail;
                }
                kafka_Bytes_t salt_bytes = { (const uint8_t*)sa.buf, (int32_t)sa.len };
                up = kafka_admin_UserScramCredentialUpsertion_with_salt(user, info, pw_bytes, salt_bytes);
                PyBuffer_Release(&sa);
            }
            PyBuffer_Release(&pw);
            kafka_admin_ScramCredentialInfo_destroy(info);
            alteration = kafka_admin_UserScramCredentialAlteration_upsertion(up);  // copies
            kafka_admin_UserScramCredentialUpsertion_destroy(up);
        }
        obj_list_add(&l, alteration);
    }
    Py_DECREF(fast);
    {
        ADMIN_OPTIONS(AlterUserScramCredentials, o, timeout_ms);
        kafka_admin_AlterUserScramCredentialsResult_t* r =
            kafka_admin_Admin_alter_user_scram_credentials_with_options(a->admin, l.list, o);
        kafka_admin_AlterUserScramCredentialsOptions_destroy(o);
        obj_list_free(&l);
        AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AlterUserScramCredentials), assemble_keyed_errors);
        if (job == NULL) { kafka_admin_AlterUserScramCredentialsResult_destroy(r); return NULL; }
        JOB_TRY(job, job_add_map(job, kafka_admin_AlterUserScramCredentialsResult_values(r), key_str, NULL, 0));
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
    unsigned long long h; PyObject* renewers; const char *owner_type, *owner_name; long long max_lifetime_ms, timeout_ms;
    if (!PyArg_ParseTuple(args, "KOzzLL", &h, &renewers, &owner_type, &owner_name, &max_lifetime_ms, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    principal_list_t l;
    if (principal_list_build(renewers, &l) < 0) return NULL;
    ADMIN_OPTIONS(CreateDelegationToken, o, timeout_ms);
    kafka_admin_CreateDelegationTokenOptions_set_renewers(o, l.list);  // copied
    principal_list_free(&l);
    if (owner_type != NULL && owner_name != NULL) {
        kafka_common_security_auth_KafkaPrincipal_t* owner = kafka_common_security_auth_KafkaPrincipal_new(owner_type, owner_name);
        kafka_admin_CreateDelegationTokenOptions_set_owner(o, owner);  // copied
        kafka_common_security_auth_KafkaPrincipal_destroy(owner);
    }
    kafka_admin_CreateDelegationTokenOptions_set_max_lifetime_ms(o, (int64_t)max_lifetime_ms);
    kafka_admin_CreateDelegationTokenResult_t* r = kafka_admin_Admin_create_delegation_token_with_options(a->admin, o);
    kafka_admin_CreateDelegationTokenOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(CreateDelegationToken), assemble_single);
    if (job == NULL) { kafka_admin_CreateDelegationTokenResult_destroy(r); return NULL; }
    // `delegation_token()` is BORROWED from the result.
    JOB_TRY(job, job_add_single(job, (kafka_common_KafkaFuture_t*)kafka_admin_CreateDelegationTokenResult_delegation_token(r), 0, conv_delegation_token, 0));
    return job_or_error(job, NULL);
}

// Admin_renew_delegation_token(h, hmac, renew_time_period_ms, timeout_ms) -> (job, err).
// Payload: (error, expiry_timestamp).
static PyObject* py_Admin_renew_delegation_token(PyObject* self, PyObject* args) {
    unsigned long long h; const uint8_t* hmac; Py_ssize_t hmac_len; long long period_ms, timeout_ms;
    if (!PyArg_ParseTuple(args, "Ky#LL", &h, &hmac, &hmac_len, &period_ms, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_Bytes_t bytes = { hmac, (int32_t)hmac_len };
    ADMIN_OPTIONS(RenewDelegationToken, o, timeout_ms);
    kafka_admin_RenewDelegationTokenOptions_set_renew_time_period_ms(o, (int64_t)period_ms);
    kafka_admin_RenewDelegationTokenResult_t* r = kafka_admin_Admin_renew_delegation_token_with_options(a->admin, bytes, o);
    kafka_admin_RenewDelegationTokenOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(RenewDelegationToken), assemble_single);
    if (job == NULL) { kafka_admin_RenewDelegationTokenResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, (kafka_common_KafkaFuture_t*)kafka_admin_RenewDelegationTokenResult_expiry_timestamp(r), 0, conv_i64_ptr, 0));
    return job_or_error(job, NULL);
}

// Admin_expire_delegation_token(h, hmac, expiry_time_period_ms, timeout_ms) -> (job, err).
// Payload: (error, expiry_timestamp).
static PyObject* py_Admin_expire_delegation_token(PyObject* self, PyObject* args) {
    unsigned long long h; const uint8_t* hmac; Py_ssize_t hmac_len; long long period_ms, timeout_ms;
    if (!PyArg_ParseTuple(args, "Ky#LL", &h, &hmac, &hmac_len, &period_ms, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_Bytes_t bytes = { hmac, (int32_t)hmac_len };
    ADMIN_OPTIONS(ExpireDelegationToken, o, timeout_ms);
    kafka_admin_ExpireDelegationTokenOptions_set_expiry_time_period_ms(o, (int64_t)period_ms);
    kafka_admin_ExpireDelegationTokenResult_t* r = kafka_admin_Admin_expire_delegation_token_with_options(a->admin, bytes, o);
    kafka_admin_ExpireDelegationTokenOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ExpireDelegationToken), assemble_single);
    if (job == NULL) { kafka_admin_ExpireDelegationTokenResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, (kafka_common_KafkaFuture_t*)kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(r), 0, conv_i64_ptr, 0));
    return job_or_error(job, NULL);
}

// Admin_describe_delegation_token(h, has_owners, [(type, name)], timeout_ms) -> (job, err).
// `has_owners` false is Java's null owners (every token). Payload: (error, [token9]).
static PyObject* py_Admin_describe_delegation_token(PyObject* self, PyObject* args) {
    unsigned long long h; int has_owners; PyObject* owners; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KpOL", &h, &has_owners, &owners, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(DescribeDelegationToken, o, timeout_ms);
    if (has_owners) {
        principal_list_t l;
        if (principal_list_build(owners, &l) < 0) { kafka_admin_DescribeDelegationTokenOptions_destroy(o); return NULL; }
        kafka_admin_DescribeDelegationTokenOptions_set_owners(o, l.list);  // copied
        principal_list_free(&l);
    }
    kafka_admin_DescribeDelegationTokenResult_t* r = kafka_admin_Admin_describe_delegation_token_with_options(a->admin, o);
    kafka_admin_DescribeDelegationTokenOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeDelegationToken), assemble_single);
    if (job == NULL) { kafka_admin_DescribeDelegationTokenResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, (kafka_common_KafkaFuture_t*)kafka_admin_DescribeDelegationTokenResult_delegation_tokens(r), 0, conv_delegation_tokens, 0));
    return job_or_error(job, NULL);
}

// ---- features -------------------------------------------------------------------------

// Admin_describe_features(h, has_node_id, node_id, timeout_ms) -> (job, err).
// Payload: (error, ([(feature, min, max)], epoch | None, [(feature, min, max)])).
static PyObject* py_Admin_describe_features(PyObject* self, PyObject* args) {
    unsigned long long h; int has_node_id, node_id; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KpiL", &h, &has_node_id, &node_id, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(DescribeFeatures, o, timeout_ms);
    if (has_node_id) kafka_admin_DescribeFeaturesOptions_set_node_id(o, (int32_t)node_id);
    kafka_admin_DescribeFeaturesResult_t* r = kafka_admin_Admin_describe_features_with_options(a->admin, o);
    kafka_admin_DescribeFeaturesOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeFeatures), assemble_single);
    if (job == NULL) { kafka_admin_DescribeFeaturesResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_DescribeFeaturesResult_feature_metadata(r), 1, conv_feature_metadata, 0));
    return job_or_error(job, NULL);
}

// Admin_update_features(h, [(feature, max_version_level, upgrade_type_code)],
//                       timeout_ms, validate_only) -> (job, err).
// Payload: {feature: error}. Java's updateFeatures validates its argument up
// front (empty map, empty feature name), which surfaces as the call error.
static PyObject* py_Admin_update_features(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; long long timeout_ms; int validate_only;
    if (!PyArg_ParseTuple(args, "KOLp", &h, &spec, &timeout_ms, &validate_only)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    PyObject* fast = fast_rows(spec, "feature_updates must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    obj_map_t m;  // borrowed char* (owned by fast) -> owned FeatureUpdate*
    if (obj_map_init(&m, n, NULL, (handle_destroy_fn)kafka_admin_FeatureUpdate_destroy) < 0) { Py_DECREF(fast); return NULL; }
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* feature; int level, upgrade;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "sii", &feature, &level, &upgrade)) { obj_map_free(&m); Py_DECREF(fast); return NULL; }
        kafka_admin_FeatureUpdate_t* fu = NULL;
        kafka_common_Error_t* err = kafka_admin_FeatureUpdate_new(
            (int16_t)level, kafka_admin_FeatureUpdate_UpgradeType_from_code((int32_t)upgrade), &fu);
        if (err != NULL) { obj_map_free(&m); Py_DECREF(fast); return job_or_error(NULL, indexed_error("feature update", i, err)); }
        obj_map_put(&m, (void*)feature, fu);
    }
    ADMIN_OPTIONS(UpdateFeatures, o, timeout_ms);
    kafka_admin_UpdateFeaturesOptions_set_validate_only(o, (int8_t)validate_only);
    kafka_admin_UpdateFeaturesResult_t* r = NULL;
    kafka_common_Error_t* err = kafka_admin_Admin_update_features_with_options(a->admin, m.map, o, &r);
    kafka_admin_UpdateFeaturesOptions_destroy(o);
    obj_map_free(&m);
    Py_DECREF(fast);
    if (err != NULL) return job_or_error(NULL, err);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(UpdateFeatures), assemble_keyed_errors);
    if (job == NULL) { kafka_admin_UpdateFeaturesResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_UpdateFeaturesResult_values(r), key_str, NULL, 0));
    return job_or_error(job, NULL);
}

// ---- producers and transactions -----------------------------------------------------------

// Admin_describe_producers(h, [(topic, partition)], has_broker_id, broker_id, timeout_ms)
// -> (job, err). Payload: {(topic, partition): (error, [state6])}.
static PyObject* py_Admin_describe_producers(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int has_broker_id, broker_id; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOpiL", &h, &spec, &has_broker_id, &broker_id, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    tp_list_t tps;
    if (tp_list_build(spec, &tps) < 0) return NULL;
    ADMIN_OPTIONS(DescribeProducers, o, timeout_ms);
    if (has_broker_id) kafka_admin_DescribeProducersOptions_set_broker_id(o, (int32_t)broker_id);
    kafka_admin_DescribeProducersResult_t* r = kafka_admin_Admin_describe_producers_with_options(a->admin, tps.list, o);
    kafka_admin_DescribeProducersOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeProducers), assemble_keyed_pairs);
    if (job == NULL) { tp_list_free(&tps); kafka_admin_DescribeProducersResult_destroy(r); return NULL; }
    for (Py_ssize_t i = 0; i < tps.n; i++) {
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = kafka_admin_DescribeProducersResult_partition_result(r, tps.tps[i], &f);
        if (perr != NULL) { tp_list_free(&tps); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = tp_to_py(tps.tps[i]);
        if (key == NULL) { kafka_common_KafkaFuture_destroy(f); tp_list_free(&tps); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, conv_partition_producer_state, NULL, 0) == NULL) { tp_list_free(&tps); job_free(job); return NULL; }
    }
    tp_list_free(&tps);
    return job_or_error(job, NULL);
}

// Admin_describe_transactions(h, [transactional_id], timeout_ms) -> (job, err).
// Payload: {transactional_id: (error, description7)}.
static PyObject* py_Admin_describe_transactions(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &ids, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "transactional_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(DescribeTransactions, o, timeout_ms);
    kafka_admin_DescribeTransactionsResult_t* r = kafka_admin_Admin_describe_transactions_with_options(a->admin, l.list, o);
    kafka_admin_DescribeTransactionsOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(DescribeTransactions), assemble_keyed_pairs);
    if (job == NULL) { str_list_free(&l); kafka_admin_DescribeTransactionsResult_destroy(r); return NULL; }
    int32_t n = kafka_List_size(l.list);
    for (int32_t i = 0; i < n; i++) {
        const char* tid = (const char*)kafka_List_get(l.list, i);
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = kafka_admin_DescribeTransactionsResult_description(r, tid, &f);
        if (perr != NULL) { str_list_free(&l); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = key_str(tid);
        if (key == NULL) { kafka_common_KafkaFuture_destroy(f); str_list_free(&l); job_free(job); return NULL; }
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
    unsigned long long h; PyObject* ids; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KOL", &h, &ids, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t l;
    if (str_list_build_msg(ids, &l, "transactional_ids must be a sequence of str") < 0) return NULL;
    ADMIN_OPTIONS(FenceProducers, o, timeout_ms);
    kafka_admin_FenceProducersResult_t* r = kafka_admin_Admin_fence_producers_with_options(a->admin, l.list, o);
    kafka_admin_FenceProducersOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(FenceProducers), assemble_fence_producers);
    if (job == NULL) { str_list_free(&l); kafka_admin_FenceProducersResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_map(job, kafka_admin_FenceProducersResult_fenced_producers(r), key_str, NULL, FP_FENCED));
    int32_t n = kafka_List_size(l.list);
    for (int32_t i = 0; i < n; i++) {
        const char* tid = (const char*)kafka_List_get(l.list, i);
        kafka_common_KafkaFuture_t* f = NULL;
        kafka_common_Error_t* perr = kafka_admin_FenceProducersResult_producer_id(r, tid, &f);
        if (perr != NULL) { str_list_free(&l); job_free(job); return job_or_error(NULL, perr); }
        PyObject* key = key_str(tid);
        if (key == NULL) { kafka_common_KafkaFuture_destroy(f); str_list_free(&l); job_free(job); return NULL; }
        if (job_add_slot(job, f, 1, key, conv_i64_ptr, NULL, FP_PRODUCER_ID) == NULL) { str_list_free(&l); job_free(job); return NULL; }
        f = NULL;
        perr = kafka_admin_FenceProducersResult_epoch_id(r, tid, &f);
        if (perr != NULL) { str_list_free(&l); job_free(job); return job_or_error(NULL, perr); }
        key = key_str(tid);
        if (key == NULL) { kafka_common_KafkaFuture_destroy(f); str_list_free(&l); job_free(job); return NULL; }
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
    if (slot->error != NULL) return 0;  // byBrokerId() failed: no per-broker futures
    void* value = NULL;
    kafka_common_Error_t* err = kafka_common_KafkaFuture_get(slot->future, &value);  // completed: immediate
    if (err != NULL) { kafka_common_Error_destroy(err); return 0; }
    const kafka_Map_t* map = (const kafka_Map_t*)value;
    int32_t n = map ? kafka_Map_size(map) : 0;
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = key_i32(kafka_Map_key(map, i));
        if (key == NULL) return -1;
        kafka_common_KafkaFuture_t* f = (kafka_common_KafkaFuture_t*)kafka_Map_value(map, i);
        if (job_add_slot(job, f, 0, key, conv_transaction_listings, NULL, 1) == NULL) return -1;
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
    unsigned long long h; PyObject *states, *pids; long long duration_ms, timeout_ms; const char* pattern;
    if (!PyArg_ParseTuple(args, "KOOLzL", &h, &states, &pids, &duration_ms, &pattern, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    str_list_t s;
    if (str_list_build_msg(states, &s, "states must be a sequence of str") < 0) return NULL;
    i64_list_t p;
    if (i64_list_build(pids, &p, "producer_ids must be a sequence of int") < 0) { str_list_free(&s); return NULL; }
    ADMIN_OPTIONS(ListTransactions, o, timeout_ms);
    int32_t n = kafka_List_size(s.list);
    if (n > 0) {
        kafka_List_t* singletons = kafka_List_new();
        for (int32_t i = 0; i < n; i++) {
            const kafka_admin_TransactionState_t* st = kafka_admin_TransactionState_parse((const char*)kafka_List_get(s.list, i));
            if (st == NULL) {
                PyErr_Format(PyExc_ValueError, "unknown transaction state %s", (const char*)kafka_List_get(s.list, i));
                kafka_List_destroy(singletons); kafka_admin_ListTransactionsOptions_destroy(o);
                str_list_free(&s); i64_list_free(&p); return NULL;
            }
            kafka_List_add(singletons, (void*)st);
        }
        kafka_admin_ListTransactionsOptions_filter_states(o, singletons);
        kafka_List_destroy(singletons);
    }
    if (kafka_List_size(p.list) > 0) kafka_admin_ListTransactionsOptions_filter_producer_ids(o, p.list);
    kafka_admin_ListTransactionsOptions_filter_on_duration(o, (int64_t)duration_ms);
    if (pattern != NULL) kafka_admin_ListTransactionsOptions_filter_on_transactional_id_pattern(o, pattern);
    str_list_free(&s); i64_list_free(&p);
    kafka_admin_ListTransactionsResult_t* r = kafka_admin_Admin_list_transactions_with_options(a->admin, o);
    kafka_admin_ListTransactionsOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(ListTransactions), assemble_list_transactions);
    if (job == NULL) { kafka_admin_ListTransactionsResult_destroy(r); return NULL; }
    if (job_add_slot(job, kafka_admin_ListTransactionsResult_by_broker_id(r), 1, NULL, NULL, cont_list_transactions, 0) == NULL) {
        job_free(job); return NULL;
    }
    return job_or_error(job, NULL);
}

// Admin_abort_transaction(h, topic, partition, producer_id, producer_epoch,
//                         coordinator_epoch, timeout_ms) -> (job, err).
// Payload: (error, None).
static PyObject* py_Admin_abort_transaction(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition, coordinator_epoch, producer_epoch; long long producer_id, timeout_ms;
    if (!PyArg_ParseTuple(args, "KsiLiiL", &h, &topic, &partition, &producer_id, &producer_epoch, &coordinator_epoch, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    kafka_common_TopicPartition_t* tp = kafka_common_TopicPartition_new(topic, (int32_t)partition);
    kafka_admin_AbortTransactionSpec_t* spec = kafka_admin_AbortTransactionSpec_new(
        tp, (int64_t)producer_id, (int16_t)producer_epoch, (int32_t)coordinator_epoch);  // copies tp
    kafka_common_TopicPartition_destroy(tp);
    ADMIN_OPTIONS(AbortTransaction, o, timeout_ms);
    kafka_admin_AbortTransactionResult_t* r = kafka_admin_Admin_abort_transaction_with_options(a->admin, spec, o);
    kafka_admin_AbortTransactionOptions_destroy(o);
    kafka_admin_AbortTransactionSpec_destroy(spec);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(AbortTransaction), assemble_single);
    if (job == NULL) { kafka_admin_AbortTransactionResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_AbortTransactionResult_all(r), 1, NULL, 0));
    return job_or_error(job, NULL);
}

// Admin_force_terminate_transaction(h, transactional_id, timeout_ms) -> (job, err).
// Payload: (error, None).
static PyObject* py_Admin_force_terminate_transaction(PyObject* self, PyObject* args) {
    unsigned long long h; const char* tid; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KsL", &h, &tid, &timeout_ms)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    ADMIN_OPTIONS(TerminateTransaction, o, timeout_ms);
    kafka_admin_TerminateTransactionResult_t* r = kafka_admin_Admin_force_terminate_transaction_with_options(a->admin, tid, o);
    kafka_admin_TerminateTransactionOptions_destroy(o);
    AdminJob* job = job_new(r, JOB_RESULT_DESTROY(TerminateTransaction), assemble_single);
    if (job == NULL) { kafka_admin_TerminateTransactionResult_destroy(r); return NULL; }
    JOB_TRY(job, job_add_single(job, kafka_admin_TerminateTransactionResult_result(r), 1, NULL, 0));
    return job_or_error(job, NULL);
}

// ---- MockAdminClient drivers ---------------------------------------------------------------
//
// Each returns an owned kafka_common_Error_t* as an int (0 = ok); a handle that
// is not a mock gets an illegal-argument error, as the setters only exist on
// the concrete MockAdminClient.

static PyObject* mock_only_error(void) {
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)
        kafka_common_Error_local_illegal_argument("this operation is only supported on a MockAdminClient"));
}

// MockAdminClient_timeout_next_request(h, n) -> error
static PyObject* py_MockAdminClient_timeout_next_request(PyObject* self, PyObject* args) {
    unsigned long long h; int n;
    if (!PyArg_ParseTuple(args, "Ki", &h, &n)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    if (a->mock == NULL) return mock_only_error();
    kafka_admin_MockAdminClient_timeout_next_request(a->mock, (int32_t)n);
    return PyLong_FromLong(0);
}

typedef void (*mock_offsets_fn)(const kafka_admin_MockAdminClient_t*, const kafka_Map_t*);

static PyObject* mock_update_offsets(PyObject* args, mock_offsets_fn fn) {
    unsigned long long h; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    if (a->mock == NULL) return mock_only_error();
    tp_i64_map_t m;
    if (tp_i64_map_build(spec, &m) < 0) return NULL;
    fn(a->mock, m.map);  // copied
    tp_i64_map_free(&m);
    return PyLong_FromLong(0);
}

// MockAdminClient_update_beginning_offsets(h, [(topic, partition, offset)]) -> error
static PyObject* py_MockAdminClient_update_beginning_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_beginning_offsets);
}

// MockAdminClient_update_end_offsets(h, [(topic, partition, offset)]) -> error
static PyObject* py_MockAdminClient_update_end_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_end_offsets);
}

// MockAdminClient_update_consumer_group_offsets(h, [(topic, partition, offset)]) -> error
static PyObject* py_MockAdminClient_update_consumer_group_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_consumer_group_offsets);
}

// MockAdminClient_set_feature_levels(h, [(feature, level, min, max)]) -> error.
// Three borrowed char* -> int16_t* maps, replacing what was seeded before.
static PyObject* py_MockAdminClient_set_feature_levels(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;
    AdminHandle* a = admin_from_handle(h);
    if (a->mock == NULL) return mock_only_error();
    PyObject* fast = fast_rows(spec, "feature levels must be a sequence");
    if (fast == NULL) return NULL;
    Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
    int16_t* vals = n > 0 ? PyMem_Calloc((size_t)n * 3, sizeof(int16_t)) : NULL;
    if (n > 0 && vals == NULL) { Py_DECREF(fast); PyErr_NoMemory(); return NULL; }
    kafka_Map_t* levels = kafka_Map_new();
    kafka_Map_t* mins = kafka_Map_new();
    kafka_Map_t* maxs = kafka_Map_new();
    for (Py_ssize_t i = 0; i < n; i++) {
        const char* feature; int level, lo, hi;
        if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(fast, i), "siii", &feature, &level, &lo, &hi)) {
            kafka_Map_destroy(levels); kafka_Map_destroy(mins); kafka_Map_destroy(maxs);
            PyMem_Free(vals); Py_DECREF(fast); return NULL;
        }
        vals[3 * i] = (int16_t)level; vals[3 * i + 1] = (int16_t)lo; vals[3 * i + 2] = (int16_t)hi;
        kafka_Map_put(levels, (void*)feature, &vals[3 * i]);
        kafka_Map_put(mins, (void*)feature, &vals[3 * i + 1]);
        kafka_Map_put(maxs, (void*)feature, &vals[3 * i + 2]);
    }
    kafka_admin_MockAdminClient_set_feature_levels(a->mock, levels, mins, maxs);  // copied
    kafka_Map_destroy(levels); kafka_Map_destroy(mins); kafka_Map_destroy(maxs);
    PyMem_Free(vals);
    Py_DECREF(fast);
    return PyLong_FromLong(0);
}

static PyMethodDef ProducerNativeMethods[] = {
    {"MockProducer_new", py_MockProducer_new, METH_VARARGS,
     "MockProducer_new(auto_complete, notify_cb=None) -> handle"},
    {"KafkaProducer_new", py_KafkaProducer_new, METH_VARARGS,
     "KafkaProducer_new(config, notify_cb=None) -> handle; RuntimeError on a rejected config"},
    {"Producer_send", py_Producer_send, METH_VARARGS,
     "Blocking send(record, on_completion | None) -> (KafkaFuture | None, error | None)"},
    {"Producer_send_cb", py_Producer_send_cb, METH_VARARGS,
     "Queued send(record, on_completion | None, resolve); resolve(KafkaFuture | None, error | None)"},
    {"Producer_poll", py_Producer_poll, METH_VARARGS,
     "poll(timeout_ms): run queued callbacks on this thread, waiting up to timeout_ms; returns count"},
    {"Producer_execute_callbacks", py_Producer_execute_callbacks, METH_VARARGS,
     "Run the queued callbacks on this thread; returns count"},
    {"Producer_flush", py_Producer_flush, METH_VARARGS, "Blocking flush -> error | None"},
    {"Producer_flush_cb", py_Producer_flush_cb, METH_VARARGS, "Queued flush; cb(error | None)"},
    {"Producer_partitions_for", py_Producer_partitions_for, METH_VARARGS,
     "Blocking partitions_for(topic) -> (list | None, error | None)"},
    {"Producer_partitions_for_cb", py_Producer_partitions_for_cb, METH_VARARGS,
     "Queued partitions_for(topic, cb); cb(list | None, error | None)"},
    {"Producer_metrics", py_Producer_metrics, METH_VARARGS,
     "Point-in-time metrics snapshot; returns list[dict]"},
    {"Producer_init_transactions", py_Producer_init_transactions, METH_VARARGS,
     "Blocking initTransactions -> error | None"},
    {"Producer_init_transactions_cb", py_Producer_init_transactions_cb, METH_VARARGS,
     "Queued initTransactions; cb(error | None)"},
    {"Producer_begin_transaction", py_Producer_begin_transaction, METH_VARARGS,
     "beginTransaction -> error | None (no _cb twin: a state transition in Java)"},
    {"Producer_send_offsets_to_transaction", py_Producer_send_offsets_to_transaction, METH_VARARGS,
     "Blocking sendOffsetsToTransaction(spec, group_metadata) -> error | None"},
    {"Producer_send_offsets_to_transaction_cb", py_Producer_send_offsets_to_transaction_cb, METH_VARARGS,
     "Queued sendOffsetsToTransaction(spec, group_metadata, cb); cb(error | None)"},
    {"Producer_commit_transaction", py_Producer_commit_transaction, METH_VARARGS,
     "Blocking commitTransaction -> error | None"},
    {"Producer_commit_transaction_cb", py_Producer_commit_transaction_cb, METH_VARARGS,
     "Queued commitTransaction; cb(error | None)"},
    {"Producer_abort_transaction", py_Producer_abort_transaction, METH_VARARGS,
     "Blocking abortTransaction -> error | None"},
    {"Producer_abort_transaction_cb", py_Producer_abort_transaction_cb, METH_VARARGS,
     "Queued abortTransaction; cb(error | None)"},
    {"Producer_close", py_Producer_close, METH_VARARGS, "Blocking close -> error | None"},
    {"Producer_close_cb", py_Producer_close_cb, METH_VARARGS, "Queued close; cb(error | None)"},
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
    // ---- Consumer ----
    //
    // Every blocking operation `X(h, ...)` has a `X_cb(h, ..., cb)` twin that
    // queues its completion for Consumer_execute_callbacks; the callable then
    // receives the same payload the blocking form returns.
    {"Consumer_MockConsumer_new", py_Consumer_MockConsumer_new, METH_VARARGS, "Create a MockConsumer; returns a handle"},
    {"Consumer_KafkaConsumer_new", py_Consumer_KafkaConsumer_new, METH_VARARGS, "Create a KafkaConsumer from a config dict; returns a handle"},
    {"Consumer_destroy", py_Consumer_destroy, METH_VARARGS, "Destroy a consumer handle (runs the still pending callbacks)"},
    {"Consumer_wakeup", py_Consumer_wakeup, METH_VARARGS, "Wake up a blocked operation"},
    {"Consumer_execute_callbacks", py_Consumer_execute_callbacks, METH_VARARGS, "Run the queued callbacks on this thread; returns how many ran"},
    {"Consumer_set_callbacks_notify", py_Consumer_set_callbacks_notify, METH_VARARGS, "Register the callable fired when callbacks become pending"},
    {"Consumer_set_callback_result", py_Consumer_set_callback_result, METH_VARARGS, "Report a deferred listener result: (h, callback_id, message | None)"},
    {"Consumer_assignment", py_Consumer_assignment, METH_VARARGS, "Current assignment as list[(topic, partition)]"},
    {"Consumer_subscription", py_Consumer_subscription, METH_VARARGS, "Current subscription as list[str]"},
    {"Consumer_paused", py_Consumer_paused, METH_VARARGS, "Paused partitions as list[(topic, partition)]"},
    {"Consumer_metrics", py_Consumer_metrics, METH_VARARGS, "Metrics snapshot as list[dict] with name/group/description/tags/value"},
    {"Consumer_group_metadata", py_Consumer_group_metadata, METH_VARARGS, "ConsumerGroupMetadata, or None while an operation is in flight"},
    {"Consumer_client_id", py_Consumer_client_id, METH_VARARGS, "Client id string"},
    {"Consumer_current_lag", py_Consumer_current_lag, METH_VARARGS, "Current lag int or None"},
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
    {"Consumer_close", py_Consumer_close, METH_VARARGS, "close(h, timeout_ms) -> err | None (negative timeout: Java's close())"},
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
    {"Admin_set_callbacks_notify", py_Admin_set_callbacks_notify, METH_VARARGS, "Admin_set_callbacks_notify(handle, cb | None): cb() fires when the callback queue goes non-empty (schedule only)"},
    {"Admin_execute_callbacks", py_Admin_execute_callbacks, METH_VARARGS, "Admin_execute_callbacks(handle) -> number of pending completion callbacks run on this thread"},
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
    {"Admin_force_terminate_transaction", py_Admin_force_terminate_transaction, METH_VARARGS, "Admin_force_terminate_transaction(handle, ...) -> (job, error)"},
    {"MockAdminClient_timeout_next_request", py_MockAdminClient_timeout_next_request, METH_VARARGS, "Mock-only seeding; returns an owned error handle (0 = ok)"},
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
