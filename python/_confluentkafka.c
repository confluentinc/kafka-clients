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
// The C layer runs NO background threads of its own (plan decision D8). The
// sync producer calls the blocking entry points (`send_with_callback`, `flush`,
// `close`, the transaction-control ops) directly with the GIL released; the
// asyncio producer drives their `_cb` twins. Delivery completions and `_cb`
// completions are QUEUED by Rust on the producer's callback vector and only
// run when this extension calls `kafka_producer_Producer_execute_callbacks`
// (Producer_poll / Producer_execute_callbacks), i.e. on the calling thread --
// `poll(timeout)` / `flush()` / `close()` for the sync producer, the event loop
// for the asyncio one.
//
// The hook installed with `kafka_producer_Producer_set_callbacks_notify` fires
// from a Rust task once per empty->non-empty transition of that vector. It
// only signals: it sets `notified` under `mtx` and broadcasts `cnd` (what the
// sync `poll(timeout)` waits on) and, for the asyncio producer, invokes a
// Python callable that does nothing but `loop.call_soon_threadsafe(pump)`. It
// never calls back into the C API.
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

static PyMethodDef KafkaFuture_methods[] = {
    {"get", (PyCFunction)KafkaFuture_get, METH_VARARGS,
     "get(timeout_ms=None) -> (metadata_tuple | None, error_tuple | None); None on timeout"},
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

// list[str] -> char* array (used by the admin section). The char* point into
// the Python str objects held by the caller's argument list, which stays alive
// for the whole FFI call. Returns count (>=0), or -1 on error (Python error
// set). On success the caller must PyMem_Free(*out).
static Py_ssize_t topics_to_array(PyObject* list, const char*** out) {
    Py_ssize_t n = PySequence_Size(list);
    if (n < 0) return -1;
    const char** arr = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    if (n > 0 && arr == NULL) { PyErr_NoMemory(); return -1; }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(list, i);  // new ref
        const char* s = item ? PyUnicode_AsUTF8(item) : NULL;
        Py_XDECREF(item);
        if (s == NULL) { PyMem_Free(arr); PyErr_SetString(PyExc_TypeError, "topics must be str"); return -1; }
        arr[i] = s;
    }
    *out = arr;
    return n;
}

// Admin-section completion trampoline: cb(handle_int, error_int), one-shot.
static void fire_handle_cb(void* handle, kafka_common_Error_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "KK",
        (unsigned long long)(uintptr_t)handle,
        (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
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
// Marshaling only: the Python `admin` module owns all orchestration. Async
// submits hold the GIL (the submit is non-blocking); the callback fires later on
// the Rust dispatcher thread and hands back opaque handle ints, which Python
// then converts with the matching *_drain function.
// ===========================================================================

// ---- trampolines -----------------------------------------------------------

// void-op callback (close): (error, user_data) -> py_cb(error_int)
static void admin_op_trampoline(kafka_common_Error_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "K", (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

static void admin_create_topics_trampoline(kafka_admin_CreateTopicsResult_t* r,
                                          kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_topics_trampoline(kafka_admin_DeleteTopicsResult_t* r,
                                          kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_topics_trampoline(kafka_admin_ListTopicsResult_t* r,
                                        kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_topics_trampoline(kafka_admin_DescribeTopicsResult_t* r,
                                            kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_create_partitions_trampoline(kafka_admin_CreatePartitionsResult_t* r,
                                              kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_records_trampoline(kafka_admin_DeleteRecordsResult_t* r,
                                           kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }

// ---- constructors / lifecycle ----------------------------------------------

static PyObject* py_Admin_MockAdminClient_new(PyObject* self, PyObject* args) {
    int num_brokers;
    if (!PyArg_ParseTuple(args, "i", &num_brokers)) return NULL;
    kafka_admin_AdminClient_t* a = kafka_admin_MockAdminClient_new(num_brokers);
    if (a == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create MockAdminClient");
        return NULL;
    }
    return PyLong_FromVoidPtr(a);
}

static PyObject* py_Admin_AdminClient_new(PyObject* self, PyObject* args) {
    PyObject* config_dict;
    if (!PyArg_ParseTuple(args, "O", &config_dict)) return NULL;
    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }
    kafka_admin_AdminClientProperties_t* props = kafka_admin_AdminClientProperties_new();
    if (props == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create AdminClientProperties");
        return NULL;
    }
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_AsUTF8(key);
        const char* v = PyUnicode_AsUTF8(value);
        if (k == NULL || v == NULL) {
            kafka_admin_AdminClientProperties_destroy(props);
            PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
        kafka_admin_AdminClientProperties_put(props, k, v);
    }
    kafka_common_Error_t* err = NULL;
    kafka_admin_AdminClient_t* a = kafka_admin_AdminClient_new(props, &err);
    kafka_admin_AdminClientProperties_destroy(props);
    if (a == NULL) {
        const char* msg = err ? kafka_common_Error_message(err) : NULL;
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create AdminClient");
        if (err) kafka_common_Error_destroy(err);
        return NULL;
    }
    return PyLong_FromVoidPtr(a);
}

static PyObject* py_Admin_destroy(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_admin_AdminClient_t* a = (kafka_admin_AdminClient_t*)(uintptr_t)h;
    Py_BEGIN_ALLOW_THREADS
    kafka_admin_AdminClient_destroy(a);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* py_Admin_close_async(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &timeout_ms, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_close_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
                                        timeout_ms, admin_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_MockAdminClient_timeout_next_request(PyObject* self, PyObject* args) {
    unsigned long long h; int n;
    if (!PyArg_ParseTuple(args, "Ki", &h, &n)) return NULL;
    kafka_common_Error_t* e = kafka_admin_MockAdminClient_timeout_next_request(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, n);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// Mock: seed beginning/end offsets. `spec` is a sequence of
// (topic:str, partition:int, offset:int); returns the error handle as an int.
static PyObject* mock_update_offsets(PyObject* args,
                                     kafka_common_Error_t* (*update)(
                                         const kafka_admin_AdminClient_t*,
                                         const char* const*, const int32_t*, const int64_t*,
                                         int32_t)) {
    unsigned long long h; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** topics = PyMem_Malloc(slots * sizeof(char*));
    int32_t* partitions = PyMem_Malloc(slots * sizeof(int32_t));
    int64_t* offsets = PyMem_Malloc(slots * sizeof(int64_t));
    if (topics == NULL || partitions == NULL || offsets == NULL) {
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* topic = NULL; int p = 0; long long off = 0;
        int ok = item && PyArg_ParseTuple(item, "siL", &topic, &p, &off);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
            return NULL;
        }
        topics[i] = topic; partitions[i] = (int32_t)p; offsets[i] = (int64_t)off;
    }
    kafka_common_Error_t* e = update((kafka_admin_AdminClient_t*)(uintptr_t)h, topics,
                                          partitions, offsets, (int32_t)n);
    PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_MockAdminClient_update_beginning_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_beginning_offsets);
}

static PyObject* py_MockAdminClient_update_end_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_end_offsets);
}

static PyObject* py_MockAdminClient_update_consumer_group_offsets(PyObject* self, PyObject* args) {
    return mock_update_offsets(args, kafka_admin_MockAdminClient_update_consumer_group_offsets);
}

// ---- createTopics ----------------------------------------------------------

// Frees `count` NewTopic handles.
static void free_new_topics(kafka_admin_NewTopic_t** topics, Py_ssize_t count) {
    for (Py_ssize_t i = 0; i < count; i++) {
        kafka_admin_NewTopic_destroy(topics[i]);
    }
    PyMem_Free(topics);
}

// Applies `configs` (a sequence of (name, value) pairs) to a NewTopic handle.
// Returns 0 on success, -1 with a Python exception set on failure.
static int apply_new_topic_configs(kafka_admin_NewTopic_t* topic, PyObject* configs) {
    if (configs == Py_None) return 0;
    Py_ssize_t n = PySequence_Size(configs);
    if (n < 0) return -1;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(configs, i);  // new ref
        const char* k = NULL; const char* v = NULL;
        int ok = item && PyArg_ParseTuple(item, "ss", &k, &v);
        if (ok) kafka_admin_NewTopic_put_config(topic, k, v);
        Py_XDECREF(item);
        if (!ok) return -1;
    }
    return 0;
}

// Applies `assignments` (a sequence of (partition, [broker_ids]) pairs).
// Returns 0 on success, -1 with a Python exception set on failure.
static int apply_new_topic_assignments(kafka_admin_NewTopic_t* topic, PyObject* assignments) {
    if (assignments == Py_None) return 0;
    Py_ssize_t n = PySequence_Size(assignments);
    if (n < 0) return -1;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(assignments, i);  // new ref
        int partition = 0; PyObject* brokers = NULL;
        if (!item || !PyArg_ParseTuple(item, "iO", &partition, &brokers)) {
            Py_XDECREF(item);
            return -1;
        }
        Py_ssize_t bn = PySequence_Size(brokers);
        if (bn < 0) { Py_DECREF(item); return -1; }
        int32_t* ids = bn > 0 ? PyMem_Malloc((size_t)bn * sizeof(int32_t)) : NULL;
        if (bn > 0 && ids == NULL) { Py_DECREF(item); PyErr_NoMemory(); return -1; }
        int ok = 1;
        for (Py_ssize_t j = 0; j < bn; j++) {
            PyObject* b = PySequence_GetItem(brokers, j);  // new ref
            if (b == NULL) { ok = 0; break; }
            long id = PyLong_AsLong(b);
            Py_DECREF(b);
            if (id == -1 && PyErr_Occurred()) { ok = 0; break; }
            ids[j] = (int32_t)id;
        }
        if (ok) kafka_admin_NewTopic_set_replicas_assignment(topic, partition, ids, (int32_t)bn);
        PyMem_Free(ids);
        Py_DECREF(item);
        if (!ok) return -1;
    }
    return 0;
}

// Builds `count` NewTopic handles from a sequence of
// (name, num_partitions, replication_factor, configs, assignments) tuples.
// Returns the array (caller frees with free_new_topics) or NULL on failure.
static kafka_admin_NewTopic_t** build_new_topics(PyObject* spec, Py_ssize_t* out_count) {
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    kafka_admin_NewTopic_t** topics = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(kafka_admin_NewTopic_t*));
    if (topics == NULL) { PyErr_NoMemory(); return NULL; }
    Py_ssize_t built = 0;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* name = NULL; int num_partitions = -1; short replication_factor = -1;
        PyObject* configs = Py_None; PyObject* assignments = Py_None;
        if (!item || !PyArg_ParseTuple(item, "sihOO", &name, &num_partitions,
                                       &replication_factor, &configs, &assignments)) {
            Py_XDECREF(item);
            free_new_topics(topics, built);
            return NULL;
        }
        kafka_admin_NewTopic_t* topic = kafka_admin_NewTopic_new(name, num_partitions,
                                                                (int16_t)replication_factor);
        if (topic == NULL) {
            Py_DECREF(item);
            free_new_topics(topics, built);
            PyErr_SetString(PyExc_ValueError, "topic name must not be None");
            return NULL;
        }
        topics[built++] = topic;
        int ok = apply_new_topic_configs(topic, configs) == 0
              && apply_new_topic_assignments(topic, assignments) == 0;
        Py_DECREF(item);
        if (!ok) {
            free_new_topics(topics, built);
            return NULL;
        }
    }
    *out_count = built;
    return topics;
}

static PyObject* py_Admin_create_topics_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms;
    int validate_only; int retry_on_quota_violation; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOippO", &h, &spec, &timeout_ms, &validate_only,
                          &retry_on_quota_violation, &cb))
        return NULL;
    Py_ssize_t count = 0;
    kafka_admin_NewTopic_t** topics = build_new_topics(spec, &count);
    if (topics == NULL) return NULL;
    Py_INCREF(cb);
    // The Rust side copies the NewTopics into owned values before returning, so
    // the handles can be freed as soon as the call returns.
    kafka_admin_AdminClient_create_topics_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h,
        (const kafka_admin_NewTopic_t* const*)topics, (int32_t)count,
        timeout_ms, validate_only ? true : false, retry_on_quota_violation ? true : false,
        admin_create_topics_trampoline, cb);
    free_new_topics(topics, count);
    Py_RETURN_NONE;
}

// ---- deleteTopics / listTopics / describeTopics -----------------------------

static PyObject* py_Admin_delete_topics_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* names; int timeout_ms; int retry; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &names, &timeout_ms, &retry, &cb)) return NULL;
    const char** arr = NULL;
    Py_ssize_t n = topics_to_array(names, &arr);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_delete_topics_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        arr, (int32_t)n, timeout_ms, retry ? true : false, admin_delete_topics_trampoline, cb);
    PyMem_Free(arr);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_delete_topics_by_ids_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids; int timeout_ms; int retry; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &ids, &timeout_ms, &retry, &cb)) return NULL;
    const char** arr = NULL;
    Py_ssize_t n = topics_to_array(ids, &arr);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_delete_topics_by_ids_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        arr, (int32_t)n, timeout_ms, retry ? true : false, admin_delete_topics_trampoline, cb);
    PyMem_Free(arr);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_list_topics_async(PyObject* self, PyObject* args) {
    unsigned long long h; int timeout_ms; int list_internal; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KipO", &h, &timeout_ms, &list_internal, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_list_topics_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        timeout_ms, list_internal ? true : false, admin_list_topics_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_topics_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* names; int timeout_ms; int include_ops;
    int partition_size_limit; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipiO", &h, &names, &timeout_ms, &include_ops,
                          &partition_size_limit, &cb))
        return NULL;
    const char** arr = NULL;
    Py_ssize_t n = topics_to_array(names, &arr);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_topics_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        arr, (int32_t)n, timeout_ms, include_ops ? true : false, partition_size_limit,
        admin_describe_topics_trampoline, cb);
    PyMem_Free(arr);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_topics_by_ids_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids; int timeout_ms; int include_ops;
    int partition_size_limit; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipiO", &h, &ids, &timeout_ms, &include_ops,
                          &partition_size_limit, &cb))
        return NULL;
    const char** arr = NULL;
    Py_ssize_t n = topics_to_array(ids, &arr);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_topics_by_ids_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        arr, (int32_t)n, timeout_ms, include_ops ? true : false, partition_size_limit,
        admin_describe_topics_trampoline, cb);
    PyMem_Free(arr);
    Py_RETURN_NONE;
}

// ---- createPartitions / deleteRecords --------------------------------------

// Frees `count` NewPartitions handles plus the array itself.
static void free_new_partitions(kafka_admin_NewPartitions_t** specs, Py_ssize_t count) {
    for (Py_ssize_t i = 0; i < count; i++) {
        kafka_admin_NewPartitions_destroy(specs[i]);
    }
    PyMem_Free(specs);
}

// Builds parallel (topic name, NewPartitions handle) arrays from a sequence of
// (topic:str, total_count:int, [[broker_id]]) tuples. The name pointers borrow
// from the spec sequence's str objects, which the caller keeps alive for the
// duration of the submit (as topics_to_array does).
// Returns the count, or -1 with a Python exception set.
static Py_ssize_t build_new_partitions(PyObject* spec, const char*** out_topics,
                                       kafka_admin_NewPartitions_t*** out_specs) {
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return -1;
    const char** topics = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    kafka_admin_NewPartitions_t** specs =
        PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(kafka_admin_NewPartitions_t*));
    if (topics == NULL || specs == NULL) {
        PyMem_Free(topics); PyMem_Free(specs); PyErr_NoMemory(); return -1;
    }
    Py_ssize_t built = 0;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* name = NULL; int total_count = 0; PyObject* assignments = Py_None;
        if (!item || !PyArg_ParseTuple(item, "siO", &name, &total_count, &assignments)) {
            Py_XDECREF(item);
            free_new_partitions(specs, built);
            PyMem_Free(topics);
            return -1;
        }
        // `assignments is not None` is the discriminant, passed through rather
        // than derived from the number of rows: Java's
        // increaseTo(int, List<List<Integer>>) with an empty list is a different
        // broker request from increaseTo(int), whose newAssignments is null.
        kafka_admin_NewPartitions_t* np =
            kafka_admin_NewPartitions_new(total_count, assignments != Py_None);
        specs[built] = np;
        topics[built] = name;
        built++;
        int ok = 1;
        if (assignments != Py_None) {
            Py_ssize_t an = PySequence_Size(assignments);
            if (an < 0) {
                ok = 0;
            }
            for (Py_ssize_t j = 0; ok && j < an; j++) {
                PyObject* brokers = PySequence_GetItem(assignments, j);  // new ref
                Py_ssize_t bn = brokers ? PySequence_Size(brokers) : -1;
                if (bn < 0) { Py_XDECREF(brokers); ok = 0; break; }
                int32_t* ids = bn > 0 ? PyMem_Malloc((size_t)bn * sizeof(int32_t)) : NULL;
                if (bn > 0 && ids == NULL) { Py_DECREF(brokers); PyErr_NoMemory(); ok = 0; break; }
                for (Py_ssize_t k = 0; k < bn; k++) {
                    PyObject* b = PySequence_GetItem(brokers, k);  // new ref
                    if (b == NULL) { ok = 0; break; }
                    long id = PyLong_AsLong(b);
                    Py_DECREF(b);
                    if (id == -1 && PyErr_Occurred()) { ok = 0; break; }
                    ids[k] = (int32_t)id;
                }
                if (ok) kafka_admin_NewPartitions_add_assignment(np, ids, (int32_t)bn);
                PyMem_Free(ids);
                Py_DECREF(brokers);
            }
        }
        Py_DECREF(item);
        if (!ok) {
            free_new_partitions(specs, built);
            PyMem_Free(topics);
            return -1;
        }
    }
    *out_topics = topics;
    *out_specs = specs;
    return built;
}

static PyObject* py_Admin_create_partitions_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms;
    int validate_only; int retry_on_quota_violation; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOippO", &h, &spec, &timeout_ms, &validate_only,
                          &retry_on_quota_violation, &cb))
        return NULL;
    const char** topics = NULL;
    kafka_admin_NewPartitions_t** specs = NULL;
    Py_ssize_t count = build_new_partitions(spec, &topics, &specs);
    if (count < 0) return NULL;
    Py_INCREF(cb);
    // The Rust side copies the NewPartitions into owned values before returning,
    // so the handles can be freed as soon as the call returns.
    kafka_admin_AdminClient_create_partitions_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, topics,
        (const kafka_admin_NewPartitions_t* const*)specs, (int32_t)count,
        timeout_ms, validate_only ? true : false, retry_on_quota_violation ? true : false,
        admin_create_partitions_trampoline, cb);
    free_new_partitions(specs, count);
    PyMem_Free(topics);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_delete_records_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &spec, &timeout_ms, &cb)) return NULL;

    // spec is a sequence of (topic:str, partition:int, before_offset:int).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    const char** topics = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    int32_t* partitions = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    int64_t* offsets = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
    if (topics == NULL || partitions == NULL || offsets == NULL) {
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* t = NULL; int p = 0; long long off = 0;
        int ok = item && PyArg_ParseTuple(item, "siL", &t, &p, &off);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
            return NULL;
        }
        topics[i] = t; partitions[i] = p; offsets[i] = (int64_t)off;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_delete_records_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        topics, partitions, offsets, (int32_t)n, timeout_ms,
        admin_delete_records_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
    Py_RETURN_NONE;
}

// ---- result drains ---------------------------------------------------------

// Copies a *borrowed* per-key error into (code, message, is_retriable, is_fatal),
// or None when the key succeeded. Borrowed errors die with their result handle,
// so they must be copied before the drain destroys it — and must NOT be passed
// to KafkaError_destroy.
static PyObject* borrowed_error_to_py(const kafka_common_Error_t* e) {
    if (e == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(isii)", kafka_common_Error_code(e),
                         kafka_common_Error_message(e),
                         kafka_common_Error_is_retriable_error(e) ? 1 : 0,
                         error_is_fatal(e) ? 1 : 0);
}

// Builds the (error, value) pair every admin `*_drain` maps a key to, consuming
// both references on every path and returning NULL on failure with an exception
// already set by whichever step failed.
//
// Callers must NOT Py_XDECREF the arguments after a NULL return. Py_BuildValue's
// 'N' unit steals its argument's reference even when the build itself fails:
// CPython's do_mktuple routes a PyTuple_New failure through do_ignore, which
// re-runs do_mkvalue over the remaining format units and releases each result.
// Releasing them again here is a double-decref. It is only reachable under
// allocation failure, which is exactly when a double-decref is least survivable.
static PyObject* error_value_pair(PyObject* err, PyObject* value) {
    if (err == NULL || value == NULL) {
        Py_XDECREF(err);
        Py_XDECREF(value);
        return NULL;
    }
    return Py_BuildValue("(NN)", err, value);
}

// (topic_id, num_partitions, replication_factor,
//  [(name, value, is_default, is_sensitive, is_read_only)], embedded_error)
static PyObject* topic_metadata_to_py(const kafka_admin_TopicMetadataAndConfig_t* mc) {
    int32_t n = kafka_admin_TopicMetadataAndConfig_config_count(mc);
    PyObject* configs = PyList_New(n < 0 ? 0 : n);
    if (configs == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* entry = Py_BuildValue(
            "(ssiii)",
            kafka_admin_TopicMetadataAndConfig_config_name(mc, i),
            kafka_admin_TopicMetadataAndConfig_config_value(mc, i),
            kafka_admin_TopicMetadataAndConfig_config_is_default(mc, i) ? 1 : 0,
            kafka_admin_TopicMetadataAndConfig_config_is_sensitive(mc, i) ? 1 : 0,
            kafka_admin_TopicMetadataAndConfig_config_is_read_only(mc, i) ? 1 : 0);
        if (entry == NULL) { Py_DECREF(configs); return NULL; }
        PyList_SET_ITEM(configs, i, entry);
    }
    PyObject* embedded = borrowed_error_to_py(kafka_admin_TopicMetadataAndConfig_error(mc));
    if (embedded == NULL) { Py_DECREF(configs); return NULL; }
    return Py_BuildValue("(siiNN)",
                         kafka_admin_TopicMetadataAndConfig_topic_id(mc),
                         kafka_admin_TopicMetadataAndConfig_num_partitions(mc),
                         kafka_admin_TopicMetadataAndConfig_replication_factor(mc),
                         configs, embedded);
}

static int32_t topic_description_acl_at(const void* d, int32_t i) {
    return kafka_admin_TopicDescription_authorized_operation(
        (const kafka_admin_TopicDescription_t*)d, i);
}

static int32_t describe_cluster_acl_at(const void* r, int32_t i) {
    return kafka_admin_DescribeClusterResult_authorized_operation(
        (const kafka_admin_DescribeClusterResult_t*)r, i);
}

// [AclOperation code, ...], or None when the broker did not report the set.
//
// `present` is the Rust `*_has_authorized_operations` bit. Counts are never
// negative (see the `counts are never negative` section of src/ffi/admin.rs), so
// an absent set and a reported-but-empty one both count 0 and only `present`
// separates them -- which is exactly Java's null vs empty Set<AclOperation>.
static PyObject* acl_codes_to_py(bool present, int32_t count, int32_t (*get)(const void*, int32_t),
                                 const void* owner) {
    if (!present) Py_RETURN_NONE;
    PyObject* out = PyList_New(count);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < count; i++) {
        PyObject* code = PyLong_FromLong(get(owner, i));
        if (code == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, code);
    }
    return out;
}

// (partition, leader, replicas, isr, elr, last_known_elr)
static PyObject* topic_partition_info_to_py(const kafka_common_TopicPartitionInfo_t* info) {
    PyObject* leader = node_to_py(kafka_common_TopicPartitionInfo_leader(info));
    PyObject* replicas = node_list_to_py(kafka_common_TopicPartitionInfo_replicas(info));
    PyObject* isr = node_list_to_py(kafka_common_TopicPartitionInfo_isr(info));
    // Java's elr()/lastKnownElr() are null when the broker did not report the
    // set, which stays distinct from a reported-but-empty one: the C getters
    // return NULL for the former, which node_list_to_py maps to None.
    PyObject* elr = node_list_to_py(kafka_common_TopicPartitionInfo_elr(info));
    PyObject* last_elr = node_list_to_py(kafka_common_TopicPartitionInfo_last_known_elr(info));
    if (!leader || !replicas || !isr || !elr || !last_elr) {
        Py_XDECREF(leader); Py_XDECREF(replicas); Py_XDECREF(isr);
        Py_XDECREF(elr); Py_XDECREF(last_elr);
        return NULL;
    }
    return Py_BuildValue("(iNNNNN)", kafka_common_TopicPartitionInfo_partition(info),
                         leader, replicas, isr, elr, last_elr);
}

// (name, topic_id, is_internal, [partition_info], [acl_operation_codes])
static PyObject* topic_description_to_py(const kafka_admin_TopicDescription_t* d) {
    int32_t pn = kafka_admin_TopicDescription_partition_count(d);
    PyObject* partitions = PyList_New(pn < 0 ? 0 : pn);
    if (partitions == NULL) return NULL;
    for (int32_t i = 0; i < pn; i++) {
        PyObject* p = topic_partition_info_to_py(kafka_admin_TopicDescription_partition(d, i));
        if (p == NULL) { Py_DECREF(partitions); return NULL; }
        PyList_SET_ITEM(partitions, i, p);
    }
    PyObject* operations =
        acl_codes_to_py(kafka_admin_TopicDescription_has_authorized_operations(d),
                        kafka_admin_TopicDescription_authorized_operation_count(d),
                        topic_description_acl_at, d);
    if (operations == NULL) { Py_DECREF(partitions); return NULL; }
    return Py_BuildValue("(ssiNN)", kafka_admin_TopicDescription_name(d),
                         kafka_admin_TopicDescription_topic_id(d),
                         kafka_admin_TopicDescription_is_internal(d) ? 1 : 0,
                         partitions, operations);
}

// {topic_name: (error, metadata)}
static PyObject* py_CreateTopicsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_CreateTopicsResult_t* r = (kafka_admin_CreateTopicsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_CreateTopicsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_CreateTopicsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(kafka_admin_CreateTopicsResult_get_key(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_CreateTopicsResult_get_error(r, i));
        const kafka_admin_TopicMetadataAndConfig_t* mc =
            kafka_admin_CreateTopicsResult_get_value(r, i);
        PyObject* meta = mc ? topic_metadata_to_py(mc) : (Py_INCREF(Py_None), Py_None);
        PyObject* val = error_value_pair(err, meta);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_CreateTopicsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_CreateTopicsResult_destroy(r);
    return d;
}

// {key: error_or_None} -- no per-key value (Java's future is KafkaFuture<Void>)
static PyObject* py_DeleteTopicsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DeleteTopicsResult_t* r = (kafka_admin_DeleteTopicsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DeleteTopicsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DeleteTopicsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(kafka_admin_DeleteTopicsResult_get_key(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_DeleteTopicsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_DeleteTopicsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_DeleteTopicsResult_destroy(r);
    return d;
}

// {topic_name: (name, topic_id, is_internal)}
static PyObject* py_ListTopicsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListTopicsResult_t* r = (kafka_admin_ListTopicsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ListTopicsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_ListTopicsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_admin_TopicListing_t* listing = kafka_admin_ListTopicsResult_get_value(r, i);
        PyObject* key = PyUnicode_FromString(kafka_admin_ListTopicsResult_get_key(r, i));
        PyObject* val = listing ? Py_BuildValue("(ssi)",
                                               kafka_admin_TopicListing_name(listing),
                                               kafka_admin_TopicListing_topic_id(listing),
                                               kafka_admin_TopicListing_is_internal(listing) ? 1 : 0)
                                : NULL;
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_ListTopicsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_ListTopicsResult_destroy(r);
    return d;
}

// {key: (error, description)}
static PyObject* py_DescribeTopicsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeTopicsResult_t* r = (kafka_admin_DescribeTopicsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeTopicsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeTopicsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(kafka_admin_DescribeTopicsResult_get_key(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_DescribeTopicsResult_get_error(r, i));
        const kafka_admin_TopicDescription_t* desc =
            kafka_admin_DescribeTopicsResult_get_value(r, i);
        PyObject* value = desc ? topic_description_to_py(desc) : (Py_INCREF(Py_None), Py_None);
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_DescribeTopicsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_DescribeTopicsResult_destroy(r);
    return d;
}

// {topic_name: error_or_None} -- no per-key value (Java's future is
// KafkaFuture<Void>, as for deleteTopics)
static PyObject* py_CreatePartitionsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_CreatePartitionsResult_t* r = (kafka_admin_CreatePartitionsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_CreatePartitionsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_CreatePartitionsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(kafka_admin_CreatePartitionsResult_get_key(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_CreatePartitionsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_CreatePartitionsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_CreatePartitionsResult_destroy(r);
    return d;
}

// {(topic, partition): (error, low_watermark)}
static PyObject* py_DeleteRecordsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DeleteRecordsResult_t* r = (kafka_admin_DeleteRecordsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DeleteRecordsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DeleteRecordsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = Py_BuildValue("(si)", kafka_admin_DeleteRecordsResult_get_topic(r, i),
                                      kafka_admin_DeleteRecordsResult_get_partition(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_DeleteRecordsResult_get_error(r, i));
        // Not error_value_pair: the second slot is a plain long watermark, not
        // an object. Same 'N'-steals-even-on-failure rule though, so `err` is
        // released only on the branch where Py_BuildValue never ran.
        PyObject* val = NULL;
        if (key == NULL || err == NULL) {
            Py_XDECREF(err);
        } else {
            val = Py_BuildValue("(NL)", err,
                                (long long)kafka_admin_DeleteRecordsResult_get_low_watermark(r, i));
        }
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_DeleteRecordsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_DeleteRecordsResult_destroy(r);
    return d;
}

// Method definitions
// ===========================================================================
// Admin — B2: cluster, configs, log dirs
//
// Same marshaling-only contract as the B1 section above: async submits hand the
// GIL back immediately, the callback returns opaque handle ints, and the
// matching *_drain converts and destroys the handle.
// ===========================================================================

// ---- trampolines -----------------------------------------------------------

static void admin_describe_cluster_trampoline(kafka_admin_DescribeClusterResult_t* r,
                                              kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_configs_trampoline(kafka_admin_DescribeConfigsResult_t* r,
                                              kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_configs_trampoline(kafka_admin_AlterConfigsResult_t* r,
                                           kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_config_resources_trampoline(kafka_admin_ListConfigResourcesResult_t* r,
                                                   kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_log_dirs_trampoline(kafka_admin_DescribeLogDirsResult_t* r,
                                               kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_replica_log_dirs_trampoline(kafka_admin_AlterReplicaLogDirsResult_t* r,
                                                    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_replica_log_dirs_trampoline(kafka_admin_DescribeReplicaLogDirsResult_t* r,
                                                       kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }

// ---- input marshaling helpers ----------------------------------------------

// Reads a sequence of ints into a freshly malloc'd int32_t array. Returns 0 on
// success (with *out_values possibly NULL when the sequence is empty), -1 with
// a Python exception set otherwise.
static int build_int_array(PyObject* seq, int32_t** out_values, Py_ssize_t* out_count) {
    Py_ssize_t n = PySequence_Size(seq);
    if (n < 0) return -1;
    *out_count = n;
    *out_values = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    if (*out_values == NULL) { PyErr_NoMemory(); return -1; }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(seq, i);  // new ref
        long v = item ? PyLong_AsLong(item) : -1;
        Py_XDECREF(item);
        if (v == -1 && PyErr_Occurred()) { PyMem_Free(*out_values); *out_values = NULL; return -1; }
        (*out_values)[i] = (int32_t)v;
    }
    return 0;
}

// ---- submits ---------------------------------------------------------------

static PyObject* py_Admin_describe_cluster_async(PyObject* self, PyObject* args) {
    unsigned long long h; int timeout_ms; int include_auth; int include_fenced; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KiiiO", &h, &timeout_ms, &include_auth, &include_fenced, &cb))
        return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_cluster_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        timeout_ms, include_auth ? true : false, include_fenced ? true : false,
        admin_describe_cluster_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_configs_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms; int synonyms; int documentation; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiiiO", &h, &spec, &timeout_ms, &synonyms, &documentation, &cb))
        return NULL;

    // spec is a sequence of (resource_type:int, resource_name:str).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    int32_t* types = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    const char** names = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    if (types == NULL || names == NULL) {
        PyMem_Free(types); PyMem_Free(names); PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        int t = 0; const char* name = NULL;
        int ok = item && PyArg_ParseTuple(item, "is", &t, &name);
        Py_XDECREF(item);
        if (!ok) { PyMem_Free(types); PyMem_Free(names); return NULL; }
        types[i] = (int32_t)t; names[i] = name;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_configs_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        types, names, (int32_t)n, timeout_ms, synonyms ? true : false,
        documentation ? true : false, admin_describe_configs_trampoline, cb);
    PyMem_Free(types); PyMem_Free(names);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_incremental_alter_configs_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms; int validate_only; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiiO", &h, &spec, &timeout_ms, &validate_only, &cb)) return NULL;

    // spec is a sequence of one row per operation:
    // (resource_type:int, resource_name:str, config_name:str, value:str|None, op_type:int).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    int32_t* types = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    const char** resources = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    const char** keys = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    const char** values = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    int32_t* ops = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    if (types == NULL || resources == NULL || keys == NULL || values == NULL || ops == NULL) {
        PyMem_Free(types); PyMem_Free(resources); PyMem_Free(keys);
        PyMem_Free(values); PyMem_Free(ops);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        int t = 0; int op = 0;
        const char* resource = NULL; const char* key = NULL; const char* value = NULL;
        // "z" accepts None for the value, which is what DELETE sends.
        int ok = item && PyArg_ParseTuple(item, "isszi", &t, &resource, &key, &value, &op);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(types); PyMem_Free(resources); PyMem_Free(keys);
            PyMem_Free(values); PyMem_Free(ops);
            return NULL;
        }
        types[i] = (int32_t)t; resources[i] = resource; keys[i] = key;
        values[i] = value; ops[i] = (int32_t)op;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_incremental_alter_configs_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, types, resources, keys, values, ops,
        (int32_t)n, timeout_ms, validate_only ? true : false,
        admin_alter_configs_trampoline, cb);
    PyMem_Free(types); PyMem_Free(resources); PyMem_Free(keys);
    PyMem_Free(values); PyMem_Free(ops);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_list_config_resources_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* types_seq; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &types_seq, &timeout_ms, &cb)) return NULL;

    int32_t* types = NULL; Py_ssize_t n = 0;
    if (build_int_array(types_seq, &types, &n) < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_list_config_resources_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, types, (int32_t)n, timeout_ms,
        admin_list_config_resources_trampoline, cb);
    PyMem_Free(types);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_log_dirs_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* brokers_seq; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &brokers_seq, &timeout_ms, &cb)) return NULL;

    int32_t* brokers = NULL; Py_ssize_t n = 0;
    if (build_int_array(brokers_seq, &brokers, &n) < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_log_dirs_async((kafka_admin_AdminClient_t*)(uintptr_t)h,
        brokers, (int32_t)n, timeout_ms, admin_describe_log_dirs_trampoline, cb);
    PyMem_Free(brokers);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_alter_replica_log_dirs_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &spec, &timeout_ms, &cb)) return NULL;

    // spec is a sequence of (topic:str, partition:int, broker_id:int, log_dir:str).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    const char** topics = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    int32_t* partitions = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    int32_t* brokers = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    const char** log_dirs = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    if (topics == NULL || partitions == NULL || brokers == NULL || log_dirs == NULL) {
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(brokers); PyMem_Free(log_dirs);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* topic = NULL; const char* dir = NULL; int p = 0; int b = 0;
        int ok = item && PyArg_ParseTuple(item, "siis", &topic, &p, &b, &dir);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(brokers); PyMem_Free(log_dirs);
            return NULL;
        }
        topics[i] = topic; partitions[i] = (int32_t)p; brokers[i] = (int32_t)b; log_dirs[i] = dir;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_alter_replica_log_dirs_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, topics, partitions, brokers, log_dirs,
        (int32_t)n, timeout_ms, admin_alter_replica_log_dirs_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(brokers); PyMem_Free(log_dirs);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_replica_log_dirs_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &spec, &timeout_ms, &cb)) return NULL;

    // spec is a sequence of (topic:str, partition:int, broker_id:int).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    const char** topics = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    int32_t* partitions = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    int32_t* brokers = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    if (topics == NULL || partitions == NULL || brokers == NULL) {
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(brokers);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* topic = NULL; int p = 0; int b = 0;
        int ok = item && PyArg_ParseTuple(item, "sii", &topic, &p, &b);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(brokers);
            return NULL;
        }
        topics[i] = topic; partitions[i] = (int32_t)p; brokers[i] = (int32_t)b;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_replica_log_dirs_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, topics, partitions, brokers, (int32_t)n,
        timeout_ms, admin_describe_replica_log_dirs_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(brokers);
    Py_RETURN_NONE;
}

// ---- result drains ---------------------------------------------------------

// (cluster_id, [node], controller_or_None, [acl_operation_codes] or None)
static PyObject* py_DescribeClusterResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeClusterResult_t* r = (kafka_admin_DescribeClusterResult_t*)(uintptr_t)ptr;

    int32_t n = kafka_admin_DescribeClusterResult_node_count(r);
    PyObject* nodes = PyList_New(n < 0 ? 0 : n);
    if (nodes == NULL) { kafka_admin_DescribeClusterResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* node = node_to_py(kafka_admin_DescribeClusterResult_get_node(r, i));
        if (node == NULL) { Py_DECREF(nodes); kafka_admin_DescribeClusterResult_destroy(r); return NULL; }
        PyList_SET_ITEM(nodes, i, node);
    }

    PyObject* controller = node_to_py(kafka_admin_DescribeClusterResult_controller(r));
    if (controller == NULL) { Py_DECREF(nodes); kafka_admin_DescribeClusterResult_destroy(r); return NULL; }

    // A false presence bit is Java's null -- the broker did not report the
    // operations at all -- which stays distinct from an empty list.
    PyObject* operations =
        acl_codes_to_py(kafka_admin_DescribeClusterResult_has_authorized_operations(r),
                        kafka_admin_DescribeClusterResult_authorized_operation_count(r),
                        describe_cluster_acl_at, r);
    if (operations == NULL) {
        Py_DECREF(nodes); Py_DECREF(controller);
        kafka_admin_DescribeClusterResult_destroy(r); return NULL;
    }

    PyObject* out = Py_BuildValue("(sNNN)", kafka_admin_DescribeClusterResult_cluster_id(r),
                                  nodes, controller, operations);
    kafka_admin_DescribeClusterResult_destroy(r);
    return out;
}

// (name, value, is_default, is_sensitive, is_read_only, source, type,
//  documentation, [(synonym_name, synonym_value, synonym_source)])
static PyObject* config_entry_to_py(const kafka_admin_ConfigEntry_t* entry) {
    int32_t n = kafka_admin_ConfigEntry_synonym_count(entry);
    PyObject* synonyms = PyList_New(n < 0 ? 0 : n);
    if (synonyms == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* s = Py_BuildValue("(szs)",
                                    kafka_admin_ConfigEntry_synonym_name(entry, i),
                                    kafka_admin_ConfigEntry_synonym_value(entry, i),
                                    kafka_admin_ConfigEntry_synonym_source(entry, i));
        if (s == NULL) { Py_DECREF(synonyms); return NULL; }
        PyList_SET_ITEM(synonyms, i, s);
    }
    // "z" yields None for a NULL value / documentation.
    return Py_BuildValue("(sziiisszN)",
                         kafka_admin_ConfigEntry_name(entry),
                         kafka_admin_ConfigEntry_value(entry),
                         kafka_admin_ConfigEntry_is_default(entry) ? 1 : 0,
                         kafka_admin_ConfigEntry_is_sensitive(entry) ? 1 : 0,
                         kafka_admin_ConfigEntry_is_read_only(entry) ? 1 : 0,
                         kafka_admin_ConfigEntry_source(entry),
                         kafka_admin_ConfigEntry_type(entry),
                         kafka_admin_ConfigEntry_documentation(entry),
                         synonyms);
}

// [config_entry]
static PyObject* config_to_py(const kafka_admin_Config_t* config) {
    int32_t n = kafka_admin_Config_entry_count(config);
    PyObject* entries = PyList_New(n < 0 ? 0 : n);
    if (entries == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* e = config_entry_to_py(kafka_admin_Config_get_entry(config, i));
        if (e == NULL) { Py_DECREF(entries); return NULL; }
        PyList_SET_ITEM(entries, i, e);
    }
    return entries;
}

// {(resource_type, resource_name): (error, [config_entry])}
static PyObject* py_DescribeConfigsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeConfigsResult_t* r = (kafka_admin_DescribeConfigsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeConfigsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeConfigsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = Py_BuildValue("(is)",
                                      kafka_admin_DescribeConfigsResult_get_key_type(r, i),
                                      kafka_admin_DescribeConfigsResult_get_key_name(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_DescribeConfigsResult_get_error(r, i));
        const kafka_admin_Config_t* config = kafka_admin_DescribeConfigsResult_get_value(r, i);
        PyObject* value = config ? config_to_py(config) : (Py_INCREF(Py_None), Py_None);
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_DescribeConfigsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_DescribeConfigsResult_destroy(r);
    return d;
}

// {(resource_type, resource_name): error_or_None}
static PyObject* py_AlterConfigsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_AlterConfigsResult_t* r = (kafka_admin_AlterConfigsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_AlterConfigsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_AlterConfigsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = Py_BuildValue("(is)",
                                      kafka_admin_AlterConfigsResult_get_key_type(r, i),
                                      kafka_admin_AlterConfigsResult_get_key_name(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_AlterConfigsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_AlterConfigsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_AlterConfigsResult_destroy(r);
    return d;
}

// [(resource_type, resource_name)]
static PyObject* py_ListConfigResourcesResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListConfigResourcesResult_t* r =
        (kafka_admin_ListConfigResourcesResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ListConfigResourcesResult_count(r);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_admin_ListConfigResourcesResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* item = Py_BuildValue("(is)",
                                       kafka_admin_ListConfigResourcesResult_get_type(r, i),
                                       kafka_admin_ListConfigResourcesResult_get_name(r, i));
        if (item == NULL) { Py_DECREF(out); kafka_admin_ListConfigResourcesResult_destroy(r); return NULL; }
        PyList_SET_ITEM(out, i, item);
    }
    kafka_admin_ListConfigResourcesResult_destroy(r);
    return out;
}

// (error, total_bytes, usable_bytes, [(topic, partition, size, offset_lag, is_future)])
static PyObject* log_dir_description_to_py(const kafka_admin_LogDirDescription_t* dir) {
    int32_t n = kafka_admin_LogDirDescription_replica_count(dir);
    PyObject* replicas = PyList_New(n < 0 ? 0 : n);
    if (replicas == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* replica = Py_BuildValue(
            "(siLLi)",
            kafka_admin_LogDirDescription_replica_topic(dir, i),
            kafka_admin_LogDirDescription_replica_partition(dir, i),
            (long long)kafka_admin_LogDirDescription_replica_size(dir, i),
            (long long)kafka_admin_LogDirDescription_replica_offset_lag(dir, i),
            kafka_admin_LogDirDescription_replica_is_future(dir, i) ? 1 : 0);
        if (replica == NULL) { Py_DECREF(replicas); return NULL; }
        PyList_SET_ITEM(replicas, i, replica);
    }
    PyObject* err = borrowed_error_to_py(kafka_admin_LogDirDescription_error(dir));
    if (err == NULL) { Py_DECREF(replicas); return NULL; }
    return Py_BuildValue("(NLLN)", err,
                         (long long)kafka_admin_LogDirDescription_total_bytes(dir),
                         (long long)kafka_admin_LogDirDescription_usable_bytes(dir),
                         replicas);
}

// {log_dir: log_dir_description}
static PyObject* log_dir_map_to_py(const kafka_admin_LogDirDescriptionMap_t* map) {
    int32_t n = kafka_admin_LogDirDescriptionMap_count(map);
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(kafka_admin_LogDirDescriptionMap_get_key(map, i));
        PyObject* val = log_dir_description_to_py(kafka_admin_LogDirDescriptionMap_get_value(map, i));
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    return d;
}

// {broker_id: (error, {log_dir: log_dir_description})}
static PyObject* py_DescribeLogDirsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeLogDirsResult_t* r = (kafka_admin_DescribeLogDirsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeLogDirsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeLogDirsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyLong_FromLong(kafka_admin_DescribeLogDirsResult_get_broker(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_DescribeLogDirsResult_get_error(r, i));
        const kafka_admin_LogDirDescriptionMap_t* map =
            kafka_admin_DescribeLogDirsResult_get_value(r, i);
        PyObject* value = map ? log_dir_map_to_py(map) : (Py_INCREF(Py_None), Py_None);
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_DescribeLogDirsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_DescribeLogDirsResult_destroy(r);
    return d;
}

// {(topic, partition, broker_id): error_or_None}
static PyObject* py_AlterReplicaLogDirsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_AlterReplicaLogDirsResult_t* r =
        (kafka_admin_AlterReplicaLogDirsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_AlterReplicaLogDirsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_AlterReplicaLogDirsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = Py_BuildValue("(sii)",
                                      kafka_admin_AlterReplicaLogDirsResult_get_topic(r, i),
                                      kafka_admin_AlterReplicaLogDirsResult_get_partition(r, i),
                                      kafka_admin_AlterReplicaLogDirsResult_get_broker_id(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_AlterReplicaLogDirsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_AlterReplicaLogDirsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_AlterReplicaLogDirsResult_destroy(r);
    return d;
}

// {(topic, partition, broker_id):
//   (error, (current_log_dir, current_offset_lag, future_log_dir, future_offset_lag))}
static PyObject* py_DescribeReplicaLogDirsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeReplicaLogDirsResult_t* r =
        (kafka_admin_DescribeReplicaLogDirsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeReplicaLogDirsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeReplicaLogDirsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = Py_BuildValue(
            "(sii)", kafka_admin_DescribeReplicaLogDirsResult_get_topic(r, i),
            kafka_admin_DescribeReplicaLogDirsResult_get_partition(r, i),
            kafka_admin_DescribeReplicaLogDirsResult_get_broker_id(r, i));
        PyObject* err = borrowed_error_to_py(
            kafka_admin_DescribeReplicaLogDirsResult_get_error(r, i));
        const kafka_admin_ReplicaLogDirInfo_t* info =
            kafka_admin_DescribeReplicaLogDirsResult_get_value(r, i);
        PyObject* value = info
            ? Py_BuildValue("(zLzL)",
                            kafka_admin_ReplicaLogDirInfo_current_replica_log_dir(info),
                            (long long)kafka_admin_ReplicaLogDirInfo_current_replica_offset_lag(info),
                            kafka_admin_ReplicaLogDirInfo_future_replica_log_dir(info),
                            (long long)kafka_admin_ReplicaLogDirInfo_future_replica_offset_lag(info))
            : (Py_INCREF(Py_None), Py_None);
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_DescribeReplicaLogDirsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_DescribeReplicaLogDirsResult_destroy(r);
    return d;
}

// ===========================================================================
// Admin — B3: elections, reassignments, offsets
//
// Same marshaling-only contract as the sections above. Every RPC here is
// partition-keyed, so each spec row starts with (topic:str, partition:int) and
// the drains key their dicts on that pair. Java's three Optionals cross as
// explicit boolean discriminants (`all_partitions`, `cancel`, `is_timestamp`),
// never an overloaded None.
// ===========================================================================

// ---- trampolines -----------------------------------------------------------

static void admin_elect_leaders_trampoline(kafka_admin_ElectLeadersResult_t* r,
                                           kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_partition_reassignments_trampoline(
    kafka_admin_AlterPartitionReassignmentsResult_t* r,
    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_partition_reassignments_trampoline(
    kafka_admin_ListPartitionReassignmentsResult_t* r,
    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_offsets_trampoline(kafka_admin_ListOffsetsResult_t* r,
                                          kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }

// ---- shared (topic, partition) spec reader ---------------------------------

// Reads a sequence of (topic:str, partition:int) rows into two parallel arrays.
// On success returns the row count and sets *out_topics / *out_partitions (both
// PyMem_Malloc'd, caller frees). On failure returns -1 with an exception set and
// nothing allocated. The topic pointers borrow from the spec sequence, which the
// caller must keep alive across the FFI call.
static Py_ssize_t build_topic_partitions(PyObject* spec, const char*** out_topics,
                                         int32_t** out_partitions) {
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return -1;
    const char** topics = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    int32_t* partitions = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(int32_t));
    if (topics == NULL || partitions == NULL) {
        PyMem_Free(topics); PyMem_Free(partitions);
        PyErr_NoMemory(); return -1;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* topic = NULL; int p = 0;
        int ok = item && PyArg_ParseTuple(item, "si", &topic, &p);
        Py_XDECREF(item);
        if (!ok) { PyMem_Free(topics); PyMem_Free(partitions); return -1; }
        topics[i] = topic; partitions[i] = (int32_t)p;
    }
    *out_topics = topics; *out_partitions = partitions;
    return n;
}

// Builds a (topic, partition) dict key.
static PyObject* topic_partition_key(const char* topic, int32_t partition) {
    return Py_BuildValue("(si)", topic, partition);
}

// ---- submits ---------------------------------------------------------------

static PyObject* py_Admin_elect_leaders_async(PyObject* self, PyObject* args) {
    unsigned long long h; int election_type; int all_partitions; PyObject* spec;
    int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KipOiO", &h, &election_type, &all_partitions, &spec,
                          &timeout_ms, &cb))
        return NULL;

    const char** topics = NULL; int32_t* partitions = NULL;
    Py_ssize_t n = build_topic_partitions(spec, &topics, &partitions);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_elect_leaders_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, (int32_t)election_type,
        all_partitions ? true : false, topics, partitions, (int32_t)n, timeout_ms,
        admin_elect_leaders_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(partitions);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_alter_partition_reassignments_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms;
    int allow_replication_factor_change; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &spec, &timeout_ms,
                          &allow_replication_factor_change, &cb))
        return NULL;

    // spec is a sequence of (topic:str, partition:int, cancel:bool, [replica:int]).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** topics = PyMem_Malloc(slots * sizeof(char*));
    int32_t* partitions = PyMem_Malloc(slots * sizeof(int32_t));
    bool* cancel = PyMem_Malloc(slots * sizeof(bool));
    const int32_t** replica_ptrs = PyMem_Malloc(slots * sizeof(int32_t*));
    int32_t* replica_counts = PyMem_Malloc(slots * sizeof(int32_t));
    if (topics == NULL || partitions == NULL || cancel == NULL || replica_ptrs == NULL ||
        replica_counts == NULL) {
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(cancel);
        PyMem_Free(replica_ptrs); PyMem_Free(replica_counts);
        PyErr_NoMemory(); return NULL;
    }
    // Each row's replica array is allocated separately; `built` counts how many
    // are live so a mid-loop failure frees exactly those.
    Py_ssize_t built = 0;
    int ok = 1;
    for (; built < n; built++) {
        PyObject* item = PySequence_GetItem(spec, built);  // new ref
        const char* topic = NULL; int p = 0; int c = 0; PyObject* replicas = NULL;
        if (item == NULL || !PyArg_ParseTuple(item, "sipO", &topic, &p, &c, &replicas)) {
            Py_XDECREF(item); ok = 0; break;
        }
        topics[built] = topic; partitions[built] = (int32_t)p;
        cancel[built] = c ? true : false;
        replica_ptrs[built] = NULL; replica_counts[built] = 0;

        Py_ssize_t rn = PySequence_Size(replicas);
        if (rn < 0) { Py_DECREF(item); ok = 0; break; }
        if (rn > 0) {
            int32_t* ids = PyMem_Malloc((size_t)rn * sizeof(int32_t));
            if (ids == NULL) { Py_DECREF(item); PyErr_NoMemory(); ok = 0; break; }
            for (Py_ssize_t k = 0; k < rn; k++) {
                PyObject* b = PySequence_GetItem(replicas, k);  // new ref
                long id = b ? PyLong_AsLong(b) : -1;
                Py_XDECREF(b);
                if (b == NULL || (id == -1 && PyErr_Occurred())) { ok = 0; break; }
                ids[k] = (int32_t)id;
            }
            if (!ok) { PyMem_Free(ids); Py_DECREF(item); break; }
            replica_ptrs[built] = ids; replica_counts[built] = (int32_t)rn;
        }
        Py_DECREF(item);
    }
    if (!ok) {
        for (Py_ssize_t i = 0; i < built; i++) PyMem_Free((void*)replica_ptrs[i]);
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(cancel);
        PyMem_Free(replica_ptrs); PyMem_Free(replica_counts);
        return NULL;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_alter_partition_reassignments_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, topics, partitions, cancel, replica_ptrs,
        replica_counts, (int32_t)n, timeout_ms,
        allow_replication_factor_change ? true : false,
        admin_alter_partition_reassignments_trampoline, cb);
    for (Py_ssize_t i = 0; i < n; i++) PyMem_Free((void*)replica_ptrs[i]);
    PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(cancel);
    PyMem_Free(replica_ptrs); PyMem_Free(replica_counts);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_list_partition_reassignments_async(PyObject* self, PyObject* args) {
    unsigned long long h; int all_partitions; PyObject* spec; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KpOiO", &h, &all_partitions, &spec, &timeout_ms, &cb))
        return NULL;

    const char** topics = NULL; int32_t* partitions = NULL;
    Py_ssize_t n = build_topic_partitions(spec, &topics, &partitions);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_list_partition_reassignments_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, all_partitions ? true : false, topics,
        partitions, (int32_t)n, timeout_ms, admin_list_partition_reassignments_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(partitions);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_list_offsets_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms; int isolation_level; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiiO", &h, &spec, &timeout_ms, &isolation_level, &cb))
        return NULL;

    // spec is a sequence of (topic:str, partition:int, is_timestamp:bool, value:int).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** topics = PyMem_Malloc(slots * sizeof(char*));
    int32_t* partitions = PyMem_Malloc(slots * sizeof(int32_t));
    bool* is_timestamp = PyMem_Malloc(slots * sizeof(bool));
    int64_t* values = PyMem_Malloc(slots * sizeof(int64_t));
    if (topics == NULL || partitions == NULL || is_timestamp == NULL || values == NULL) {
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(is_timestamp); PyMem_Free(values);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* topic = NULL; int p = 0; int ts = 0; long long value = 0;
        int ok = item && PyArg_ParseTuple(item, "sipL", &topic, &p, &ts, &value);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(is_timestamp);
            PyMem_Free(values);
            return NULL;
        }
        topics[i] = topic; partitions[i] = (int32_t)p;
        is_timestamp[i] = ts ? true : false; values[i] = (int64_t)value;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_list_offsets_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, topics, partitions, is_timestamp, values,
        (int32_t)n, timeout_ms, (int32_t)isolation_level, admin_list_offsets_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(is_timestamp); PyMem_Free(values);
    Py_RETURN_NONE;
}

// ---- result drains ---------------------------------------------------------

// {(topic, partition): error_or_None}
//
// Java's ElectLeadersResult.partitions() is Map<TopicPartition,
// Optional<Throwable>>: a per-partition error and no per-partition value, so
// this is not an (error, value) pair.
static PyObject* py_ElectLeadersResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ElectLeadersResult_t* r = (kafka_admin_ElectLeadersResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ElectLeadersResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_ElectLeadersResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = topic_partition_key(kafka_admin_ElectLeadersResult_get_topic(r, i),
                                            kafka_admin_ElectLeadersResult_get_partition(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_ElectLeadersResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_ElectLeadersResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_ElectLeadersResult_destroy(r);
    return d;
}

// {(topic, partition): error_or_None} — per-partition future is
// KafkaFuture<Void>, so None means success.
static PyObject* py_AlterPartitionReassignmentsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_AlterPartitionReassignmentsResult_t* r =
        (kafka_admin_AlterPartitionReassignmentsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_AlterPartitionReassignmentsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_AlterPartitionReassignmentsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = topic_partition_key(
            kafka_admin_AlterPartitionReassignmentsResult_get_topic(r, i),
            kafka_admin_AlterPartitionReassignmentsResult_get_partition(r, i));
        PyObject* err = borrowed_error_to_py(
            kafka_admin_AlterPartitionReassignmentsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_AlterPartitionReassignmentsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_AlterPartitionReassignmentsResult_destroy(r);
    return d;
}

// Builds [broker_id] from a count/index accessor pair on a PartitionReassignment.
static PyObject* reassignment_ids_to_py(const kafka_admin_PartitionReassignment_t* pr, int32_t count,
                                        int32_t (*get)(const kafka_admin_PartitionReassignment_t*,
                                                       int32_t)) {
    PyObject* out = PyList_New(count < 0 ? 0 : count);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < count; i++) {
        PyObject* id = PyLong_FromLong(get(pr, i));
        if (id == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, id);
    }
    return out;
}

// {(topic, partition): (replicas, adding_replicas, removing_replicas)}
//
// Java holds one future for the whole listing, so there is no per-key error.
static PyObject* py_ListPartitionReassignmentsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListPartitionReassignmentsResult_t* r =
        (kafka_admin_ListPartitionReassignmentsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ListPartitionReassignmentsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_ListPartitionReassignmentsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = topic_partition_key(
            kafka_admin_ListPartitionReassignmentsResult_get_topic(r, i),
            kafka_admin_ListPartitionReassignmentsResult_get_partition(r, i));
        const kafka_admin_PartitionReassignment_t* pr =
            kafka_admin_ListPartitionReassignmentsResult_get_value(r, i);
        PyObject* replicas = pr ? reassignment_ids_to_py(
            pr, kafka_admin_PartitionReassignment_replica_count(pr),
            kafka_admin_PartitionReassignment_replica) : NULL;
        PyObject* adding = pr ? reassignment_ids_to_py(
            pr, kafka_admin_PartitionReassignment_adding_replica_count(pr),
            kafka_admin_PartitionReassignment_adding_replica) : NULL;
        PyObject* removing = pr ? reassignment_ids_to_py(
            pr, kafka_admin_PartitionReassignment_removing_replica_count(pr),
            kafka_admin_PartitionReassignment_removing_replica) : NULL;
        // 'N' steals even on failure, so only build when all three are live.
        PyObject* val = NULL;
        if (replicas && adding && removing) {
            val = Py_BuildValue("(NNN)", replicas, adding, removing);
        } else {
            Py_XDECREF(replicas); Py_XDECREF(adding); Py_XDECREF(removing);
        }
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_ListPartitionReassignmentsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_ListPartitionReassignmentsResult_destroy(r);
    return d;
}

// {(topic, partition): (error, (offset, timestamp, leader_epoch_or_None))}
static PyObject* py_ListOffsetsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListOffsetsResult_t* r = (kafka_admin_ListOffsetsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ListOffsetsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_ListOffsetsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = topic_partition_key(kafka_admin_ListOffsetsResult_get_topic(r, i),
                                            kafka_admin_ListOffsetsResult_get_partition(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_ListOffsetsResult_get_error(r, i));
        const kafka_admin_ListOffsetsResultInfo_t* info =
            kafka_admin_ListOffsetsResult_get_value(r, i);
        PyObject* value;
        if (info == NULL) {
            Py_INCREF(Py_None);
            value = Py_None;
        } else {
            int32_t epoch = 0;
            // Java's leaderEpoch() is Optional<Integer>: absent stays None.
            bool has_epoch = kafka_admin_ListOffsetsResultInfo_leader_epoch(info, &epoch);
            value = has_epoch
                ? Py_BuildValue("(LLi)",
                                (long long)kafka_admin_ListOffsetsResultInfo_offset(info),
                                (long long)kafka_admin_ListOffsetsResultInfo_timestamp(info),
                                epoch)
                : Py_BuildValue("(LLO)",
                                (long long)kafka_admin_ListOffsetsResultInfo_offset(info),
                                (long long)kafka_admin_ListOffsetsResultInfo_timestamp(info),
                                Py_None);
        }
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_ListOffsetsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_ListOffsetsResult_destroy(r);
    return d;
}

// ---------------------------------------------------------------------------
// B4 — groups and group offsets
// ---------------------------------------------------------------------------

static void admin_list_groups_trampoline(kafka_admin_ListGroupsResult_t* r,
                                         kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_consumer_groups_trampoline(kafka_admin_DescribeConsumerGroupsResult_t* r,
                                                      kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_classic_groups_trampoline(kafka_admin_DescribeClassicGroupsResult_t* r,
                                                     kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_consumer_group_offsets_trampoline(kafka_admin_ListConsumerGroupOffsetsResult_t* r,
                                                         kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_consumer_group_offsets_trampoline(kafka_admin_AlterConsumerGroupOffsetsResult_t* r,
                                                          kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_consumer_group_offsets_trampoline(kafka_admin_DeleteConsumerGroupOffsetsResult_t* r,
                                                           kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_consumer_groups_trampoline(kafka_admin_DeleteConsumerGroupsResult_t* r,
                                                    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_remove_members_trampoline(kafka_admin_RemoveMembersFromConsumerGroupResult_t* r,
                                            kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }

// ---- shared string-array reader --------------------------------------------

// Reads a sequence of str into a PyMem_Malloc'd `const char*` array (caller
// frees). The pointers borrow from the sequence, which the caller must keep
// alive across the FFI call. Returns the count, or -1 with an exception set.
static Py_ssize_t build_string_array(PyObject* seq, const char*** out) {
    Py_ssize_t n = PySequence_Size(seq);
    if (n < 0) return -1;
    const char** items = PyMem_Malloc((size_t)(n > 0 ? n : 1) * sizeof(char*));
    if (items == NULL) { PyErr_NoMemory(); return -1; }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(seq, i);  // new ref
        const char* text = item ? PyUnicode_AsUTF8(item) : NULL;
        Py_XDECREF(item);
        if (text == NULL) { PyMem_Free(items); return -1; }
        items[i] = text;
    }
    *out = items;
    return n;
}

// ---- submits ---------------------------------------------------------------

static PyObject* py_Admin_list_groups_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* states; PyObject* protocols; PyObject* types;
    int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOOOiO", &h, &states, &protocols, &types, &timeout_ms, &cb))
        return NULL;

    const char** s = NULL; const char** p = NULL; const char** t = NULL;
    Py_ssize_t ns = build_string_array(states, &s);
    if (ns < 0) return NULL;
    Py_ssize_t np = build_string_array(protocols, &p);
    if (np < 0) { PyMem_Free(s); return NULL; }
    Py_ssize_t nt = build_string_array(types, &t);
    if (nt < 0) { PyMem_Free(s); PyMem_Free(p); return NULL; }

    Py_INCREF(cb);
    kafka_admin_AdminClient_list_groups_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, s, (int32_t)ns, p, (int32_t)np, t, (int32_t)nt,
        timeout_ms, admin_list_groups_trampoline, cb);
    PyMem_Free(s); PyMem_Free(p); PyMem_Free(t);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_consumer_groups_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* groups; int timeout_ms; int include_authorized; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &groups, &timeout_ms, &include_authorized, &cb))
        return NULL;

    const char** ids = NULL;
    Py_ssize_t n = build_string_array(groups, &ids);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_consumer_groups_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, ids, (int32_t)n, timeout_ms,
        include_authorized ? true : false, admin_describe_consumer_groups_trampoline, cb);
    PyMem_Free(ids);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_classic_groups_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* groups; int timeout_ms; int include_authorized; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &groups, &timeout_ms, &include_authorized, &cb))
        return NULL;

    const char** ids = NULL;
    Py_ssize_t n = build_string_array(groups, &ids);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_classic_groups_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, ids, (int32_t)n, timeout_ms,
        include_authorized ? true : false, admin_describe_classic_groups_trampoline, cb);
    PyMem_Free(ids);
    Py_RETURN_NONE;
}

// spec is a sequence of (group_id:str, all_partitions:bool,
// [(topic:str, partition:int), ...]) — one ragged partition list per group.
static PyObject* py_Admin_list_consumer_group_offsets_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int timeout_ms; int require_stable; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &spec, &timeout_ms, &require_stable, &cb))
        return NULL;

    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** groups = PyMem_Malloc(slots * sizeof(char*));
    bool* all_partitions = PyMem_Malloc(slots * sizeof(bool));
    const char*** topics = PyMem_Malloc(slots * sizeof(char**));
    int32_t** partitions = PyMem_Malloc(slots * sizeof(int32_t*));
    int32_t* counts = PyMem_Malloc(slots * sizeof(int32_t));
    if (!groups || !all_partitions || !topics || !partitions || !counts) {
        PyMem_Free(groups); PyMem_Free(all_partitions); PyMem_Free(topics);
        PyMem_Free(partitions); PyMem_Free(counts);
        PyErr_NoMemory(); return NULL;
    }

    // `built` counts fully populated rows; each row's two pointers are NULLed
    // before any fallible work, so the cleanup loop never frees a stale value.
    Py_ssize_t built = 0;
    int failed = 0;
    for (; built < n; built++) {
        topics[built] = NULL; partitions[built] = NULL;
        PyObject* item = PySequence_GetItem(spec, built);  // new ref
        const char* group = NULL; int all = 0; PyObject* tps = NULL;
        int ok = item && PyArg_ParseTuple(item, "spO", &group, &all, &tps);
        if (ok) {
            groups[built] = group;
            all_partitions[built] = all ? true : false;
            const char** row_topics = NULL; int32_t* row_partitions = NULL;
            Py_ssize_t rows = build_topic_partitions(tps, &row_topics, &row_partitions);
            if (rows < 0) {
                ok = 0;
            } else {
                topics[built] = row_topics;
                partitions[built] = row_partitions;
                counts[built] = (int32_t)rows;
            }
        }
        Py_XDECREF(item);
        if (!ok) { failed = 1; break; }
    }

    if (!failed) {
        Py_INCREF(cb);
        kafka_admin_AdminClient_list_consumer_group_offsets_async(
            (kafka_admin_AdminClient_t*)(uintptr_t)h, groups, all_partitions,
            (const char* const* const*)topics, (const int32_t* const*)partitions, counts,
            (int32_t)n, timeout_ms, require_stable ? true : false,
            admin_list_consumer_group_offsets_trampoline, cb);
    }
    for (Py_ssize_t i = 0; i < built; i++) {
        PyMem_Free((void*)topics[i]); PyMem_Free(partitions[i]);
    }
    PyMem_Free(groups); PyMem_Free(all_partitions); PyMem_Free(topics);
    PyMem_Free(partitions); PyMem_Free(counts);
    if (failed) return NULL;
    Py_RETURN_NONE;
}

// spec is a sequence of
// (topic:str, partition:int, offset:int, metadata:str|None,
//  has_leader_epoch:bool, leader_epoch:int).
static PyObject* py_Admin_alter_consumer_group_offsets_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* group_id; PyObject* spec; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsOiO", &h, &group_id, &spec, &timeout_ms, &cb)) return NULL;

    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** topics = PyMem_Malloc(slots * sizeof(char*));
    int32_t* partitions = PyMem_Malloc(slots * sizeof(int32_t));
    int64_t* offsets = PyMem_Malloc(slots * sizeof(int64_t));
    const char** metadata = PyMem_Malloc(slots * sizeof(char*));
    int32_t* epochs = PyMem_Malloc(slots * sizeof(int32_t));
    bool* has_epoch = PyMem_Malloc(slots * sizeof(bool));
    if (!topics || !partitions || !offsets || !metadata || !epochs || !has_epoch) {
        PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
        PyMem_Free(metadata); PyMem_Free(epochs); PyMem_Free(has_epoch);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* topic = NULL; int p = 0; long long offset = 0;
        const char* meta = NULL; int has = 0; int epoch = 0;
        int ok = item && PyArg_ParseTuple(item, "siLzpi", &topic, &p, &offset, &meta, &has, &epoch);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
            PyMem_Free(metadata); PyMem_Free(epochs); PyMem_Free(has_epoch);
            return NULL;
        }
        topics[i] = topic; partitions[i] = (int32_t)p; offsets[i] = (int64_t)offset;
        metadata[i] = meta; epochs[i] = (int32_t)epoch; has_epoch[i] = has ? true : false;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_alter_consumer_group_offsets_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, group_id, topics, partitions, offsets, metadata,
        epochs, has_epoch, (int32_t)n, timeout_ms, admin_alter_consumer_group_offsets_trampoline,
        cb);
    PyMem_Free(topics); PyMem_Free(partitions); PyMem_Free(offsets);
    PyMem_Free(metadata); PyMem_Free(epochs); PyMem_Free(has_epoch);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_delete_consumer_group_offsets_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* group_id; PyObject* spec; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsOiO", &h, &group_id, &spec, &timeout_ms, &cb)) return NULL;

    const char** topics = NULL; int32_t* partitions = NULL;
    Py_ssize_t n = build_topic_partitions(spec, &topics, &partitions);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_delete_consumer_group_offsets_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, group_id, topics, partitions, (int32_t)n,
        timeout_ms, admin_delete_consumer_group_offsets_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(partitions);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_delete_consumer_groups_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* groups; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &groups, &timeout_ms, &cb)) return NULL;

    const char** ids = NULL;
    Py_ssize_t n = build_string_array(groups, &ids);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_delete_consumer_groups_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, ids, (int32_t)n, timeout_ms,
        admin_delete_consumer_groups_trampoline, cb);
    PyMem_Free(ids);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_remove_members_from_consumer_group_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* group_id; int remove_all; PyObject* members;
    const char* reason; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KspOziO", &h, &group_id, &remove_all, &members, &reason,
                          &timeout_ms, &cb))
        return NULL;

    const char** ids = NULL;
    Py_ssize_t n = build_string_array(members, &ids);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_remove_members_from_consumer_group_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, group_id, remove_all ? true : false, ids,
        (int32_t)n, reason, timeout_ms, admin_remove_members_trampoline, cb);
    PyMem_Free(ids);
    Py_RETURN_NONE;
}

// ---- result drains ---------------------------------------------------------

// A `const char*` that may be NULL becomes None rather than "".
static PyObject* optional_str_to_py(const char* text) {
    if (text == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(text);
}

// [(topic, partition), ...] for a MemberAssignment.
static PyObject* member_assignment_to_py(const kafka_admin_MemberAssignment_t* a) {
    if (a == NULL) Py_RETURN_NONE;
    int32_t n = kafka_admin_MemberAssignment_count(a);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* tp = topic_partition_key(kafka_admin_MemberAssignment_get_topic(a, i),
                                           kafka_admin_MemberAssignment_get_partition(a, i));
        if (tp == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, tp);
    }
    return out;
}

// (consumer_id, group_instance_id, rack_id, client_id, host, assignment,
//  target_assignment, member_epoch, upgraded)
static PyObject* member_description_to_py(const kafka_admin_MemberDescription_t* m) {
    if (m == NULL) Py_RETURN_NONE;
    PyObject* gid = optional_str_to_py(kafka_admin_MemberDescription_group_instance_id(m));
    PyObject* rack = optional_str_to_py(kafka_admin_MemberDescription_rack_id(m));
    PyObject* assignment = member_assignment_to_py(kafka_admin_MemberDescription_assignment(m));
    PyObject* target =
        member_assignment_to_py(kafka_admin_MemberDescription_target_assignment(m));
    int32_t epoch = 0;
    PyObject* py_epoch = kafka_admin_MemberDescription_member_epoch(m, &epoch)
                             ? PyLong_FromLong(epoch)
                             : (Py_INCREF(Py_None), Py_None);
    bool upgraded = false;
    PyObject* py_upgraded = kafka_admin_MemberDescription_upgraded(m, &upgraded)
                                ? PyBool_FromLong(upgraded ? 1 : 0)
                                : (Py_INCREF(Py_None), Py_None);
    if (!gid || !rack || !assignment || !target || !py_epoch || !py_upgraded) {
        Py_XDECREF(gid); Py_XDECREF(rack); Py_XDECREF(assignment); Py_XDECREF(target);
        Py_XDECREF(py_epoch); Py_XDECREF(py_upgraded);
        return NULL;
    }
    return Py_BuildValue("(sNNssNNNN)", kafka_admin_MemberDescription_consumer_id(m), gid, rack,
                         kafka_admin_MemberDescription_client_id(m),
                         kafka_admin_MemberDescription_host(m), assignment, target, py_epoch,
                         py_upgraded);
}

static int32_t consumer_group_acl_at(const void* d, int32_t i) {
    return kafka_admin_ConsumerGroupDescription_authorized_operation(
        (const kafka_admin_ConsumerGroupDescription_t*)d, i);
}

static int32_t classic_group_acl_at(const void* d, int32_t i) {
    return kafka_admin_ClassicGroupDescription_authorized_operation(
        (const kafka_admin_ClassicGroupDescription_t*)d, i);
}

// [member_tuple, ...] from a count/index accessor pair.
static PyObject* members_to_py(int32_t count,
                               const kafka_admin_MemberDescription_t* (*get)(const void*, int32_t),
                               const void* owner) {
    PyObject* out = PyList_New(count < 0 ? 0 : count);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < count; i++) {
        PyObject* m = member_description_to_py(get(owner, i));
        if (m == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, m);
    }
    return out;
}

static const kafka_admin_MemberDescription_t* consumer_group_member_at(const void* d, int32_t i) {
    return kafka_admin_ConsumerGroupDescription_get_member(
        (const kafka_admin_ConsumerGroupDescription_t*)d, i);
}

static const kafka_admin_MemberDescription_t* classic_group_member_at(const void* d, int32_t i) {
    return kafka_admin_ClassicGroupDescription_get_member(
        (const kafka_admin_ClassicGroupDescription_t*)d, i);
}

// (group_id, is_simple, members, partition_assignor, group_type,
//  group_state, coordinator, authorized_operations, group_epoch,
//  target_assignment_epoch)
static PyObject* consumer_group_description_to_py(const kafka_admin_ConsumerGroupDescription_t* d) {
    if (d == NULL) Py_RETURN_NONE;
    PyObject* members =
        members_to_py(kafka_admin_ConsumerGroupDescription_member_count(d),
                      consumer_group_member_at, d);
    PyObject* coordinator = node_to_py(kafka_admin_ConsumerGroupDescription_coordinator(d));
    PyObject* acls =
        acl_codes_to_py(kafka_admin_ConsumerGroupDescription_has_authorized_operations(d),
                        kafka_admin_ConsumerGroupDescription_authorized_operation_count(d),
                        consumer_group_acl_at, d);
    int32_t epoch = 0;
    PyObject* group_epoch = kafka_admin_ConsumerGroupDescription_group_epoch(d, &epoch)
                                ? PyLong_FromLong(epoch)
                                : (Py_INCREF(Py_None), Py_None);
    int32_t target = 0;
    PyObject* target_epoch =
        kafka_admin_ConsumerGroupDescription_target_assignment_epoch(d, &target)
            ? PyLong_FromLong(target)
            : (Py_INCREF(Py_None), Py_None);
    if (!members || !coordinator || !acls || !group_epoch || !target_epoch) {
        Py_XDECREF(members); Py_XDECREF(coordinator); Py_XDECREF(acls);
        Py_XDECREF(group_epoch); Py_XDECREF(target_epoch);
        return NULL;
    }
    return Py_BuildValue(
        // Ten format units for ten arguments, in the order
        // `_to_consumer_group_description` unpacks them:
        //   s     O          N        s                   s
        //   group is_simple  members  partition_assignor  group_type
        //   s            N            N     N            N
        //   group_state  coordinator  acls  group_epoch  target_epoch
        // Worth counting by hand: no test can reach this branch, because Java's
        // own MockAdminClient throws for describeConsumerGroups, so a wrong
        // arity would surface only against a real broker.
        "(sONsssNNNN)", kafka_admin_ConsumerGroupDescription_group_id(d),
        kafka_admin_ConsumerGroupDescription_is_simple_consumer_group(d) ? Py_True : Py_False,
        members, kafka_admin_ConsumerGroupDescription_partition_assignor(d),
        kafka_admin_ConsumerGroupDescription_group_type(d),
        kafka_admin_ConsumerGroupDescription_group_state(d), coordinator, acls, group_epoch,
        target_epoch);
}

// (group_id, protocol, protocol_data, is_simple, members, state, coordinator,
//  authorized_operations)
static PyObject* classic_group_description_to_py(const kafka_admin_ClassicGroupDescription_t* d) {
    if (d == NULL) Py_RETURN_NONE;
    PyObject* members = members_to_py(kafka_admin_ClassicGroupDescription_member_count(d),
                                      classic_group_member_at, d);
    PyObject* coordinator = node_to_py(kafka_admin_ClassicGroupDescription_coordinator(d));
    PyObject* acls =
        acl_codes_to_py(kafka_admin_ClassicGroupDescription_has_authorized_operations(d),
                        kafka_admin_ClassicGroupDescription_authorized_operation_count(d),
                        classic_group_acl_at, d);
    if (!members || !coordinator || !acls) {
        Py_XDECREF(members); Py_XDECREF(coordinator); Py_XDECREF(acls);
        return NULL;
    }
    return Py_BuildValue("(sssONsNN)", kafka_admin_ClassicGroupDescription_group_id(d),
                         kafka_admin_ClassicGroupDescription_protocol(d),
                         kafka_admin_ClassicGroupDescription_protocol_data(d),
                         kafka_admin_ClassicGroupDescription_is_simple_consumer_group(d) ? Py_True
                                                                                        : Py_False,
                         members, kafka_admin_ClassicGroupDescription_state(d), coordinator, acls);
}

// ([(group_id, group_type, protocol, group_state, is_simple), ...], [error, ...])
//
// Java's ListGroupsResult has no per-key future: valid() and errors() are two
// independent collections of generally different length, so this is a pair of
// lists rather than a dict.
static PyObject* py_ListGroupsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListGroupsResult_t* r = (kafka_admin_ListGroupsResult_t*)(uintptr_t)ptr;
    int32_t nv = kafka_admin_ListGroupsResult_valid_count(r);
    int32_t ne = kafka_admin_ListGroupsResult_error_count(r);
    PyObject* valid = PyList_New(nv < 0 ? 0 : nv);
    PyObject* errors = PyList_New(ne < 0 ? 0 : ne);
    if (!valid || !errors) goto fail;
    for (int32_t i = 0; i < nv; i++) {
        const kafka_admin_GroupListing_t* g = kafka_admin_ListGroupsResult_get_valid(r, i);
        PyObject* type = optional_str_to_py(kafka_admin_GroupListing_group_type(g));
        PyObject* state = optional_str_to_py(kafka_admin_GroupListing_group_state(g));
        if (!type || !state) { Py_XDECREF(type); Py_XDECREF(state); goto fail; }
        PyObject* row = Py_BuildValue("(sNsNO)", kafka_admin_GroupListing_group_id(g), type,
                                      kafka_admin_GroupListing_protocol(g), state,
                                      kafka_admin_GroupListing_is_simple_consumer_group(g)
                                          ? Py_True : Py_False);
        if (row == NULL) goto fail;
        PyList_SET_ITEM(valid, i, row);
    }
    for (int32_t i = 0; i < ne; i++) {
        PyObject* e = borrowed_error_to_py(kafka_admin_ListGroupsResult_get_error(r, i));
        if (e == NULL) goto fail;
        PyList_SET_ITEM(errors, i, e);
    }
    kafka_admin_ListGroupsResult_destroy(r);
    return Py_BuildValue("(NN)", valid, errors);
fail:
    Py_XDECREF(valid); Py_XDECREF(errors);
    kafka_admin_ListGroupsResult_destroy(r);
    return NULL;
}

// {group_id: (error, description_or_None)}
static PyObject* py_DescribeConsumerGroupsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeConsumerGroupsResult_t* r =
        (kafka_admin_DescribeConsumerGroupsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeConsumerGroupsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeConsumerGroupsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(
            kafka_admin_DescribeConsumerGroupsResult_get_group_id(r, i));
        PyObject* err =
            borrowed_error_to_py(kafka_admin_DescribeConsumerGroupsResult_get_error(r, i));
        PyObject* value = consumer_group_description_to_py(
            kafka_admin_DescribeConsumerGroupsResult_get_value(r, i));
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_DescribeConsumerGroupsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_DescribeConsumerGroupsResult_destroy(r);
    return d;
}

// {group_id: (error, description_or_None)}
static PyObject* py_DescribeClassicGroupsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeClassicGroupsResult_t* r =
        (kafka_admin_DescribeClassicGroupsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeClassicGroupsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeClassicGroupsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key =
            PyUnicode_FromString(kafka_admin_DescribeClassicGroupsResult_get_group_id(r, i));
        PyObject* err =
            borrowed_error_to_py(kafka_admin_DescribeClassicGroupsResult_get_error(r, i));
        PyObject* value = classic_group_description_to_py(
            kafka_admin_DescribeClassicGroupsResult_get_value(r, i));
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_DescribeClassicGroupsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_DescribeClassicGroupsResult_destroy(r);
    return d;
}

// {(topic, partition): (offset, metadata, leader_epoch) | None}
//
// A None value is Java's null map value: the group has no committed offset for
// that partition, which is distinct from a committed offset of 0.
static PyObject* offset_map_to_py(const kafka_admin_OffsetAndMetadataMap_t* map) {
    if (map == NULL) Py_RETURN_NONE;
    int32_t n = kafka_admin_OffsetAndMetadataMap_count(map);
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = topic_partition_key(kafka_admin_OffsetAndMetadataMap_get_topic(map, i),
                                            kafka_admin_OffsetAndMetadataMap_get_partition(map, i));
        PyObject* value;
        if (!kafka_admin_OffsetAndMetadataMap_has_offset(map, i)) {
            Py_INCREF(Py_None);
            value = Py_None;
        } else {
            int32_t epoch = 0;
            bool has_epoch =
                kafka_admin_OffsetAndMetadataMap_get_leader_epoch(map, i, &epoch);
            PyObject* py_epoch =
                has_epoch ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None);
            value = py_epoch == NULL
                        ? NULL
                        : Py_BuildValue(
                              "(LsN)",
                              (long long)kafka_admin_OffsetAndMetadataMap_get_offset(map, i),
                              kafka_admin_OffsetAndMetadataMap_get_metadata(map, i), py_epoch);
        }
        if (!key || !value || PyDict_SetItem(d, key, value) < 0) {
            Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d); return NULL;
        }
        Py_DECREF(key); Py_DECREF(value);
    }
    return d;
}

// {group_id: (error, {(topic, partition): offset_tuple | None} | None)}
static PyObject* py_ListConsumerGroupOffsetsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListConsumerGroupOffsetsResult_t* r =
        (kafka_admin_ListConsumerGroupOffsetsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ListConsumerGroupOffsetsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_ListConsumerGroupOffsetsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(
            kafka_admin_ListConsumerGroupOffsetsResult_get_group_id(r, i));
        PyObject* err =
            borrowed_error_to_py(kafka_admin_ListConsumerGroupOffsetsResult_get_error(r, i));
        PyObject* value =
            offset_map_to_py(kafka_admin_ListConsumerGroupOffsetsResult_get_value(r, i));
        PyObject* val = error_value_pair(err, value);
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_admin_ListConsumerGroupOffsetsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_admin_ListConsumerGroupOffsetsResult_destroy(r);
    return d;
}

// {(topic, partition): error_or_None} — per-partition future is
// KafkaFuture<Void>, so None means success.
static PyObject* py_AlterConsumerGroupOffsetsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_AlterConsumerGroupOffsetsResult_t* r =
        (kafka_admin_AlterConsumerGroupOffsetsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_AlterConsumerGroupOffsetsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_AlterConsumerGroupOffsetsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = topic_partition_key(
            kafka_admin_AlterConsumerGroupOffsetsResult_get_topic(r, i),
            kafka_admin_AlterConsumerGroupOffsetsResult_get_partition(r, i));
        PyObject* err =
            borrowed_error_to_py(kafka_admin_AlterConsumerGroupOffsetsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_AlterConsumerGroupOffsetsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_AlterConsumerGroupOffsetsResult_destroy(r);
    return d;
}

// {(topic, partition): error_or_None}
static PyObject* py_DeleteConsumerGroupOffsetsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DeleteConsumerGroupOffsetsResult_t* r =
        (kafka_admin_DeleteConsumerGroupOffsetsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DeleteConsumerGroupOffsetsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = topic_partition_key(
            kafka_admin_DeleteConsumerGroupOffsetsResult_get_topic(r, i),
            kafka_admin_DeleteConsumerGroupOffsetsResult_get_partition(r, i));
        PyObject* err =
            borrowed_error_to_py(kafka_admin_DeleteConsumerGroupOffsetsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_DeleteConsumerGroupOffsetsResult_destroy(r);
    return d;
}

// {group_id: error_or_None}
static PyObject* py_DeleteConsumerGroupsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DeleteConsumerGroupsResult_t* r =
        (kafka_admin_DeleteConsumerGroupsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DeleteConsumerGroupsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DeleteConsumerGroupsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key =
            PyUnicode_FromString(kafka_admin_DeleteConsumerGroupsResult_get_group_id(r, i));
        PyObject* err =
            borrowed_error_to_py(kafka_admin_DeleteConsumerGroupsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_DeleteConsumerGroupsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_DeleteConsumerGroupsResult_destroy(r);
    return d;
}

// {group_instance_id: error_or_None} — empty in removeAll mode, where Java
// exposes no per-member outcome at all.
static PyObject* py_RemoveMembersFromConsumerGroupResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_RemoveMembersFromConsumerGroupResult_t* r =
        (kafka_admin_RemoveMembersFromConsumerGroupResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_RemoveMembersFromConsumerGroupResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(
            kafka_admin_RemoveMembersFromConsumerGroupResult_get_group_instance_id(r, i));
        PyObject* err = borrowed_error_to_py(
            kafka_admin_RemoveMembersFromConsumerGroupResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_RemoveMembersFromConsumerGroupResult_destroy(r);
    return d;
}


// ---------------------------------------------------------------------------
// B5a — ACLs and client quotas
//
// Java's MockAdminClient throws for all five RPCs, so the success branch of
// every drain below is unreachable from the test suite. Their Py_BuildValue
// arity is instead checked statically by `test/static/test_format_arity.py`, and the
// field *order* by review against the matching `_to_*` unpacker in admin.py.
// ---------------------------------------------------------------------------

static void admin_create_acls_trampoline(kafka_admin_CreateAclsResult_t* r,
                                         kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_acls_trampoline(kafka_admin_DescribeAclsResult_t* r,
                                           kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_acls_trampoline(kafka_admin_DeleteAclsResult_t* r,
                                         kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_client_quotas_trampoline(kafka_admin_DescribeClientQuotasResult_t* r,
                                                    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_client_quotas_trampoline(kafka_admin_AlterClientQuotasResult_t* r,
                                                 kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }

// ---- ACL request marshaling -------------------------------------------------

// The seven parallel arrays an ACL binding or filter request needs. Held
// together so the seven allocations have one owner and one cleanup path.
typedef struct {
    int32_t* resource_types;
    const char** resource_names;
    int32_t* pattern_types;
    const char** principals;
    const char** hosts;
    int32_t* operations;
    int32_t* permission_types;
    Py_ssize_t count;
} acl_arrays_t;

static void acl_arrays_free(acl_arrays_t* a) {
    PyMem_Free(a->resource_types); PyMem_Free((void*)a->resource_names);
    PyMem_Free(a->pattern_types); PyMem_Free((void*)a->principals);
    PyMem_Free((void*)a->hosts); PyMem_Free(a->operations);
    PyMem_Free(a->permission_types);
    memset(a, 0, sizeof(*a));
}

// Reads a sequence of 7-tuples into `a`. `nullable` selects the filter form,
// whose resource name, principal and host may be None (Java's null = match
// any); the binding form requires all three. Returns 0 on success, -1 with an
// exception set. The `const char*`s borrow from `seq`, which the caller must
// keep alive across the FFI call.
static int build_acl_arrays(PyObject* seq, int nullable, acl_arrays_t* a) {
    memset(a, 0, sizeof(*a));
    Py_ssize_t n = PySequence_Size(seq);
    if (n < 0) return -1;
    size_t slots = (size_t)(n > 0 ? n : 1);
    a->resource_types = PyMem_Malloc(slots * sizeof(int32_t));
    a->resource_names = PyMem_Malloc(slots * sizeof(char*));
    a->pattern_types = PyMem_Malloc(slots * sizeof(int32_t));
    a->principals = PyMem_Malloc(slots * sizeof(char*));
    a->hosts = PyMem_Malloc(slots * sizeof(char*));
    a->operations = PyMem_Malloc(slots * sizeof(int32_t));
    a->permission_types = PyMem_Malloc(slots * sizeof(int32_t));
    if (!a->resource_types || !a->resource_names || !a->pattern_types || !a->principals ||
        !a->hosts || !a->operations || !a->permission_types) {
        acl_arrays_free(a);
        PyErr_NoMemory();
        return -1;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(seq, i);  // new ref
        int rt = 0, pt = 0, op = 0, pm = 0;
        const char* name = NULL; const char* principal = NULL; const char* host = NULL;
        // Two literal formats rather than one conditional expression: a
        // non-literal format is unverifiable by `test/static/test_format_arity.py`,
        // and this is precisely the call shape that gate exists to guard.
        int ok = item != NULL;
        if (ok) {
            ok = nullable
                     ? PyArg_ParseTuple(item, "izizzii", &rt, &name, &pt, &principal, &host, &op,
                                        &pm)
                     : PyArg_ParseTuple(item, "isissii", &rt, &name, &pt, &principal, &host, &op,
                                        &pm);
        }
        Py_XDECREF(item);
        if (!ok) { acl_arrays_free(a); return -1; }
        a->resource_types[i] = (int32_t)rt;
        a->resource_names[i] = name;
        a->pattern_types[i] = (int32_t)pt;
        a->principals[i] = principal;
        a->hosts[i] = host;
        a->operations[i] = (int32_t)op;
        a->permission_types[i] = (int32_t)pm;
    }
    a->count = n;
    return 0;
}

// (resource_type, resource_name, pattern_type, principal, host, operation,
//  permission_type) — the field order `_to_acl_binding` unpacks.
// The binding keeps Java's object graph, so the fields are read off its
// `pattern()` and `entry()`; each enum is passed on as its Java code, which
// is what the Python side's int-based enum classes expect.
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
// any") becomes None rather than crashing on PyUnicode_FromString(NULL).
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

// [(entity_type, entity_name_or_None)] — a None name is Java's null map value,
// the built-in default entity, which is not the empty name.
static PyObject* client_quota_entity_to_py(const kafka_common_quota_ClientQuotaEntity_t* e) {
    if (e == NULL) Py_RETURN_NONE;
    // `entries()` is an owned map of entity type -> entity name (NULL for the
    // default entity), sorted by type; destroyed once copied out.
    kafka_Map_t* entries = kafka_common_quota_ClientQuotaEntity_entries(e);
    int32_t n = kafka_Map_size(entries);
    PyObject* pairs = PyTuple_New(n < 0 ? 0 : n);
    if (pairs == NULL) { kafka_Map_destroy(entries); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* pair = Py_BuildValue("(sz)",
                                       (const char*)kafka_Map_key(entries, i),
                                       (const char*)kafka_Map_value(entries, i));
        if (pair == NULL) { Py_DECREF(pairs); kafka_Map_destroy(entries); return NULL; }
        PyTuple_SET_ITEM(pairs, i, pair);
    }
    kafka_Map_destroy(entries);
    return pairs;
}

// ---- submits ----------------------------------------------------------------

static PyObject* py_Admin_create_acls_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* acls; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &acls, &timeout_ms, &cb)) return NULL;

    acl_arrays_t a;
    if (build_acl_arrays(acls, 0, &a) < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_create_acls_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, a.resource_types, a.resource_names,
        a.pattern_types, a.principals, a.hosts, a.operations, a.permission_types,
        (int32_t)a.count, timeout_ms, admin_create_acls_trampoline, cb);
    acl_arrays_free(&a);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_acls_async(PyObject* self, PyObject* args) {
    unsigned long long h; int rt, pt, op, pm; int timeout_ms; PyObject* cb;
    const char* name = NULL; const char* principal = NULL; const char* host = NULL;
    if (!PyArg_ParseTuple(args, "KizizziiiO", &h, &rt, &name, &pt, &principal, &host, &op, &pm,
                          &timeout_ms, &cb))
        return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_acls_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, (int32_t)rt, name, (int32_t)pt, principal, host,
        (int32_t)op, (int32_t)pm, timeout_ms, admin_describe_acls_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_delete_acls_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* filters; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &filters, &timeout_ms, &cb)) return NULL;

    acl_arrays_t a;
    if (build_acl_arrays(filters, 1, &a) < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_delete_acls_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, a.resource_types, a.resource_names,
        a.pattern_types, a.principals, a.hosts, a.operations, a.permission_types,
        (int32_t)a.count, timeout_ms, admin_delete_acls_trampoline, cb);
    acl_arrays_free(&a);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_client_quotas_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* components; int strict; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOpiO", &h, &components, &strict, &timeout_ms, &cb)) return NULL;

    Py_ssize_t n = PySequence_Size(components);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** types = PyMem_Malloc(slots * sizeof(char*));
    int32_t* match_types = PyMem_Malloc(slots * sizeof(int32_t));
    const char** names = PyMem_Malloc(slots * sizeof(char*));
    if (!types || !match_types || !names) {
        PyMem_Free((void*)types); PyMem_Free(match_types); PyMem_Free((void*)names);
        PyErr_NoMemory(); return NULL;
    }
    int failed = 0;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(components, i);  // new ref
        const char* type = NULL; int match_type = 0; const char* name = NULL;
        int ok = item && PyArg_ParseTuple(item, "siz", &type, &match_type, &name);
        Py_XDECREF(item);
        if (!ok) { failed = 1; break; }
        types[i] = type;
        match_types[i] = (int32_t)match_type;
        names[i] = name;
    }
    if (!failed) {
        Py_INCREF(cb);
        kafka_admin_AdminClient_describe_client_quotas_async(
            (kafka_admin_AdminClient_t*)(uintptr_t)h, types, match_types, names, (int32_t)n,
            strict ? true : false, timeout_ms, admin_describe_client_quotas_trampoline, cb);
    }
    PyMem_Free((void*)types); PyMem_Free(match_types); PyMem_Free((void*)names);
    if (failed) return NULL;
    Py_RETURN_NONE;
}

static PyObject* py_Admin_alter_client_quotas_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* entries; int timeout_ms; int validate_only; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &entries, &timeout_ms, &validate_only, &cb))
        return NULL;

    Py_ssize_t n = PySequence_Size(entries);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char*** entity_types = PyMem_Malloc(slots * sizeof(char**));
    const char*** entity_names = PyMem_Malloc(slots * sizeof(char**));
    int32_t* entity_counts = PyMem_Malloc(slots * sizeof(int32_t));
    const char*** op_keys = PyMem_Malloc(slots * sizeof(char**));
    double** op_values = PyMem_Malloc(slots * sizeof(double*));
    bool** op_has_values = PyMem_Malloc(slots * sizeof(bool*));
    int32_t* op_counts = PyMem_Malloc(slots * sizeof(int32_t));
    if (!entity_types || !entity_names || !entity_counts || !op_keys || !op_values ||
        !op_has_values || !op_counts) {
        PyMem_Free((void*)entity_types); PyMem_Free((void*)entity_names);
        PyMem_Free(entity_counts); PyMem_Free((void*)op_keys); PyMem_Free(op_values);
        PyMem_Free(op_has_values); PyMem_Free(op_counts);
        PyErr_NoMemory(); return NULL;
    }

    // `built` counts fully populated rows; every row's five pointers are NULLed
    // before any fallible work, so the cleanup loop never frees a stale value.
    Py_ssize_t built = 0;
    int failed = 0;
    for (; built < n; built++) {
        entity_types[built] = NULL; entity_names[built] = NULL;
        op_keys[built] = NULL; op_values[built] = NULL; op_has_values[built] = NULL;
        PyObject* item = PySequence_GetItem(entries, built);  // new ref
        PyObject* pairs = NULL; PyObject* ops = NULL;
        int ok = item && PyArg_ParseTuple(item, "OO", &pairs, &ops);
        if (ok) {
            Py_ssize_t np = PySequence_Size(pairs);
            Py_ssize_t no = ops == Py_None ? 0 : PySequence_Size(ops);
            if (np < 0 || no < 0) {
                ok = 0;
            } else {
                const char** t = PyMem_Malloc((size_t)(np > 0 ? np : 1) * sizeof(char*));
                const char** nm = PyMem_Malloc((size_t)(np > 0 ? np : 1) * sizeof(char*));
                const char** k = PyMem_Malloc((size_t)(no > 0 ? no : 1) * sizeof(char*));
                double* v = PyMem_Malloc((size_t)(no > 0 ? no : 1) * sizeof(double));
                bool* hv = PyMem_Malloc((size_t)(no > 0 ? no : 1) * sizeof(bool));
                if (!t || !nm || !k || !v || !hv) {
                    PyMem_Free((void*)t); PyMem_Free((void*)nm); PyMem_Free((void*)k);
                    PyMem_Free(v); PyMem_Free(hv);
                    PyErr_NoMemory(); ok = 0;
                } else {
                    entity_types[built] = t; entity_names[built] = nm;
                    op_keys[built] = k; op_values[built] = v; op_has_values[built] = hv;
                    entity_counts[built] = (int32_t)np;
                    op_counts[built] = (int32_t)no;
                    for (Py_ssize_t i = 0; ok && i < np; i++) {
                        PyObject* pair = PySequence_GetItem(pairs, i);  // new ref
                        const char* type = NULL; const char* name = NULL;
                        ok = pair && PyArg_ParseTuple(pair, "sz", &type, &name);
                        Py_XDECREF(pair);
                        if (ok) { t[i] = type; nm[i] = name; }
                    }
                    for (Py_ssize_t i = 0; ok && i < no; i++) {
                        PyObject* op = PySequence_GetItem(ops, i);  // new ref
                        const char* key = NULL; PyObject* value = NULL;
                        ok = op && PyArg_ParseTuple(op, "sO", &key, &value);
                        if (ok) {
                            k[i] = key;
                            // A None value is Java's `Op(key, null)`: remove the
                            // quota. The flag, not the double, carries that —
                            // every double including 0 is a legal quota value.
                            if (value == Py_None) {
                                hv[i] = false;
                                v[i] = 0.0;
                            } else {
                                double d = PyFloat_AsDouble(value);
                                if (d == -1.0 && PyErr_Occurred()) {
                                    ok = 0;
                                } else {
                                    hv[i] = true;
                                    v[i] = d;
                                }
                            }
                        }
                        Py_XDECREF(op);
                    }
                }
            }
        }
        Py_XDECREF(item);
        // On failure, count this row before breaking: if the five buffers
        // above were successfully allocated (t/nm/k/v/hv) but a later
        // sub-step in this same iteration failed (parsing a pair or op
        // element), the row is fully populated and owns real allocations
        // that must still be freed by the `built`-bounded cleanup loop
        // below. Breaking without this increment left `built` one short of
        // covering that row, leaking it — the buffers were never NULL, so
        // this was a real leak on ordinary malformed input, not OOM-only.
        // Incrementing here is always safe even when the row's pointers are
        // still NULL (the earlier `if (!t || !nm || ...)` branch), since
        // PyMem_Free(NULL) is a no-op.
        if (!ok) { failed = 1; built++; break; }
    }

    if (!failed) {
        Py_INCREF(cb);
        kafka_admin_AdminClient_alter_client_quotas_async(
            (kafka_admin_AdminClient_t*)(uintptr_t)h,
            (const char* const* const*)entity_types, (const char* const* const*)entity_names,
            entity_counts, (const char* const* const*)op_keys,
            (const double* const*)op_values, (const bool* const*)op_has_values, op_counts,
            (int32_t)n, timeout_ms, validate_only ? true : false,
            admin_alter_client_quotas_trampoline, cb);
    }
    for (Py_ssize_t i = 0; i < built; i++) {
        PyMem_Free((void*)entity_types[i]); PyMem_Free((void*)entity_names[i]);
        PyMem_Free((void*)op_keys[i]); PyMem_Free(op_values[i]); PyMem_Free(op_has_values[i]);
    }
    PyMem_Free((void*)entity_types); PyMem_Free((void*)entity_names); PyMem_Free(entity_counts);
    PyMem_Free((void*)op_keys); PyMem_Free(op_values); PyMem_Free(op_has_values);
    PyMem_Free(op_counts);
    if (failed) return NULL;
    Py_RETURN_NONE;
}

// ---- drains -----------------------------------------------------------------

// {binding_tuple: error_or_None}
static PyObject* py_CreateAclsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_CreateAclsResult_t* r = (kafka_admin_CreateAclsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_CreateAclsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_CreateAclsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = acl_binding_to_py(kafka_admin_CreateAclsResult_get_binding(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_CreateAclsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_CreateAclsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_CreateAclsResult_destroy(r);
    return d;
}

// [binding_tuple] — a plain list, because describeAcls has one future for the
// whole call and so no key to hang an error on.
static PyObject* py_DescribeAclsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeAclsResult_t* r = (kafka_admin_DescribeAclsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeAclsResult_count(r);
    PyObject* list = PyList_New(n < 0 ? 0 : n);
    if (list == NULL) { kafka_admin_DescribeAclsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* b = acl_binding_to_py(kafka_admin_DescribeAclsResult_get_binding(r, i));
        if (b == NULL) {
            Py_DECREF(list);
            kafka_admin_DescribeAclsResult_destroy(r); return NULL;
        }
        PyList_SET_ITEM(list, i, b);
    }
    kafka_admin_DescribeAclsResult_destroy(r);
    return list;
}

// {filter_tuple: (error_or_None, [(binding_or_None, error_or_None)])}
//
// Two levels, because Java's FilterResults holds one FilterResult per matched
// ACL and each carries either a binding or its own exception. The outer error
// is the filter's future failing, which is a different thing.
static PyObject* py_DeleteAclsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DeleteAclsResult_t* r = (kafka_admin_DeleteAclsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DeleteAclsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DeleteAclsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        int32_t rn = kafka_admin_DeleteAclsResult_get_result_count(r, i);
        PyObject* results = PyList_New(rn < 0 ? 0 : rn);
        if (results == NULL) { Py_DECREF(d); kafka_admin_DeleteAclsResult_destroy(r); return NULL; }
        for (int32_t j = 0; j < rn; j++) {
            PyObject* b = acl_binding_to_py(kafka_admin_DeleteAclsResult_get_binding(r, i, j));
            PyObject* e =
                borrowed_error_to_py(kafka_admin_DeleteAclsResult_get_result_error(r, i, j));
            PyObject* row = error_value_pair(e, b);
            if (row == NULL) {
                Py_DECREF(results); Py_DECREF(d);
                kafka_admin_DeleteAclsResult_destroy(r); return NULL;
            }
            PyList_SET_ITEM(results, j, row);
        }
        PyObject* key = acl_binding_filter_to_py(kafka_admin_DeleteAclsResult_get_filter(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_DeleteAclsResult_get_error(r, i));
        PyObject* value = error_value_pair(err, results);
        if (!key || !value || PyDict_SetItem(d, key, value) < 0) {
            Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d);
            kafka_admin_DeleteAclsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(value);
    }
    kafka_admin_DeleteAclsResult_destroy(r);
    return d;
}

// {entity_pairs: [(quota_key, quota_value)]}
static PyObject* py_DescribeClientQuotasResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeClientQuotasResult_t* r =
        (kafka_admin_DescribeClientQuotasResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeClientQuotasResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeClientQuotasResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        int32_t qn = kafka_admin_DescribeClientQuotasResult_get_quota_count(r, i);
        PyObject* quotas = PyList_New(qn < 0 ? 0 : qn);
        if (quotas == NULL) {
            Py_DECREF(d); kafka_admin_DescribeClientQuotasResult_destroy(r); return NULL;
        }
        int broken = 0;
        for (int32_t j = 0; j < qn; j++) {
            double value = 0.0;
            // Out of range is impossible here (j < the reported count), but the
            // accessor is an out-param because no double sentinel could be
            // unambiguous, so the return value is still checked.
            if (!kafka_admin_DescribeClientQuotasResult_get_quota_value(r, i, j, &value)) {
                broken = 1;
                break;
            }
            PyObject* pair = Py_BuildValue(
                "(sd)", kafka_admin_DescribeClientQuotasResult_get_quota_key(r, i, j), value);
            if (pair == NULL) { broken = 1; break; }
            PyList_SET_ITEM(quotas, j, pair);
        }
        PyObject* key = broken ? NULL
                               : client_quota_entity_to_py(
                                     kafka_admin_DescribeClientQuotasResult_get_entity(r, i));
        if (broken || !key || PyDict_SetItem(d, key, quotas) < 0) {
            Py_XDECREF(key); Py_DECREF(quotas); Py_DECREF(d);
            kafka_admin_DescribeClientQuotasResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(quotas);
    }
    kafka_admin_DescribeClientQuotasResult_destroy(r);
    return d;
}

// {entity_pairs: error_or_None}
static PyObject* py_AlterClientQuotasResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_AlterClientQuotasResult_t* r = (kafka_admin_AlterClientQuotasResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_AlterClientQuotasResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_AlterClientQuotasResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key =
            client_quota_entity_to_py(kafka_admin_AlterClientQuotasResult_get_entity(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_AlterClientQuotasResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_AlterClientQuotasResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_AlterClientQuotasResult_destroy(r);
    return d;
}

// ---------------------------------------------------------------------------
// B5b — SCRAM, delegation tokens and features
//
// Java's MockAdminClient throws for the two SCRAM RPCs
// (MockAdminClient.java:1251-1259), so the success branch of those two drains
// is unreachable from the test suite; their Py_BuildValue arity is checked
// statically by `test/static/test_format_arity.py` and their field order by review
// against the matching `_to_*` unpacker in admin.py. The mock *does* implement
// the four delegation-token RPCs and both feature RPCs, so those drains are
// exercised end to end by test_admin.py.
// ---------------------------------------------------------------------------

static void admin_describe_user_scram_credentials_trampoline(
    kafka_admin_DescribeUserScramCredentialsResult_t* r,
    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_user_scram_credentials_trampoline(
    kafka_admin_AlterUserScramCredentialsResult_t* r,
    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_create_delegation_token_trampoline(kafka_admin_CreateDelegationTokenResult_t* r,
                                                     kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_renew_delegation_token_trampoline(kafka_admin_RenewDelegationTokenResult_t* r,
                                                    kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_expire_delegation_token_trampoline(kafka_admin_ExpireDelegationTokenResult_t* r,
                                                     kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_delegation_token_trampoline(kafka_admin_DescribeDelegationTokenResult_t* r,
                                                       kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_features_trampoline(kafka_admin_DescribeFeaturesResult_t* r,
                                               kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_update_features_trampoline(kafka_admin_UpdateFeaturesResult_t* r,
                                             kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }

// ---- value converters -------------------------------------------------------

// (principal_type, name, token_authenticated) — the field order
// `_to_kafka_principal` unpacks.
static PyObject* kafka_principal_to_py(const kafka_common_security_auth_KafkaPrincipal_t* p) {
    if (p == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(ssO)", kafka_common_security_auth_KafkaPrincipal_principal_type(p),
                         kafka_common_security_auth_KafkaPrincipal_name(p),
                         kafka_common_security_auth_KafkaPrincipal_token_authenticated(p) ? Py_True : Py_False);
}

// (token_id, owner, requester, [renewers], issue_ts, expiry_ts, max_ts,
//  hmac_bytes, hmac_base64) — the field order `_to_delegation_token` unpacks.
// The HMAC crosses as `bytes` rather than `str`: it is a raw MAC and can
// contain interior NULs, so it needs the explicit length that `y#` carries.
static PyObject* delegation_token_to_py(const kafka_common_security_token_delegation_DelegationToken_t* t) {
    if (t == NULL) Py_RETURN_NONE;
    const kafka_common_security_token_delegation_TokenInformation_t* info = kafka_common_security_token_delegation_DelegationToken_token_info(t);
    // `renewers()` is an owned list of owned principals; destroyed once
    // copied out.
    kafka_List_t* renewer_list = kafka_common_security_token_delegation_TokenInformation_renewers(info);
    int32_t renewer_count = kafka_List_size(renewer_list);
    PyObject* renewers = PyList_New(renewer_count < 0 ? 0 : renewer_count);
    if (renewers == NULL) { kafka_List_destroy(renewer_list); return NULL; }
    for (int32_t i = 0; i < renewer_count; i++) {
        PyObject* renewer = kafka_principal_to_py(
            (const kafka_common_security_auth_KafkaPrincipal_t*)kafka_List_get(renewer_list, i));
        if (renewer == NULL) { Py_DECREF(renewers); kafka_List_destroy(renewer_list); return NULL; }
        PyList_SET_ITEM(renewers, i, renewer);
    }
    kafka_List_destroy(renewer_list);
    PyObject* owner = kafka_principal_to_py(kafka_common_security_token_delegation_TokenInformation_owner(info));
    PyObject* requester = kafka_principal_to_py(kafka_common_security_token_delegation_TokenInformation_token_requester(info));
    if (owner == NULL || requester == NULL) {
        Py_XDECREF(owner); Py_XDECREF(requester); Py_DECREF(renewers);
        return NULL;
    }
    kafka_Bytes_t hmac = kafka_common_security_token_delegation_DelegationToken_hmac(t);
    // Owned, unlike the borrowed getters: freed after Py_BuildValue copied it.
    char* hmac_base64 = kafka_common_security_token_delegation_DelegationToken_hmac_as_base64_string(t);
    // 'O' (not 'N') plus an explicit, unconditional Py_DECREF below: 'N'
    // steals its reference only when do_mkvalue actually runs for that item,
    // which do_mktuple skips entirely if its own PyTuple_New fails (OOM) —
    // in that case an 'N' argument is never consumed and leaks. 'O' plus a
    // decref that runs regardless of Py_BuildValue's outcome makes ownership
    // independent of that internal control flow. See node_to_py() above for
    // the established precedent of this pattern in this file.
    PyObject* out = Py_BuildValue("(sOOOLLLy#s)", kafka_common_security_token_delegation_TokenInformation_token_id(info),
                         owner, requester, renewers,
                         (long long)kafka_common_security_token_delegation_TokenInformation_issue_timestamp(info),
                         (long long)kafka_common_security_token_delegation_TokenInformation_expiry_timestamp(info),
                         (long long)kafka_common_security_token_delegation_TokenInformation_max_timestamp(info),
                         (const char*)hmac.data, (Py_ssize_t)(hmac.len < 0 ? 0 : hmac.len),
                         hmac_base64);
    kafka_string_destroy(hmac_base64);
    Py_DECREF(owner); Py_DECREF(requester); Py_DECREF(renewers);
    return out;
}

// ---- request marshaling -----------------------------------------------------

// The two parallel arrays a principal list crosses as. `names` borrows from the
// Python tuples, which the caller keeps alive for the duration of the call.
typedef struct {
    const char** types;
    const char** names;
    Py_ssize_t count;
} principal_arrays_t;

static void principal_arrays_free(principal_arrays_t* a) {
    PyMem_Free((void*)a->types);
    PyMem_Free((void*)a->names);
    a->types = NULL; a->names = NULL; a->count = 0;
}

// Fills `a` from a sequence of (principal_type, name) pairs. Returns 0 on
// success, -1 with a Python exception set on failure.
static int build_principal_arrays(PyObject* principals, principal_arrays_t* a) {
    a->types = NULL; a->names = NULL; a->count = 0;
    Py_ssize_t n = principals == Py_None ? 0 : PySequence_Size(principals);
    if (n < 0) return -1;
    size_t slots = (size_t)(n > 0 ? n : 1);
    a->types = PyMem_Malloc(slots * sizeof(char*));
    a->names = PyMem_Malloc(slots * sizeof(char*));
    if (!a->types || !a->names) { principal_arrays_free(a); PyErr_NoMemory(); return -1; }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(principals, i);  // new ref
        const char* type = NULL; const char* name = NULL;
        int ok = item && PyArg_ParseTuple(item, "ss", &type, &name);
        Py_XDECREF(item);
        if (!ok) { principal_arrays_free(a); return -1; }
        a->types[i] = type;
        a->names[i] = name;
    }
    a->count = n;
    return 0;
}

// ---- submits ----------------------------------------------------------------

static PyObject* py_Admin_describe_user_scram_credentials_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* users; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &users, &timeout_ms, &cb)) return NULL;

    Py_ssize_t n = PySequence_Size(users);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** names = PyMem_Malloc(slots * sizeof(char*));
    if (names == NULL) { PyErr_NoMemory(); return NULL; }
    int failed = 0;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(users, i);  // new ref
        const char* user = item ? PyUnicode_AsUTF8(item) : NULL;
        Py_XDECREF(item);
        if (user == NULL) { failed = 1; break; }
        names[i] = user;
    }
    if (!failed) {
        Py_INCREF(cb);
        kafka_admin_AdminClient_describe_user_scram_credentials_async(
            (kafka_admin_AdminClient_t*)(uintptr_t)h, names, (int32_t)n, timeout_ms,
            admin_describe_user_scram_credentials_trampoline, cb);
    }
    PyMem_Free((void*)names);
    if (failed) return NULL;
    Py_RETURN_NONE;
}

static PyObject* py_Admin_alter_user_scram_credentials_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* rows; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &rows, &timeout_ms, &cb)) return NULL;

    Py_ssize_t n = PySequence_Size(rows);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** users = PyMem_Malloc(slots * sizeof(char*));
    bool* is_deletions = PyMem_Malloc(slots * sizeof(bool));
    int32_t* mechanisms = PyMem_Malloc(slots * sizeof(int32_t));
    int32_t* iterations = PyMem_Malloc(slots * sizeof(int32_t));
    const uint8_t** passwords = PyMem_Malloc(slots * sizeof(uint8_t*));
    int32_t* password_lens = PyMem_Malloc(slots * sizeof(int32_t));
    const uint8_t** salts = PyMem_Malloc(slots * sizeof(uint8_t*));
    int32_t* salt_lens = PyMem_Malloc(slots * sizeof(int32_t));
    // `salt is not None` is a discriminant of its own: Java's four-argument
    // upsertion constructor accepts a zero-length salt, so an empty-but-present
    // salt must select it rather than the salt-generating three-argument one.
    bool* has_salts = PyMem_Malloc(slots * sizeof(bool));
    if (!users || !is_deletions || !mechanisms || !iterations || !passwords || !password_lens ||
        !salts || !salt_lens || !has_salts) {
        PyMem_Free((void*)users); PyMem_Free(is_deletions); PyMem_Free(mechanisms);
        PyMem_Free(iterations); PyMem_Free((void*)passwords); PyMem_Free(password_lens);
        PyMem_Free((void*)salts); PyMem_Free(salt_lens); PyMem_Free(has_salts);
        PyErr_NoMemory(); return NULL;
    }
    int failed = 0;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(rows, i);  // new ref
        const char* user = NULL; int is_deletion = 0; int mechanism = 0; int iteration = 0;
        PyObject* password_obj = NULL; PyObject* salt_obj = NULL;
        // The password and salt are nullable *bytes*, which no single
        // PyArg_ParseTuple unit covers: `y#` rejects None and `z#` rejects
        // bytes. They cross as objects and are unpacked below.
        int ok = item && PyArg_ParseTuple(item, "spiiOO", &user, &is_deletion, &mechanism,
                                          &iteration, &password_obj, &salt_obj);
        char* password = NULL; Py_ssize_t password_len = 0;
        char* salt = NULL; Py_ssize_t salt_len = 0;
        if (ok && password_obj != Py_None) {
            ok = PyBytes_AsStringAndSize(password_obj, &password, &password_len) == 0;
        }
        if (ok && salt_obj != Py_None) {
            ok = PyBytes_AsStringAndSize(salt_obj, &salt, &salt_len) == 0;
        }
        Py_XDECREF(item);
        if (!ok) { failed = 1; break; }
        users[i] = user;
        is_deletions[i] = is_deletion ? true : false;
        mechanisms[i] = (int32_t)mechanism;
        iterations[i] = (int32_t)iteration;
        passwords[i] = (const uint8_t*)password;
        password_lens[i] = (int32_t)password_len;
        salts[i] = (const uint8_t*)salt;
        salt_lens[i] = (int32_t)salt_len;
        has_salts[i] = salt_obj != Py_None;
    }
    if (!failed) {
        Py_INCREF(cb);
        kafka_admin_AdminClient_alter_user_scram_credentials_async(
            (kafka_admin_AdminClient_t*)(uintptr_t)h, users, is_deletions, mechanisms, iterations,
            passwords, password_lens, salts, salt_lens, has_salts, (int32_t)n, timeout_ms,
            admin_alter_user_scram_credentials_trampoline, cb);
    }
    PyMem_Free((void*)users); PyMem_Free(is_deletions); PyMem_Free(mechanisms);
    PyMem_Free(iterations); PyMem_Free((void*)passwords); PyMem_Free(password_lens);
    PyMem_Free((void*)salts); PyMem_Free(salt_lens); PyMem_Free(has_salts);
    if (failed) return NULL;
    Py_RETURN_NONE;
}

static PyObject* py_Admin_create_delegation_token_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* renewers; int timeout_ms; PyObject* cb;
    const char* owner_type = NULL; const char* owner_name = NULL; long long max_lifetime_ms = -1;
    if (!PyArg_ParseTuple(args, "KOzzLiO", &h, &renewers, &owner_type, &owner_name,
                          &max_lifetime_ms, &timeout_ms, &cb))
        return NULL;

    principal_arrays_t a;
    if (build_principal_arrays(renewers, &a) < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_create_delegation_token_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, a.types, a.names, (int32_t)a.count, owner_type,
        owner_name, (int64_t)max_lifetime_ms, timeout_ms, admin_create_delegation_token_trampoline,
        cb);
    principal_arrays_free(&a);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_renew_delegation_token_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* hmac = NULL; Py_ssize_t hmac_len = 0;
    long long renew_period_ms = -1; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "Ky#LiO", &h, &hmac, &hmac_len, &renew_period_ms, &timeout_ms, &cb))
        return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_renew_delegation_token_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, (const uint8_t*)hmac, (int32_t)hmac_len,
        (int64_t)renew_period_ms, timeout_ms, admin_renew_delegation_token_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_expire_delegation_token_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* hmac = NULL; Py_ssize_t hmac_len = 0;
    long long expiry_period_ms = -1; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "Ky#LiO", &h, &hmac, &hmac_len, &expiry_period_ms, &timeout_ms, &cb))
        return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_expire_delegation_token_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, (const uint8_t*)hmac, (int32_t)hmac_len,
        (int64_t)expiry_period_ms, timeout_ms, admin_expire_delegation_token_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_delegation_token_async(PyObject* self, PyObject* args) {
    unsigned long long h; int has_owners; PyObject* owners; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KpOiO", &h, &has_owners, &owners, &timeout_ms, &cb)) return NULL;

    principal_arrays_t a;
    if (build_principal_arrays(owners, &a) < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_delegation_token_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, has_owners ? true : false, a.types, a.names,
        (int32_t)a.count, timeout_ms, admin_describe_delegation_token_trampoline, cb);
    principal_arrays_free(&a);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_features_async(PyObject* self, PyObject* args) {
    unsigned long long h; int has_node_id; int node_id; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KpiiO", &h, &has_node_id, &node_id, &timeout_ms, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_features_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, has_node_id ? true : false, (int32_t)node_id,
        timeout_ms, admin_describe_features_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_update_features_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* rows; int timeout_ms; int validate_only; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOipO", &h, &rows, &timeout_ms, &validate_only, &cb)) return NULL;

    Py_ssize_t n = PySequence_Size(rows);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** features = PyMem_Malloc(slots * sizeof(char*));
    int16_t* max_version_levels = PyMem_Malloc(slots * sizeof(int16_t));
    int32_t* upgrade_types = PyMem_Malloc(slots * sizeof(int32_t));
    if (!features || !max_version_levels || !upgrade_types) {
        PyMem_Free((void*)features); PyMem_Free(max_version_levels); PyMem_Free(upgrade_types);
        PyErr_NoMemory(); return NULL;
    }
    int failed = 0;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(rows, i);  // new ref
        const char* feature = NULL; int level = 0; int upgrade_type = 0;
        int ok = item && PyArg_ParseTuple(item, "sii", &feature, &level, &upgrade_type);
        Py_XDECREF(item);
        if (!ok) { failed = 1; break; }
        features[i] = feature;
        max_version_levels[i] = (int16_t)level;
        upgrade_types[i] = (int32_t)upgrade_type;
    }
    if (!failed) {
        Py_INCREF(cb);
        kafka_admin_AdminClient_update_features_async(
            (kafka_admin_AdminClient_t*)(uintptr_t)h, features, max_version_levels, upgrade_types,
            (int32_t)n, timeout_ms, validate_only ? true : false, admin_update_features_trampoline,
            cb);
    }
    PyMem_Free((void*)features); PyMem_Free(max_version_levels); PyMem_Free(upgrade_types);
    if (failed) return NULL;
    Py_RETURN_NONE;
}

// Mock-only: seed the three feature-level maps `describeFeatures` reports and
// `updateFeatures` validates against. `spec` is [(feature, level, min, max)].
static PyObject* py_MockAdminClient_set_feature_levels(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** features = PyMem_Malloc(slots * sizeof(char*));
    int16_t* levels = PyMem_Malloc(slots * sizeof(int16_t));
    int16_t* min_levels = PyMem_Malloc(slots * sizeof(int16_t));
    int16_t* max_levels = PyMem_Malloc(slots * sizeof(int16_t));
    if (!features || !levels || !min_levels || !max_levels) {
        PyMem_Free((void*)features); PyMem_Free(levels); PyMem_Free(min_levels);
        PyMem_Free(max_levels);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* feature = NULL; int level = 0; int min_level = 0; int max_level = 0;
        int ok = item && PyArg_ParseTuple(item, "siii", &feature, &level, &min_level, &max_level);
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free((void*)features); PyMem_Free(levels); PyMem_Free(min_levels);
            PyMem_Free(max_levels);
            return NULL;
        }
        features[i] = feature;
        levels[i] = (int16_t)level;
        min_levels[i] = (int16_t)min_level;
        max_levels[i] = (int16_t)max_level;
    }
    kafka_common_Error_t* e = kafka_admin_MockAdminClient_set_feature_levels(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, features, levels, min_levels, max_levels,
        (int32_t)n);
    PyMem_Free((void*)features); PyMem_Free(levels); PyMem_Free(min_levels); PyMem_Free(max_levels);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// ---- drains -----------------------------------------------------------------

// {user: (error_or_None, [(mechanism, iterations)])}
static PyObject* py_DescribeUserScramCredentialsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeUserScramCredentialsResult_t* r =
        (kafka_admin_DescribeUserScramCredentialsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeUserScramCredentialsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeUserScramCredentialsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        int32_t credentials = kafka_admin_DescribeUserScramCredentialsResult_get_credential_count(r, i);
        PyObject* infos = PyList_New(credentials < 0 ? 0 : credentials);
        if (infos == NULL) { Py_DECREF(d); kafka_admin_DescribeUserScramCredentialsResult_destroy(r); return NULL; }
        for (int32_t j = 0; j < credentials; j++) {
            PyObject* info = Py_BuildValue(
                "(ii)",
                kafka_admin_DescribeUserScramCredentialsResult_get_credential_mechanism(r, i, j),
                kafka_admin_DescribeUserScramCredentialsResult_get_credential_iterations(r, i, j));
            // An unfilled slot stays NULL, which list_dealloc's Py_XDECREF
            // handles, so breaking out here leaks nothing.
            if (info == NULL) { break; }
            PyList_SET_ITEM(infos, j, info);
        }
        PyObject* key =
            PyUnicode_FromString(kafka_admin_DescribeUserScramCredentialsResult_get_user(r, i));
        PyObject* value = error_value_pair(
            borrowed_error_to_py(kafka_admin_DescribeUserScramCredentialsResult_get_error(r, i)),
            infos);
        if (!key || !value || PyErr_Occurred() || PyDict_SetItem(d, key, value) < 0) {
            Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d);
            kafka_admin_DescribeUserScramCredentialsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(value);
    }
    kafka_admin_DescribeUserScramCredentialsResult_destroy(r);
    return d;
}

// {user: error_or_None}
static PyObject* py_AlterUserScramCredentialsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_AlterUserScramCredentialsResult_t* r =
        (kafka_admin_AlterUserScramCredentialsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_AlterUserScramCredentialsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_AlterUserScramCredentialsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key =
            PyUnicode_FromString(kafka_admin_AlterUserScramCredentialsResult_get_user(r, i));
        PyObject* err =
            borrowed_error_to_py(kafka_admin_AlterUserScramCredentialsResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_AlterUserScramCredentialsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_AlterUserScramCredentialsResult_destroy(r);
    return d;
}

// One token tuple.
static PyObject* py_CreateDelegationTokenResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_CreateDelegationTokenResult_t* r =
        (kafka_admin_CreateDelegationTokenResult_t*)(uintptr_t)ptr;
    PyObject* token =
        delegation_token_to_py(kafka_admin_CreateDelegationTokenResult_get_token(r));
    kafka_admin_CreateDelegationTokenResult_destroy(r);
    return token;
}

// [token tuple]
static PyObject* py_DescribeDelegationTokenResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeDelegationTokenResult_t* r =
        (kafka_admin_DescribeDelegationTokenResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeDelegationTokenResult_count(r);
    PyObject* list = PyList_New(n < 0 ? 0 : n);
    if (list == NULL) { kafka_admin_DescribeDelegationTokenResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* token =
            delegation_token_to_py(kafka_admin_DescribeDelegationTokenResult_get_token(r, i));
        if (token == NULL) {
            Py_DECREF(list); kafka_admin_DescribeDelegationTokenResult_destroy(r); return NULL;
        }
        PyList_SET_ITEM(list, i, token);
    }
    kafka_admin_DescribeDelegationTokenResult_destroy(r);
    return list;
}

static PyObject* py_RenewDelegationTokenResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_RenewDelegationTokenResult_t* r =
        (kafka_admin_RenewDelegationTokenResult_t*)(uintptr_t)ptr;
    long long expiry = (long long)kafka_admin_RenewDelegationTokenResult_expiry_timestamp(r);
    kafka_admin_RenewDelegationTokenResult_destroy(r);
    return PyLong_FromLongLong(expiry);
}

static PyObject* py_ExpireDelegationTokenResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ExpireDelegationTokenResult_t* r =
        (kafka_admin_ExpireDelegationTokenResult_t*)(uintptr_t)ptr;
    long long expiry = (long long)kafka_admin_ExpireDelegationTokenResult_expiry_timestamp(r);
    kafka_admin_ExpireDelegationTokenResult_destroy(r);
    return PyLong_FromLongLong(expiry);
}

// ([(feature, min_version_level, max_version_level)], epoch_or_None,
//  [(feature, min_version, max_version)]) — finalized first, then the epoch,
// then supported. The two lists are independently indexed.
static PyObject* py_DescribeFeaturesResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeFeaturesResult_t* r = (kafka_admin_DescribeFeaturesResult_t*)(uintptr_t)ptr;

    int32_t fn = kafka_admin_DescribeFeaturesResult_finalized_count(r);
    PyObject* finalized = PyList_New(fn < 0 ? 0 : fn);
    if (finalized == NULL) { kafka_admin_DescribeFeaturesResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < fn; i++) {
        PyObject* row = Py_BuildValue(
            "(shh)", kafka_admin_DescribeFeaturesResult_get_finalized_feature(r, i),
            kafka_admin_DescribeFeaturesResult_get_finalized_min_version_level(r, i),
            kafka_admin_DescribeFeaturesResult_get_finalized_max_version_level(r, i));
        if (row == NULL) {
            Py_DECREF(finalized); kafka_admin_DescribeFeaturesResult_destroy(r); return NULL;
        }
        PyList_SET_ITEM(finalized, i, row);
    }

    int32_t sn = kafka_admin_DescribeFeaturesResult_supported_count(r);
    PyObject* supported = PyList_New(sn < 0 ? 0 : sn);
    if (supported == NULL) {
        Py_DECREF(finalized); kafka_admin_DescribeFeaturesResult_destroy(r); return NULL;
    }
    for (int32_t i = 0; i < sn; i++) {
        PyObject* row = Py_BuildValue(
            "(shh)", kafka_admin_DescribeFeaturesResult_get_supported_feature(r, i),
            kafka_admin_DescribeFeaturesResult_get_supported_min_version(r, i),
            kafka_admin_DescribeFeaturesResult_get_supported_max_version(r, i));
        if (row == NULL) {
            Py_DECREF(finalized); Py_DECREF(supported);
            kafka_admin_DescribeFeaturesResult_destroy(r); return NULL;
        }
        PyList_SET_ITEM(supported, i, row);
    }

    int64_t epoch = 0;
    // Every int64 is a legal epoch, so the boolean, not a sentinel, decides
    // whether the broker reported one.
    bool has_epoch = kafka_admin_DescribeFeaturesResult_finalized_features_epoch(r, &epoch);
    kafka_admin_DescribeFeaturesResult_destroy(r);
    // 'O' (not 'N') plus an explicit, unconditional Py_DECREF: 'N' steals its
    // reference only when do_mkvalue runs for that item, which do_mktuple
    // skips entirely if its own PyTuple_New fails (OOM) — in that case an 'N'
    // argument is never consumed and leaks. See node_to_py() above for the
    // established precedent of this pattern in this file.
    PyObject* out = has_epoch
        ? Py_BuildValue("(OLO)", finalized, (long long)epoch, supported)
        : Py_BuildValue("(OOO)", finalized, Py_None, supported);
    Py_DECREF(finalized); Py_DECREF(supported);
    return out;
}

// {feature: error_or_None}
static PyObject* py_UpdateFeaturesResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_UpdateFeaturesResult_t* r = (kafka_admin_UpdateFeaturesResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_UpdateFeaturesResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_UpdateFeaturesResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* key = PyUnicode_FromString(kafka_admin_UpdateFeaturesResult_get_feature(r, i));
        PyObject* err = borrowed_error_to_py(kafka_admin_UpdateFeaturesResult_get_error(r, i));
        if (!key || !err || PyDict_SetItem(d, key, err) < 0) {
            Py_XDECREF(key); Py_XDECREF(err); Py_DECREF(d);
            kafka_admin_UpdateFeaturesResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(err);
    }
    kafka_admin_UpdateFeaturesResult_destroy(r);
    return d;
}

// ---------------------------------------------------------------------------
// B6 — producers and transactions
//
// Java's MockAdminClient throws for all six (MockAdminClient.java:1368-1395),
// so the *success* branch of every drain below is unreachable from the test
// suite; their Py_BuildValue arity is checked statically by
// `test/static/test_format_arity.py` and their field order by review against the
// matching `_to_*` unpacker in admin.py. What the suite does exercise is the
// error branch of each: the mocks that throw per key still echo the requested
// key set, so key columns and per-key errors flow end to end.
//
// `abortTransaction` and `forceTerminateTransaction` have no result handle at
// all (Java's AbortTransactionResult exposes only all(), and
// TerminateTransactionResult only result()), so they reuse `admin_op_trampoline`
// and the `_resolve_void` / `_free_void` pair that `close_async` already uses.
// ---------------------------------------------------------------------------

static void admin_describe_producers_trampoline(kafka_admin_DescribeProducersResult_t* r,
                                                kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_transactions_trampoline(kafka_admin_DescribeTransactionsResult_t* r,
                                                   kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_fence_producers_trampoline(kafka_admin_FenceProducersResult_t* r,
                                             kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_transactions_trampoline(kafka_admin_ListTransactionsResult_t* r,
                                               kafka_common_Error_t* e, void* ud) { fire_handle_cb(r, e, ud); }

static PyObject* py_Admin_describe_producers_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; int has_broker_id; int broker_id; int timeout_ms;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOpiiO", &h, &spec, &has_broker_id, &broker_id, &timeout_ms, &cb))
        return NULL;

    // spec is a sequence of (topic:str, partition:int).
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    size_t slots = (size_t)(n > 0 ? n : 1);
    const char** topics = PyMem_Malloc(slots * sizeof(char*));
    int32_t* partitions = PyMem_Malloc(slots * sizeof(int32_t));
    if (topics == NULL || partitions == NULL) {
        PyMem_Free((void*)topics); PyMem_Free(partitions);
        PyErr_NoMemory(); return NULL;
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);  // new ref
        const char* t = NULL; int p = 0;
        int ok = item && PyArg_ParseTuple(item, "si", &t, &p);
        Py_XDECREF(item);
        if (!ok) { PyMem_Free((void*)topics); PyMem_Free(partitions); return NULL; }
        topics[i] = t; partitions[i] = p;
    }
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_producers_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, topics, partitions, (int32_t)n,
        has_broker_id ? true : false, (int32_t)broker_id, timeout_ms,
        admin_describe_producers_trampoline, cb);
    PyMem_Free((void*)topics); PyMem_Free(partitions);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_describe_transactions_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids_obj; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &ids_obj, &timeout_ms, &cb)) return NULL;

    const char** ids = NULL;
    Py_ssize_t n = build_string_array(ids_obj, &ids);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_describe_transactions_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, ids, (int32_t)n, timeout_ms,
        admin_describe_transactions_trampoline, cb);
    PyMem_Free(ids);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_fence_producers_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* ids_obj; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOiO", &h, &ids_obj, &timeout_ms, &cb)) return NULL;

    const char** ids = NULL;
    Py_ssize_t n = build_string_array(ids_obj, &ids);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_fence_producers_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, ids, (int32_t)n, timeout_ms,
        admin_fence_producers_trampoline, cb);
    PyMem_Free(ids);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_list_transactions_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* states_obj; PyObject* producer_ids_obj;
    long long duration_ms; const char* pattern = NULL; int timeout_ms; PyObject* cb;
    // `z` for the pattern: NULL is Java's "no pattern filter", distinct from "".
    if (!PyArg_ParseTuple(args, "KOOLziO", &h, &states_obj, &producer_ids_obj, &duration_ms,
                          &pattern, &timeout_ms, &cb))
        return NULL;

    const char** states = NULL;
    Py_ssize_t state_count = build_string_array(states_obj, &states);
    if (state_count < 0) return NULL;

    Py_ssize_t id_count = PySequence_Size(producer_ids_obj);
    if (id_count < 0) { PyMem_Free(states); return NULL; }
    int64_t* producer_ids = PyMem_Malloc((size_t)(id_count > 0 ? id_count : 1) * sizeof(int64_t));
    if (producer_ids == NULL) { PyMem_Free(states); PyErr_NoMemory(); return NULL; }
    for (Py_ssize_t i = 0; i < id_count; i++) {
        PyObject* item = PySequence_GetItem(producer_ids_obj, i);  // new ref
        long long value = item ? PyLong_AsLongLong(item) : 0;
        Py_XDECREF(item);
        if (PyErr_Occurred()) { PyMem_Free(states); PyMem_Free(producer_ids); return NULL; }
        producer_ids[i] = (int64_t)value;
    }

    Py_INCREF(cb);
    kafka_admin_AdminClient_list_transactions_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, states, (int32_t)state_count, producer_ids,
        (int32_t)id_count, (int64_t)duration_ms, pattern, timeout_ms,
        admin_list_transactions_trampoline, cb);
    PyMem_Free(states); PyMem_Free(producer_ids);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_abort_transaction_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long producer_id;
    int producer_epoch; int coordinator_epoch; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsiLiiiO", &h, &topic, &partition, &producer_id, &producer_epoch,
                          &coordinator_epoch, &timeout_ms, &cb))
        return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_abort_transaction_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, topic, (int32_t)partition, (int64_t)producer_id,
        (int32_t)producer_epoch, (int32_t)coordinator_epoch, timeout_ms, admin_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Admin_force_terminate_transaction_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* transactional_id; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsiO", &h, &transactional_id, &timeout_ms, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_force_terminate_transaction_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, transactional_id, timeout_ms,
        admin_op_trampoline, cb);
    Py_RETURN_NONE;
}

// ---- drains -----------------------------------------------------------------

// {(topic, partition): (error_or_None,
//                       [(producer_id, producer_epoch, last_sequence,
//                         last_timestamp, coordinator_epoch_or_None,
//                         current_transaction_start_offset_or_None)])}
//
// The two trailing columns follow Java's ProducerState constructor order,
// coordinatorEpoch before currentTransactionStartOffset, which is also the order
// `_to_producer_state` unpacks.
static PyObject* py_DescribeProducersResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeProducersResult_t* r = (kafka_admin_DescribeProducersResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeProducersResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeProducersResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        int32_t producers = kafka_admin_DescribeProducersResult_get_producer_count(r, i);
        PyObject* states = PyList_New(producers < 0 ? 0 : producers);
        if (states == NULL) { Py_DECREF(d); kafka_admin_DescribeProducersResult_destroy(r); return NULL; }
        for (int32_t j = 0; j < producers; j++) {
            int64_t start_offset = 0;
            int32_t coordinator_epoch = 0;
            bool has_start_offset =
                kafka_admin_DescribeProducersResult_get_current_transaction_start_offset(
                    r, i, j, &start_offset);
            bool has_coordinator_epoch =
                kafka_admin_DescribeProducersResult_get_coordinator_epoch(r, i, j, &coordinator_epoch);
            // The two Optionals are built first and handed over with `N`, which
            // steals the reference (including on a Py_BuildValue failure); an
            // `O` here would leak the fresh PyLong.
            PyObject* py_coordinator_epoch = has_coordinator_epoch
                                                 ? PyLong_FromLong((long)coordinator_epoch)
                                                 : (Py_INCREF(Py_None), Py_None);
            PyObject* py_start_offset = has_start_offset
                                            ? PyLong_FromLongLong((long long)start_offset)
                                            : (Py_INCREF(Py_None), Py_None);
            PyObject* state = (py_coordinator_epoch == NULL || py_start_offset == NULL)
                ? NULL
                : Py_BuildValue(
                      "(LiiLNN)",
                      (long long)kafka_admin_DescribeProducersResult_get_producer_id(r, i, j),
                      kafka_admin_DescribeProducersResult_get_producer_epoch(r, i, j),
                      kafka_admin_DescribeProducersResult_get_last_sequence(r, i, j),
                      (long long)kafka_admin_DescribeProducersResult_get_last_timestamp(r, i, j),
                      py_coordinator_epoch, py_start_offset);
            if (state == NULL) {
                Py_XDECREF(py_coordinator_epoch);
                Py_XDECREF(py_start_offset);
            }
            // An unfilled slot stays NULL, which list_dealloc's Py_XDECREF
            // handles, so breaking out here leaks nothing.
            if (state == NULL) { break; }
            PyList_SET_ITEM(states, j, state);
        }
        PyObject* key = Py_BuildValue("(si)", kafka_admin_DescribeProducersResult_get_topic(r, i),
                                      kafka_admin_DescribeProducersResult_get_partition(r, i));
        PyObject* value = error_value_pair(
            borrowed_error_to_py(kafka_admin_DescribeProducersResult_get_error(r, i)), states);
        if (!key || !value || PyErr_Occurred() || PyDict_SetItem(d, key, value) < 0) {
            Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d);
            kafka_admin_DescribeProducersResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(value);
    }
    kafka_admin_DescribeProducersResult_destroy(r);
    return d;
}

// {transactional_id: (error_or_None,
//                     (coordinator_id, state_name, producer_id, producer_epoch,
//                      transaction_timeout_ms, start_time_ms_or_None,
//                      [(topic, partition)]))}
static PyObject* py_DescribeTransactionsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_DescribeTransactionsResult_t* r =
        (kafka_admin_DescribeTransactionsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_DescribeTransactionsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_DescribeTransactionsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        int32_t partitions = kafka_admin_DescribeTransactionsResult_get_topic_partition_count(r, i);
        PyObject* topic_partitions = PyList_New(partitions < 0 ? 0 : partitions);
        if (topic_partitions == NULL) {
            Py_DECREF(d); kafka_admin_DescribeTransactionsResult_destroy(r); return NULL;
        }
        for (int32_t j = 0; j < partitions; j++) {
            PyObject* tp = Py_BuildValue(
                "(si)", kafka_admin_DescribeTransactionsResult_get_topic_partition_topic(r, i, j),
                kafka_admin_DescribeTransactionsResult_get_topic_partition_partition(r, i, j));
            if (tp == NULL) { break; }
            PyList_SET_ITEM(topic_partitions, j, tp);
        }
        int64_t start_time = 0;
        bool has_start_time = kafka_admin_DescribeTransactionsResult_get_transaction_start_time_ms(
            r, i, &start_time);
        PyObject* py_start_time = has_start_time ? PyLong_FromLongLong((long long)start_time)
                                                 : (Py_INCREF(Py_None), Py_None);
        PyObject* description =
            py_start_time == NULL
                ? NULL
                : Py_BuildValue(
                      "(isLiLNN)", kafka_admin_DescribeTransactionsResult_get_coordinator_id(r, i),
                      kafka_admin_DescribeTransactionsResult_get_state(r, i),
                      (long long)kafka_admin_DescribeTransactionsResult_get_producer_id(r, i),
                      kafka_admin_DescribeTransactionsResult_get_producer_epoch(r, i),
                      (long long)kafka_admin_DescribeTransactionsResult_get_transaction_timeout_ms(r, i),
                      py_start_time, topic_partitions);
        if (description == NULL) {
            Py_XDECREF(py_start_time);
            Py_DECREF(topic_partitions);
        }
        PyObject* key = PyUnicode_FromString(
            kafka_admin_DescribeTransactionsResult_get_transactional_id(r, i));
        PyObject* value = error_value_pair(
            borrowed_error_to_py(kafka_admin_DescribeTransactionsResult_get_error(r, i)),
            description);
        if (!key || !value || PyErr_Occurred() || PyDict_SetItem(d, key, value) < 0) {
            Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d);
            kafka_admin_DescribeTransactionsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(value);
    }
    kafka_admin_DescribeTransactionsResult_destroy(r);
    return d;
}

// {transactional_id: (error_or_None, (producer_id, epoch))}
static PyObject* py_FenceProducersResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_FenceProducersResult_t* r = (kafka_admin_FenceProducersResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_FenceProducersResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_FenceProducersResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* producer = Py_BuildValue(
            "(Li)", (long long)kafka_admin_FenceProducersResult_get_producer_id(r, i),
            (int)kafka_admin_FenceProducersResult_get_epoch_id(r, i));
        PyObject* key =
            PyUnicode_FromString(kafka_admin_FenceProducersResult_get_transactional_id(r, i));
        PyObject* value = error_value_pair(
            borrowed_error_to_py(kafka_admin_FenceProducersResult_get_error(r, i)), producer);
        if (!key || !value || PyErr_Occurred() || PyDict_SetItem(d, key, value) < 0) {
            Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d);
            kafka_admin_FenceProducersResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(value);
    }
    kafka_admin_FenceProducersResult_destroy(r);
    return d;
}

// {broker_id: (error_or_None, [(transactional_id, producer_id, state_name)])}
static PyObject* py_ListTransactionsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListTransactionsResult_t* r = (kafka_admin_ListTransactionsResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ListTransactionsResult_count(r);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_admin_ListTransactionsResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        int32_t listings = kafka_admin_ListTransactionsResult_get_listing_count(r, i);
        PyObject* rows = PyList_New(listings < 0 ? 0 : listings);
        if (rows == NULL) { Py_DECREF(d); kafka_admin_ListTransactionsResult_destroy(r); return NULL; }
        for (int32_t j = 0; j < listings; j++) {
            PyObject* row = Py_BuildValue(
                "(sLs)", kafka_admin_ListTransactionsResult_get_transactional_id(r, i, j),
                (long long)kafka_admin_ListTransactionsResult_get_producer_id(r, i, j),
                kafka_admin_ListTransactionsResult_get_state(r, i, j));
            if (row == NULL) { break; }
            PyList_SET_ITEM(rows, j, row);
        }
        PyObject* key =
            PyLong_FromLong((long)kafka_admin_ListTransactionsResult_get_broker_id(r, i));
        PyObject* value = error_value_pair(
            borrowed_error_to_py(kafka_admin_ListTransactionsResult_get_error(r, i)), rows);
        if (!key || !value || PyErr_Occurred() || PyDict_SetItem(d, key, value) < 0) {
            Py_XDECREF(key); Py_XDECREF(value); Py_DECREF(d);
            kafka_admin_ListTransactionsResult_destroy(r); return NULL;
        }
        Py_DECREF(key); Py_DECREF(value);
    }
    kafka_admin_ListTransactionsResult_destroy(r);
    return d;
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
    {"Admin_MockAdminClient_new", py_Admin_MockAdminClient_new, METH_VARARGS, "Create a MockAdminClient"},
    {"Admin_AdminClient_new", py_Admin_AdminClient_new, METH_VARARGS, "Create an AdminClient"},
    {"Admin_destroy", py_Admin_destroy, METH_VARARGS, "Destroy an admin-client handle"},
    {"Admin_close_async", py_Admin_close_async, METH_VARARGS, "Async close; cb(error_int)"},
    {"Admin_create_topics_async", py_Admin_create_topics_async, METH_VARARGS, "Async createTopics; cb(result_int, error_int)"},
    {"Admin_delete_topics_async", py_Admin_delete_topics_async, METH_VARARGS, "Async deleteTopics by name; cb(result_int, error_int)"},
    {"Admin_delete_topics_by_ids_async", py_Admin_delete_topics_by_ids_async, METH_VARARGS, "Async deleteTopics by id; cb(result_int, error_int)"},
    {"Admin_list_topics_async", py_Admin_list_topics_async, METH_VARARGS, "Async listTopics; cb(result_int, error_int)"},
    {"Admin_describe_topics_async", py_Admin_describe_topics_async, METH_VARARGS, "Async describeTopics by name; cb(result_int, error_int)"},
    {"Admin_describe_topics_by_ids_async", py_Admin_describe_topics_by_ids_async, METH_VARARGS, "Async describeTopics by id; cb(result_int, error_int)"},
    {"Admin_create_partitions_async", py_Admin_create_partitions_async, METH_VARARGS, "Async createPartitions; cb(result_int, error_int)"},
    {"Admin_delete_records_async", py_Admin_delete_records_async, METH_VARARGS, "Async deleteRecords; cb(result_int, error_int)"},
    {"MockAdminClient_timeout_next_request", py_MockAdminClient_timeout_next_request, METH_VARARGS, "Mock: time out the next N requests; returns error_int"},
    {"MockAdminClient_update_beginning_offsets", py_MockAdminClient_update_beginning_offsets,
     METH_VARARGS, "Mock: seed beginning offsets; returns error_int"},
    {"MockAdminClient_update_end_offsets", py_MockAdminClient_update_end_offsets, METH_VARARGS,
     "Mock: seed end offsets; returns error_int"},
    {"CreateTopicsResult_drain", py_CreateTopicsResult_drain, METH_VARARGS, "Drain+destroy a CreateTopicsResult handle into a dict"},
    {"DeleteTopicsResult_drain", py_DeleteTopicsResult_drain, METH_VARARGS, "Drain+destroy a DeleteTopicsResult handle into a dict"},
    {"ListTopicsResult_drain", py_ListTopicsResult_drain, METH_VARARGS, "Drain+destroy a ListTopicsResult handle into a dict"},
    {"DescribeTopicsResult_drain", py_DescribeTopicsResult_drain, METH_VARARGS, "Drain+destroy a DescribeTopicsResult handle into a dict"},
    {"CreatePartitionsResult_drain", py_CreatePartitionsResult_drain, METH_VARARGS, "Drain+destroy a CreatePartitionsResult handle into a dict"},
    {"DeleteRecordsResult_drain", py_DeleteRecordsResult_drain, METH_VARARGS, "Drain+destroy a DeleteRecordsResult handle into a dict"},
    {"Admin_describe_cluster_async", py_Admin_describe_cluster_async, METH_VARARGS,
     "Async describeCluster; cb(result_int, error_int)"},
    {"Admin_describe_configs_async", py_Admin_describe_configs_async, METH_VARARGS,
     "Async describeConfigs; cb(result_int, error_int)"},
    {"Admin_incremental_alter_configs_async", py_Admin_incremental_alter_configs_async, METH_VARARGS,
     "Async incrementalAlterConfigs; cb(result_int, error_int)"},
    {"Admin_list_config_resources_async", py_Admin_list_config_resources_async, METH_VARARGS,
     "Async listConfigResources; cb(result_int, error_int)"},
    {"Admin_describe_log_dirs_async", py_Admin_describe_log_dirs_async, METH_VARARGS,
     "Async describeLogDirs; cb(result_int, error_int)"},
    {"Admin_alter_replica_log_dirs_async", py_Admin_alter_replica_log_dirs_async, METH_VARARGS,
     "Async alterReplicaLogDirs; cb(result_int, error_int)"},
    {"Admin_describe_replica_log_dirs_async", py_Admin_describe_replica_log_dirs_async, METH_VARARGS,
     "Async describeReplicaLogDirs; cb(result_int, error_int)"},
    {"DescribeClusterResult_drain", py_DescribeClusterResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeClusterResult handle into a tuple"},
    {"DescribeConfigsResult_drain", py_DescribeConfigsResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeConfigsResult handle into a dict"},
    {"AlterConfigsResult_drain", py_AlterConfigsResult_drain, METH_VARARGS,
     "Drain+destroy an AlterConfigsResult handle into a dict"},
    {"ListConfigResourcesResult_drain", py_ListConfigResourcesResult_drain, METH_VARARGS,
     "Drain+destroy a ListConfigResourcesResult handle into a list"},
    {"DescribeLogDirsResult_drain", py_DescribeLogDirsResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeLogDirsResult handle into a dict"},
    {"AlterReplicaLogDirsResult_drain", py_AlterReplicaLogDirsResult_drain, METH_VARARGS,
     "Drain+destroy an AlterReplicaLogDirsResult handle into a dict"},
    {"DescribeReplicaLogDirsResult_drain", py_DescribeReplicaLogDirsResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeReplicaLogDirsResult handle into a dict"},
    {"Admin_elect_leaders_async", py_Admin_elect_leaders_async, METH_VARARGS,
     "Async electLeaders; cb(result_int, error_int)"},
    {"Admin_alter_partition_reassignments_async", py_Admin_alter_partition_reassignments_async,
     METH_VARARGS, "Async alterPartitionReassignments; cb(result_int, error_int)"},
    {"Admin_list_partition_reassignments_async", py_Admin_list_partition_reassignments_async,
     METH_VARARGS, "Async listPartitionReassignments; cb(result_int, error_int)"},
    {"Admin_list_offsets_async", py_Admin_list_offsets_async, METH_VARARGS,
     "Async listOffsets; cb(result_int, error_int)"},
    {"ElectLeadersResult_drain", py_ElectLeadersResult_drain, METH_VARARGS,
     "Drain+destroy an ElectLeadersResult handle into a dict"},
    {"AlterPartitionReassignmentsResult_drain", py_AlterPartitionReassignmentsResult_drain,
     METH_VARARGS, "Drain+destroy an AlterPartitionReassignmentsResult handle into a dict"},
    {"ListPartitionReassignmentsResult_drain", py_ListPartitionReassignmentsResult_drain,
     METH_VARARGS, "Drain+destroy a ListPartitionReassignmentsResult handle into a dict"},
    {"ListOffsetsResult_drain", py_ListOffsetsResult_drain, METH_VARARGS,
     "Drain+destroy a ListOffsetsResult handle into a dict"},
    {"MockAdminClient_update_consumer_group_offsets",
     py_MockAdminClient_update_consumer_group_offsets, METH_VARARGS,
     "Mock: seed committed consumer-group offsets; returns error_int"},
    {"Admin_list_groups_async", py_Admin_list_groups_async, METH_VARARGS,
     "Async listGroups; cb(result_int, error_int)"},
    {"Admin_describe_consumer_groups_async", py_Admin_describe_consumer_groups_async,
     METH_VARARGS, "Async describeConsumerGroups; cb(result_int, error_int)"},
    {"Admin_describe_classic_groups_async", py_Admin_describe_classic_groups_async, METH_VARARGS,
     "Async describeClassicGroups; cb(result_int, error_int)"},
    {"Admin_list_consumer_group_offsets_async", py_Admin_list_consumer_group_offsets_async,
     METH_VARARGS, "Async listConsumerGroupOffsets; cb(result_int, error_int)"},
    {"Admin_alter_consumer_group_offsets_async", py_Admin_alter_consumer_group_offsets_async,
     METH_VARARGS, "Async alterConsumerGroupOffsets; cb(result_int, error_int)"},
    {"Admin_delete_consumer_group_offsets_async", py_Admin_delete_consumer_group_offsets_async,
     METH_VARARGS, "Async deleteConsumerGroupOffsets; cb(result_int, error_int)"},
    {"Admin_delete_consumer_groups_async", py_Admin_delete_consumer_groups_async, METH_VARARGS,
     "Async deleteConsumerGroups; cb(result_int, error_int)"},
    {"Admin_remove_members_from_consumer_group_async",
     py_Admin_remove_members_from_consumer_group_async, METH_VARARGS,
     "Async removeMembersFromConsumerGroup; cb(result_int, error_int)"},
    {"ListGroupsResult_drain", py_ListGroupsResult_drain, METH_VARARGS,
     "Drain+destroy a ListGroupsResult handle into (valid, errors)"},
    {"DescribeConsumerGroupsResult_drain", py_DescribeConsumerGroupsResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeConsumerGroupsResult handle into a dict"},
    {"DescribeClassicGroupsResult_drain", py_DescribeClassicGroupsResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeClassicGroupsResult handle into a dict"},
    {"ListConsumerGroupOffsetsResult_drain", py_ListConsumerGroupOffsetsResult_drain,
     METH_VARARGS, "Drain+destroy a ListConsumerGroupOffsetsResult handle into a dict"},
    {"AlterConsumerGroupOffsetsResult_drain", py_AlterConsumerGroupOffsetsResult_drain,
     METH_VARARGS, "Drain+destroy an AlterConsumerGroupOffsetsResult handle into a dict"},
    {"DeleteConsumerGroupOffsetsResult_drain", py_DeleteConsumerGroupOffsetsResult_drain,
     METH_VARARGS, "Drain+destroy a DeleteConsumerGroupOffsetsResult handle into a dict"},
    {"DeleteConsumerGroupsResult_drain", py_DeleteConsumerGroupsResult_drain, METH_VARARGS,
     "Drain+destroy a DeleteConsumerGroupsResult handle into a dict"},
    {"RemoveMembersFromConsumerGroupResult_drain",
     py_RemoveMembersFromConsumerGroupResult_drain, METH_VARARGS,
     "Drain+destroy a RemoveMembersFromConsumerGroupResult handle into a dict"},
    {"Admin_create_acls_async", py_Admin_create_acls_async, METH_VARARGS,
     "Async createAcls; cb(result_int, error_int)"},
    {"Admin_describe_acls_async", py_Admin_describe_acls_async, METH_VARARGS,
     "Async describeAcls; cb(result_int, error_int)"},
    {"Admin_delete_acls_async", py_Admin_delete_acls_async, METH_VARARGS,
     "Async deleteAcls; cb(result_int, error_int)"},
    {"Admin_describe_client_quotas_async", py_Admin_describe_client_quotas_async, METH_VARARGS,
     "Async describeClientQuotas; cb(result_int, error_int)"},
    {"Admin_alter_client_quotas_async", py_Admin_alter_client_quotas_async, METH_VARARGS,
     "Async alterClientQuotas; cb(result_int, error_int)"},
    {"CreateAclsResult_drain", py_CreateAclsResult_drain, METH_VARARGS,
     "Drain+destroy a CreateAclsResult handle into a dict"},
    {"DescribeAclsResult_drain", py_DescribeAclsResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeAclsResult handle into a list"},
    {"DeleteAclsResult_drain", py_DeleteAclsResult_drain, METH_VARARGS,
     "Drain+destroy a DeleteAclsResult handle into a dict"},
    {"DescribeClientQuotasResult_drain", py_DescribeClientQuotasResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeClientQuotasResult handle into a dict"},
    {"AlterClientQuotasResult_drain", py_AlterClientQuotasResult_drain, METH_VARARGS,
     "Drain+destroy an AlterClientQuotasResult handle into a dict"},
    {"Admin_describe_user_scram_credentials_async", py_Admin_describe_user_scram_credentials_async,
     METH_VARARGS, "Async describeUserScramCredentials; cb(result_int, error_int)"},
    {"Admin_alter_user_scram_credentials_async", py_Admin_alter_user_scram_credentials_async,
     METH_VARARGS, "Async alterUserScramCredentials; cb(result_int, error_int)"},
    {"Admin_create_delegation_token_async", py_Admin_create_delegation_token_async, METH_VARARGS,
     "Async createDelegationToken; cb(result_int, error_int)"},
    {"Admin_renew_delegation_token_async", py_Admin_renew_delegation_token_async, METH_VARARGS,
     "Async renewDelegationToken; cb(result_int, error_int)"},
    {"Admin_expire_delegation_token_async", py_Admin_expire_delegation_token_async, METH_VARARGS,
     "Async expireDelegationToken; cb(result_int, error_int)"},
    {"Admin_describe_delegation_token_async", py_Admin_describe_delegation_token_async,
     METH_VARARGS, "Async describeDelegationToken; cb(result_int, error_int)"},
    {"Admin_describe_features_async", py_Admin_describe_features_async, METH_VARARGS,
     "Async describeFeatures; cb(result_int, error_int)"},
    {"Admin_update_features_async", py_Admin_update_features_async, METH_VARARGS,
     "Async updateFeatures; cb(result_int, error_int)"},
    {"MockAdminClient_set_feature_levels", py_MockAdminClient_set_feature_levels, METH_VARARGS,
     "Mock: seed the feature-level maps; returns error_int"},
    {"DescribeUserScramCredentialsResult_drain", py_DescribeUserScramCredentialsResult_drain,
     METH_VARARGS, "Drain+destroy a DescribeUserScramCredentialsResult handle into a dict"},
    {"AlterUserScramCredentialsResult_drain", py_AlterUserScramCredentialsResult_drain,
     METH_VARARGS, "Drain+destroy an AlterUserScramCredentialsResult handle into a dict"},
    {"CreateDelegationTokenResult_drain", py_CreateDelegationTokenResult_drain, METH_VARARGS,
     "Drain+destroy a CreateDelegationTokenResult handle into a token tuple"},
    {"RenewDelegationTokenResult_drain", py_RenewDelegationTokenResult_drain, METH_VARARGS,
     "Drain+destroy a RenewDelegationTokenResult handle into an int"},
    {"ExpireDelegationTokenResult_drain", py_ExpireDelegationTokenResult_drain, METH_VARARGS,
     "Drain+destroy an ExpireDelegationTokenResult handle into an int"},
    {"DescribeDelegationTokenResult_drain", py_DescribeDelegationTokenResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeDelegationTokenResult handle into a list"},
    {"DescribeFeaturesResult_drain", py_DescribeFeaturesResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeFeaturesResult handle into (finalized, epoch, supported)"},
    {"UpdateFeaturesResult_drain", py_UpdateFeaturesResult_drain, METH_VARARGS,
     "Drain+destroy an UpdateFeaturesResult handle into a dict"},
    {"Admin_describe_producers_async", py_Admin_describe_producers_async, METH_VARARGS,
     "Async describeProducers; cb(result_int, error_int)"},
    {"Admin_describe_transactions_async", py_Admin_describe_transactions_async, METH_VARARGS,
     "Async describeTransactions; cb(result_int, error_int)"},
    {"Admin_fence_producers_async", py_Admin_fence_producers_async, METH_VARARGS,
     "Async fenceProducers; cb(result_int, error_int)"},
    {"Admin_list_transactions_async", py_Admin_list_transactions_async, METH_VARARGS,
     "Async listTransactions; cb(result_int, error_int)"},
    {"Admin_abort_transaction_async", py_Admin_abort_transaction_async, METH_VARARGS,
     "Async abortTransaction; cb(error_int) -- Java's result carries no value"},
    {"Admin_force_terminate_transaction_async", py_Admin_force_terminate_transaction_async,
     METH_VARARGS,
     "Async forceTerminateTransaction; cb(error_int) -- Java's result carries no value"},
    {"DescribeProducersResult_drain", py_DescribeProducersResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeProducersResult handle into a dict"},
    {"DescribeTransactionsResult_drain", py_DescribeTransactionsResult_drain, METH_VARARGS,
     "Drain+destroy a DescribeTransactionsResult handle into a dict"},
    {"FenceProducersResult_drain", py_FenceProducersResult_drain, METH_VARARGS,
     "Drain+destroy a FenceProducersResult handle into a dict"},
    {"ListTransactionsResult_drain", py_ListTransactionsResult_drain, METH_VARARGS,
     "Drain+destroy a ListTransactionsResult handle into a dict"},
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
