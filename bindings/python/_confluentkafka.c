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

#define PRODUCER_RECORD_SLOT_THRESHOLD 1000
#define PRODUCER_RECORD_SLOT_CAPACITY (PRODUCER_RECORD_SLOT_THRESHOLD + 100)

// Backpressure bound: once this many records are accumulated but not yet taken
// by the send task, the producer is "full" and further enqueuing should wait
// until the send task drains a batch. One complete batch beyond the one being
// filled — mirrors Java's send() blocking once buffer.memory is full, applied
// here at batch granularity in front of the Rust accumulator.
#define PRODUCER_MAX_ACCUMULATED_RECORDS PRODUCER_RECORD_SLOT_THRESHOLD

// ProducerRecord C extension type
typedef struct {
    PyObject_HEAD
    PyObject* key;          // PyBytesObject or Py_None
    PyObject* value;        // PyBytesObject
    kafka_producer_ProducerRecord_t record_struct;
    char* topic_owned;      // owned copy of topic string
} ProducerRecordObject;

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
    self->record_struct.topic = NULL;
    self->record_struct.key = NULL;
    self->record_struct.value = NULL;
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
        memset(&self->record_struct, 0, sizeof(self->record_struct));
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

    // Populate record_struct
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
    self->record_struct.topic = self->topic_owned;
    self->record_struct.partition = partition;
    self->record_struct.timestamp = timestamp;

    if (key != Py_None) {
        self->record_struct.key = (const uint8_t*)PyBytes_AsString(key);
        self->record_struct.key_len = (int32_t)PyBytes_Size(key);
    } else {
        self->record_struct.key = NULL;
        self->record_struct.key_len = -1;
    }

    self->record_struct.value = (const uint8_t*)PyBytes_AsString(value);
    self->record_struct.value_len = (int32_t)PyBytes_Size(value);

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
    if (self->record_struct.partition == -1) {
        Py_RETURN_NONE;
    }
    return PyLong_FromLong(self->record_struct.partition);
}

static PyObject* ProducerRecord_get_timestamp(ProducerRecordObject* self, void* closure) {
    if (self->record_struct.timestamp == -1) {
        Py_RETURN_NONE;
    }
    return PyLong_FromLongLong(self->record_struct.timestamp);
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

// Python-callable constructor:
//   ConsumerGroupMetadata(group_id, generation_id, member_id, group_instance_id=None)
// Synthesises a fresh owned handle from the four fields via the FFI — the same
// path a caller that is not a consumer (e.g. a gRPC server rebuilding the
// metadata from the wire to drive send_offsets_to_transaction) uses; a consumer
// normally gets one from Consumer.group_metadata() instead. group_instance_id
// accepts str or None: an absent / None one (proto3 `optional`, a static member
// only) is passed to the FFI as NULL, becoming Java's Optional.empty(). The
// handle is freed exactly once in tp_dealloc, identically to the
// group_metadata() path — that path uses PyObject_New and this one tp_alloc, but
// both allocate an object freed via tp_free, so there is no leak and no
// double-free regardless of which created the object.
static PyObject* ConsumerGroupMetadata_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    static char* kwlist[] = {"group_id", "generation_id", "member_id",
                             "group_instance_id", NULL};
    const char* group_id = NULL;
    int generation_id = 0;
    const char* member_id = NULL;
    const char* group_instance_id = NULL;  // z: None / absent -> NULL
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "sis|z", kwlist,
                                     &group_id, &generation_id, &member_id,
                                     &group_instance_id)) {
        return NULL;
    }
    kafka_consumer_ConsumerGroupMetadata_t* handle =
        kafka_consumer_ConsumerGroupMetadata_new(
            group_id, generation_id, member_id, group_instance_id);
    if (handle == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Failed to create ConsumerGroupMetadata");
        return NULL;
    }
    ConsumerGroupMetadataObject* self =
        (ConsumerGroupMetadataObject*)type->tp_alloc(type, 0);
    if (self == NULL) {
        kafka_consumer_ConsumerGroupMetadata_destroy(handle);
        return NULL;
    }
    self->handle = handle;
    return (PyObject*)self;
}

static PyTypeObject ConsumerGroupMetadataType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_confluentkafka.ConsumerGroupMetadata",
    .tp_doc = "Consumer group membership metadata (owns a live Rust handle)",
    .tp_basicsize = sizeof(ConsumerGroupMetadataObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = ConsumerGroupMetadata_new,
    .tp_dealloc = (destructor)ConsumerGroupMetadata_dealloc,
    .tp_getset = ConsumerGroupMetadata_getsetters,
    .tp_repr = (reprfunc)ConsumerGroupMetadata_repr,
};

// Linked list node for tracking pending batches
typedef struct BatchNode {
    int count;
    ProducerRecordObject* producer_records[PRODUCER_RECORD_SLOT_CAPACITY];
    kafka_producer_ProducerRecord_t* producer_structs[PRODUCER_RECORD_SLOT_CAPACITY];
    PyObject* complete_cbs[PRODUCER_RECORD_SLOT_CAPACITY];
    kafka_producer_FutureRecordMetadata_t* futures[PRODUCER_RECORD_SLOT_CAPACITY];
    kafka_common_KafkaError_t* batch_errors[PRODUCER_RECORD_SLOT_CAPACITY];
    struct BatchNode* next_batch;
} BatchNode;

// Producer with background batch sending
typedef struct {
    PyObject *py_producer;
    kafka_producer_Producer_t *producer;
    int closed;
    int send_completed;
    BatchNode *next_batches_to_send;
    BatchNode *last_accumulating_batch;
    BatchNode* next_pending_batch;
    BatchNode* last_pending_batch;
    cnd_t record_batches_new_record_cnd;
    mtx_t record_batches_mutex;
    cnd_t pending_batches_available_cnd;
    mtx_t pending_batches_mutex;
    thrd_t send_thread;
    thrd_t poll_futures_thread;
    // Backpressure: records accumulated but not yet taken by the send task,
    // and the "space available" callbacks waiting for the next take. Both are
    // guarded by record_batches_mutex.
    int64_t accumulated_records;
    PyObject** space_cbs;
    int space_cbs_count;
    int space_cbs_capacity;
    // Test-only: when set, the send task stops draining accumulated batches so
    // backpressure can be exercised deterministically (the mock otherwise
    // accepts instantly and never fills). Always 0 in production.
    int test_paused;
} Producer;


static void Producer_complete_callback(PyObject* cb,
    ProducerRecordObject *record_obj,
    kafka_producer_RecordMetadata_t *metadata,
    kafka_common_KafkaError_t *error) {
    // Pass raw pointers as Python ints — the Python wrapper
    // calls accessor/destroy functions on them.
    PyObject *result_long = PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)metadata);
    PyObject *error_long  = PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)error);
    PyObject* result = PyObject_CallFunctionObjArgs(cb,
        result_long, error_long, NULL);
    Py_DECREF(cb);
    Py_DECREF(result_long);
    Py_DECREF(error_long);
    if (result) {
        Py_DECREF(result);
    } else {
        PyErr_Print();
    }
    Py_DECREF(record_obj);
}

static void Producer_complete_callbacks(
    PyObject **complete_cbs,
    ProducerRecordObject **result_objs,
    kafka_producer_FutureRecordMetadata_t **futures,
    int count) {
    // Phase 1: Block on all futures at once WITHOUT the GIL.
    // Uses a single tokio runtime for the entire batch.
    kafka_producer_RecordMetadata_t *metadata_ptrs[PRODUCER_RECORD_SLOT_CAPACITY];
    kafka_common_KafkaError_t *error_ptrs[PRODUCER_RECORD_SLOT_CAPACITY];
    kafka_producer_FutureRecordMetadata_get_all(
        futures, count, metadata_ptrs, error_ptrs);
    kafka_producer_FutureRecordMetadata_destroy_all(futures, count);

    // Phase 2: Acquire GIL and dispatch Python callbacks.
    // Ownership of metadata/error handles transfers to the callback.
    PyGILState_STATE gstate = PyGILState_Ensure();
    for (int i = 0; i < count; i++) {
        Producer_complete_callback(complete_cbs[i], result_objs[i],
                                   metadata_ptrs[i], error_ptrs[i]);
    }
    PyGILState_Release(gstate);
}


// Invoke each pending "space available" callback (resolving the Python-side
// space Future) and release the array. The caller must have detached the array
// from the producer under record_batches_mutex first, so this runs without that
// lock held. Acquires the GIL to call into Python.
static void Producer_fire_and_free_space_cbs(PyObject** cbs, int count) {
    if (cbs == NULL) {
        return;
    }
    PyGILState_STATE gstate = PyGILState_Ensure();
    for (int i = 0; i < count; i++) {
        PyObject* result = PyObject_CallFunctionObjArgs(cbs[i], NULL);
        if (result) {
            Py_DECREF(result);
        } else {
            PyErr_Print();
        }
        Py_DECREF(cbs[i]);
    }
    PyGILState_Release(gstate);
    PyMem_RawFree(cbs);
}

// Detach the pending space-callback array under record_batches_mutex. Returns
// the array (caller owns it) via out params and resets the producer's fields.
// The mutex MUST be held by the caller.
static void Producer_take_space_cbs_locked(Producer* producer,
    PyObject*** out_cbs, int* out_count) {
    *out_cbs = producer->space_cbs;
    *out_count = producer->space_cbs_count;
    producer->space_cbs = NULL;
    producer->space_cbs_count = 0;
    producer->space_cbs_capacity = 0;
}

static int Producer_poll_futures_thread(void* arg) {
    Producer* producer = (Producer*)arg;

    BatchNode* current_pending_batch = NULL;
    while (!producer->send_completed || current_pending_batch != NULL)
    {
        BatchNode* batch_to_free = NULL;
        if (current_pending_batch != NULL) {
            Producer_complete_callbacks(
                &current_pending_batch->complete_cbs[0],
                &current_pending_batch->producer_records[0],
                &current_pending_batch->futures[0],
                current_pending_batch->count
            );
            batch_to_free = current_pending_batch;
        }

        mtx_lock(&producer->pending_batches_mutex);
        if (current_pending_batch != NULL) {
            if (current_pending_batch == producer->last_pending_batch)
                producer->last_pending_batch = NULL;
            producer->next_pending_batch = current_pending_batch->next_batch;
        }
        
        while (producer->next_pending_batch == NULL && !producer->send_completed) {
            cnd_wait(&producer->pending_batches_available_cnd,
                     &producer->pending_batches_mutex);
        }
        current_pending_batch = producer->next_pending_batch;
        mtx_unlock(&producer->pending_batches_mutex);
        PyMem_RawFree(batch_to_free);
    }

    return thrd_success;
}

static int64_t current_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec * 1000000000L) + ts.tv_nsec;
}


static int Producer_send_thread(void* arg) {
    Producer* producer = (Producer*)arg;

    thrd_create(&producer->poll_futures_thread,
        Producer_poll_futures_thread, producer);

    while (!producer->closed) {
        int64_t timeout, now;
        BatchNode *batch_node, *head_batch_node = NULL, *tail_batch_node = NULL;

        mtx_lock(&producer->record_batches_mutex);
        now = current_time_ns();
        timeout = now + 10000000; // 10ms
        while ((
            producer->next_batches_to_send == NULL
            || producer->next_batches_to_send->count < PRODUCER_RECORD_SLOT_THRESHOLD
        ) && timeout > now && !producer->closed) {
            struct timespec ts;

            timespec_get(&ts, TIME_UTC);
            ts.tv_nsec += timeout + 1000 - now;
            if (ts.tv_nsec >= 1000000000) {
                ts.tv_sec += 1;
                ts.tv_nsec -= 1000000000;
            }
            cnd_timedwait(&producer->record_batches_new_record_cnd,
                          &producer->record_batches_mutex, &ts);
            now = current_time_ns();
        }

        // Test-only: while paused, do not drain — let accumulation build so
        // backpressure (py_Producer_on_space_available) can be tested.
        if (producer->test_paused) {
            mtx_unlock(&producer->record_batches_mutex);
            struct timespec pause_ts = {0, 5000000};  // 5ms
            thrd_sleep(&pause_ts, NULL);
            continue;
        }

        if (producer->next_batches_to_send == NULL) {
            mtx_unlock(&producer->record_batches_mutex);
            continue;
        }

        head_batch_node = producer->next_batches_to_send;
        tail_batch_node = producer->last_accumulating_batch;
        producer->next_batches_to_send = NULL;
        producer->last_accumulating_batch = NULL;
        // Accumulation has been taken: capacity is free again. Reset the
        // backpressure counter and wake any senders waiting for space.
        producer->accumulated_records = 0;
        PyObject** space_cbs;
        int space_cbs_count;
        Producer_take_space_cbs_locked(producer, &space_cbs, &space_cbs_count);
        mtx_unlock(&producer->record_batches_mutex);

        Producer_fire_and_free_space_cbs(space_cbs, space_cbs_count);

        batch_node = head_batch_node;

        while (batch_node != NULL) {
            // Build a flat array of records for send_batch
            kafka_producer_ProducerRecord_t flat_records[PRODUCER_RECORD_SLOT_CAPACITY];
            for (int i = 0; i < batch_node->count; i++) {
                flat_records[i] = *batch_node->producer_structs[i];
            }

            memset(batch_node->futures, 0, sizeof(batch_node->futures[0]) * batch_node->count);
            memset(batch_node->batch_errors, 0, sizeof(batch_node->batch_errors[0]) * batch_node->count);

            kafka_producer_Producer_send_batch(
                producer->producer,
                flat_records,
                batch_node->count,
                batch_node->futures,
                batch_node->batch_errors);

            // Handle immediate errors from send_batch
            int errors_found = 0;
            {
                PyGILState_STATE gstate = PyGILState_Ensure();
                for (int i = 0; i < batch_node->count; i++) {
                    if (batch_node->batch_errors[i] != NULL) {
                        if (batch_node->futures[i] != NULL) {
                            kafka_producer_FutureRecordMetadata_destroy(batch_node->futures[i]);
                            batch_node->futures[i] = NULL;
                        }
                        // Pass error pointer to callback; ownership transfers
                        Producer_complete_callback(batch_node->complete_cbs[i],
                            batch_node->producer_records[i],
                            NULL, batch_node->batch_errors[i]);
                        batch_node->batch_errors[i] = NULL;
                        errors_found++;
                    } else if (errors_found > 0) {
                        batch_node->complete_cbs[i - errors_found] = batch_node->complete_cbs[i];
                        batch_node->producer_records[i - errors_found] = batch_node->producer_records[i];
                        batch_node->producer_structs[i - errors_found] = batch_node->producer_structs[i];
                        batch_node->futures[i - errors_found] = batch_node->futures[i];
                    }
                }
                batch_node->count -= errors_found;
                PyGILState_Release(gstate);
            }

            batch_node = batch_node->next_batch;
        }
        mtx_lock(&producer->pending_batches_mutex);
        if (producer->last_pending_batch) {
            producer->last_pending_batch->next_batch = head_batch_node;
            producer->last_pending_batch = tail_batch_node;
        } else {
            producer->next_pending_batch = head_batch_node;
            producer->last_pending_batch = tail_batch_node;
        }
        cnd_signal(&producer->pending_batches_available_cnd);
        mtx_unlock(&producer->pending_batches_mutex);
    }
    mtx_lock(&producer->pending_batches_mutex);
    producer->send_completed = 1;
    cnd_signal(&producer->pending_batches_available_cnd);
    mtx_unlock(&producer->pending_batches_mutex);
    // Flush so every in-flight record resolves before we join the poll-futures
    // task. Without this the poll task can block forever in
    // FutureRecordMetadata_get_all on a record that never completes on its own
    // (e.g. a MockProducer with auto_complete disabled), deadlocking the join
    // below. Java's flush() likewise completes outstanding records. We hold no
    // GIL here (background C task), so the blocking flush does not stall the
    // event loop.
    kafka_producer_Producer_flush(producer->producer, NULL);
    thrd_join(producer->poll_futures_thread, NULL);

    return thrd_success;
}

// Batching Producer functions
static PyObject* py_Producer_new(PyObject* self, PyObject* args) {
    int auto_complete;
    PyObject *producer_obj;
    Producer* producer = (Producer*)PyMem_Malloc(sizeof(Producer));
    if (!producer) {
        return PyErr_NoMemory();
    }

    if (!PyArg_ParseTuple(args, "pO", &auto_complete, &producer_obj)) {
        PyMem_Free(producer);
        return NULL;
    }

    memset(producer, 0, sizeof(Producer));
    producer->closed = 0;

    mtx_init(&producer->record_batches_mutex, mtx_plain);
    cnd_init(&producer->record_batches_new_record_cnd);

    mtx_init(&producer->pending_batches_mutex, mtx_plain);
    cnd_init(&producer->pending_batches_available_cnd);
    producer->next_batches_to_send = NULL;
    producer->last_accumulating_batch = NULL;
    producer->next_pending_batch = NULL;
    producer->last_pending_batch = NULL;
    producer->py_producer = producer_obj;
    Py_INCREF(producer->py_producer);

    producer->producer = kafka_producer_MockProducer_new(auto_complete ? true : false);
    if (producer->producer == NULL) {
        Py_DECREF(producer->py_producer);
        PyMem_Free(producer);
        PyErr_SetString(PyExc_RuntimeError, "Failed to create MockProducer");
        return NULL;
    }

    thrd_create(&producer->send_thread, Producer_send_thread, producer);

    return PyLong_FromVoidPtr(producer);
}

static PyObject* py_KafkaProducer_new(PyObject* self, PyObject* args) {
    PyObject *config_dict;
    PyObject *producer_obj;

    if (!PyArg_ParseTuple(args, "OO", &config_dict, &producer_obj)) {
        return NULL;
    }

    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }

    kafka_producer_ProducerProperties_t *props =
        kafka_producer_ProducerProperties_new();
    if (props == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ProducerProperties");
        return NULL;
    }

    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char *k = PyUnicode_AsUTF8(key);
        const char *v = PyUnicode_AsUTF8(value);
        if (k == NULL || v == NULL) {
            kafka_producer_ProducerProperties_destroy(props);
            PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
        kafka_producer_ProducerProperties_put(props, k, v);
    }

    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_t *kafka_producer =
        kafka_producer_KafkaProducer_new(props, &err);
    kafka_producer_ProducerProperties_destroy(props);

    if (kafka_producer == NULL) {
        if (err != NULL) {
            const char *msg = kafka_common_KafkaError_message(err);
            PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create KafkaProducer");
            kafka_common_KafkaError_destroy(err);
        } else {
            PyErr_SetString(PyExc_RuntimeError, "Failed to create KafkaProducer");
        }
        return NULL;
    }

    Producer* producer = (Producer*)PyMem_Malloc(sizeof(Producer));
    if (!producer) {
        kafka_producer_Producer_close(kafka_producer, NULL);
        kafka_producer_Producer_destroy(kafka_producer);
        return PyErr_NoMemory();
    }

    memset(producer, 0, sizeof(Producer));
    producer->closed = 0;

    mtx_init(&producer->record_batches_mutex, mtx_plain);
    cnd_init(&producer->record_batches_new_record_cnd);

    mtx_init(&producer->pending_batches_mutex, mtx_plain);
    cnd_init(&producer->pending_batches_available_cnd);
    producer->next_batches_to_send = NULL;
    producer->last_accumulating_batch = NULL;
    producer->next_pending_batch = NULL;
    producer->last_pending_batch = NULL;
    producer->py_producer = producer_obj;
    Py_INCREF(producer->py_producer);

    producer->producer = kafka_producer;

    thrd_create(&producer->send_thread, Producer_send_thread, producer);

    return PyLong_FromVoidPtr(producer);
}

static PyObject* py_Producer_send(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject *record_obj, *complete_cb;

    if (!PyArg_ParseTuple(args, "KOO", &producer_ptr, &record_obj, &complete_cb)) {
        return NULL;
    }

    if (!PyObject_TypeCheck(record_obj, &ProducerRecordType)) {
        PyErr_SetString(PyExc_TypeError, "Record must be a ProducerRecord object");
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;

    if (producer->closed) {
        PyErr_SetString(PyExc_RuntimeError, "Producer is closed");
        return NULL;
    }

    ProducerRecordObject* record = (ProducerRecordObject*)record_obj;

    // Increment refcounts while we still hold the GIL
    Py_INCREF(record);
    Py_INCREF(complete_cb);

    int full = 0;
    Py_BEGIN_ALLOW_THREADS
    mtx_lock(&producer->record_batches_mutex);
    if (!producer->last_accumulating_batch || producer->last_accumulating_batch->count == PRODUCER_RECORD_SLOT_CAPACITY) {
        BatchNode* new_batch = (BatchNode*)PyMem_RawMalloc(sizeof(BatchNode));
        new_batch->count = 0;
        new_batch->next_batch = NULL;
        if (producer->last_accumulating_batch) {
            producer->last_accumulating_batch->next_batch = new_batch;
            producer->last_accumulating_batch = new_batch;
        } else {
            producer->next_batches_to_send = new_batch;
            producer->last_accumulating_batch = new_batch;
        }
    }

    producer->last_accumulating_batch->producer_records[producer->last_accumulating_batch->count] = record;
    producer->last_accumulating_batch->complete_cbs[producer->last_accumulating_batch->count] = complete_cb;
    producer->last_accumulating_batch->producer_structs[producer->last_accumulating_batch->count] = &record->record_struct;
    producer->last_accumulating_batch->count++;
    producer->accumulated_records++;

    if (producer->last_accumulating_batch->count >= PRODUCER_RECORD_SLOT_THRESHOLD) {
        cnd_signal(&producer->record_batches_new_record_cnd);
    }
    // Report whether the producer is now over the backpressure bound, so the
    // caller can wait for space (see py_Producer_on_space_available).
    full = producer->accumulated_records >= PRODUCER_MAX_ACCUMULATED_RECORDS;
    mtx_unlock(&producer->record_batches_mutex);
    Py_END_ALLOW_THREADS
    return PyBool_FromLong(full ? 1 : 0);
}

// Register a "space available" callback to be invoked when the send task next
// drains accumulated batches (freeing capacity). Returns True if space is
// already available (the caller need not wait), False if `space_cb` was
// registered and will be called later. The check-and-register is done under
// record_batches_mutex — the same lock the send task holds when it takes
// batches — so there is no lost-wakeup window. Called by the Python `send`
// only after `Producer_send` reported the producer full.
static PyObject* py_Producer_on_space_available(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* space_cb;

    if (!PyArg_ParseTuple(args, "KO", &producer_ptr, &space_cb)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;

    int available = 0;
    Py_INCREF(space_cb);
    Py_BEGIN_ALLOW_THREADS
    mtx_lock(&producer->record_batches_mutex);
    if (producer->closed
        || producer->accumulated_records < PRODUCER_MAX_ACCUMULATED_RECORDS) {
        // Space already freed (or producer closing): don't make the caller wait.
        available = 1;
    } else {
        if (producer->space_cbs_count == producer->space_cbs_capacity) {
            int new_capacity = producer->space_cbs_capacity
                ? producer->space_cbs_capacity * 2 : 8;
            producer->space_cbs = (PyObject**)PyMem_RawRealloc(
                producer->space_cbs, new_capacity * sizeof(PyObject*));
            producer->space_cbs_capacity = new_capacity;
        }
        producer->space_cbs[producer->space_cbs_count++] = space_cb;
    }
    mtx_unlock(&producer->record_batches_mutex);
    Py_END_ALLOW_THREADS

    if (available) {
        Py_DECREF(space_cb);  // not stored
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

// Test-only: pause/resume the send task's draining so backpressure can be
// exercised deterministically (the mock accepts instantly and would never
// fill the buffer otherwise). Not part of the public API.
static PyObject* py_Producer_test_set_paused(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    int paused;

    if (!PyArg_ParseTuple(args, "Kp", &producer_ptr, &paused)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;
    mtx_lock(&producer->record_batches_mutex);
    producer->test_paused = paused ? 1 : 0;
    cnd_signal(&producer->record_batches_new_record_cnd);
    mtx_unlock(&producer->record_batches_mutex);
    Py_RETURN_NONE;
}

// ---- async producer op trampolines -----------------------------------------
// These sit here so they precede the producer wrappers that reference them
// (fire_handle_cb / consumer_op_trampoline live later in the file, after the
// consumer section, so we can't reuse them from the producer wrappers above).

// cb(error_int): void-returning producer async op (flush, close). Mirrors the
// consumer's consumer_op_trampoline.
static void producer_op_trampoline(kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "K", (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

// cb(list_int, error_int): producer partitions_for async. The list handle is a
// kafka_consumer_PartitionInfoList_t (shared with the consumer FFI) that Python
// drains via PartitionInfoList_drain.
static void producer_partitions_for_trampoline(kafka_consumer_PartitionInfoList_t* list,
                                               kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "KK",
        (unsigned long long)(uintptr_t)list,
        (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

// Close is split into three Python-visible steps so the Rust-side close can be
// awaited/interrupted from Python exactly like flush, instead of blocking in a
// C-level wait:
//   1. Producer_shutdown   — stop + join the C batching threads (blocking C
//                            join; GIL released), fire pending space waiters,
//                            tear down the C mutexes/cnds, drop the py_producer
//                            self-reference. Does NOT touch the Rust producer.
//   2. Producer_close_async — drive kafka_producer_Producer_close_async; the
//                            Python wrapper waits on it via _run_sync /
//                            _run_async (interruptible), like flush.
//   3. Producer_destroy    — free the Rust producer handle and the C struct.
// The Python Producer.close() / AsyncProducer.close() orchestrate the three in
// order (idempotency is guarded Python-side by self.closed).

static PyObject* py_Producer_shutdown(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) {
        return NULL;
    }
    Producer* producer = (Producer*)producer_ptr;

    if (producer->closed) {
        Py_RETURN_NONE;
    }

    // Signal the send thread to stop, then join it (releasing the GIL
    // so the background threads can acquire it for callbacks).
    PyObject** space_cbs = NULL;
    int space_cbs_count = 0;
    Py_BEGIN_ALLOW_THREADS
    mtx_lock(&producer->record_batches_mutex);
    producer->closed = 1;
    // Take any pending space waiters so blocked senders unblock on close
    // instead of hanging (whoever wins the lock — here or the send task's
    // final take — fires them once).
    Producer_take_space_cbs_locked(producer, &space_cbs, &space_cbs_count);
    cnd_signal(&producer->record_batches_new_record_cnd);
    mtx_unlock(&producer->record_batches_mutex);
    thrd_join(producer->send_thread, NULL);
    Py_END_ALLOW_THREADS

    Producer_fire_and_free_space_cbs(space_cbs, space_cbs_count);

    // Clean up after threads have stopped
    cnd_destroy(&producer->record_batches_new_record_cnd);
    mtx_destroy(&producer->record_batches_mutex);
    cnd_destroy(&producer->pending_batches_available_cnd);
    mtx_destroy(&producer->pending_batches_mutex);
    Py_DECREF(producer->py_producer);

    Py_RETURN_NONE;
}

// Drive the Rust-side close asynchronously; cb(error_int) fires on the
// dispatcher thread. The Python wrapper waits via _run_sync / _run_async so a
// stuck close stays interruptible on the main thread (like flush). Must be
// called after Producer_shutdown (the C batching threads are already joined).
static PyObject* py_Producer_close_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &producer_ptr, &cb)) {
        return NULL;
    }
    Producer* producer = (Producer*)producer_ptr;
    Py_INCREF(cb);
    kafka_producer_Producer_close_async(producer->producer, producer_op_trampoline, cb);
    Py_RETURN_NONE;
}

// Free the Rust producer handle and the C struct. Call after the close future
// (Producer_close_async) has completed.
static PyObject* py_Producer_destroy(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) {
        return NULL;
    }
    Producer* producer = (Producer*)producer_ptr;
    Py_BEGIN_ALLOW_THREADS
    kafka_producer_Producer_destroy(producer->producer);
    Py_END_ALLOW_THREADS
    PyMem_Free(producer);
    Py_RETURN_NONE;
}

// MockProducer-specific functions
static PyObject* py_MockProducer_complete_next(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;

    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;
    bool result = kafka_producer_MockProducer_complete_next(producer->producer);
    return PyBool_FromLong(result ? 1 : 0);
}

static PyObject* py_MockProducer_error_next(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    int32_t error_code;
    const char *error_message = NULL;

    if (!PyArg_ParseTuple(args, "Kiz", &producer_ptr, &error_code, &error_message)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;
    bool result = kafka_producer_MockProducer_error_next(
        producer->producer, error_code, error_message);
    return PyBool_FromLong(result ? 1 : 0);
}

static PyObject* py_MockProducer_history_count(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;

    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;
    int32_t count = kafka_producer_MockProducer_history_count(producer->producer);
    return PyLong_FromLong(count);
}

static PyObject* py_MockProducer_clear(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;

    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;
    kafka_producer_MockProducer_clear(producer->producer);
    Py_RETURN_NONE;
}

// ---- MockProducer transaction test hooks (unit tests only) -----------------

// Install (or clear) the error the mock's commitTransaction returns. Mirrors the
// FFI hook used to exercise the abortable-commit path. clear=True removes any
// installed error (error_code/message ignored). Returns True if applied.
static PyObject* py_MockProducer_set_commit_transaction_error(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    int clear;
    int error_code;
    const char* message = NULL;
    if (!PyArg_ParseTuple(args, "Kpiz", &producer_ptr, &clear, &error_code, &message)) {
        return NULL;
    }
    Producer* producer = (Producer*)producer_ptr;
    bool ok = kafka_producer_MockProducer_set_commit_transaction_error(
        producer->producer, clear ? true : false, error_code, message);
    return PyBool_FromLong(ok ? 1 : 0);
}

// Whether the mock has staged consumer-group offsets in the current transaction
// (Java MockProducer.sentOffsets()).
static PyObject* py_MockProducer_sent_offsets(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    return PyBool_FromLong(
        kafka_producer_MockProducer_sent_offsets(producer->producer) ? 1 : 0);
}

// Look up the offset a committed transaction staged for (group_id, topic,
// partition). Returns (offset, leader_epoch, metadata) or None if not found —
// so a test can verify the send_offsets_to_transaction round-trip.
static PyObject* py_MockProducer_committed_offset(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    const char* group_id;
    const char* topic;
    int partition;
    if (!PyArg_ParseTuple(args, "Kssi", &producer_ptr, &group_id, &topic, &partition)) {
        return NULL;
    }
    Producer* producer = (Producer*)producer_ptr;
    int64_t offset = 0;
    int32_t leader_epoch = -1;
    char metadata[512];
    metadata[0] = '\0';
    bool found = kafka_producer_MockProducer_committed_offset(
        producer->producer, group_id, topic, partition,
        &offset, &leader_epoch, metadata, (int32_t)sizeof(metadata));
    if (!found) Py_RETURN_NONE;
    return Py_BuildValue("(Lis)", (long long)offset, leader_epoch, metadata);
}

static PyObject* py_Producer_flush(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;

    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;
    kafka_common_KafkaError_t *err = NULL;
    // Blocking FFI call: release the GIL. flush() parks until every in-flight
    // send completes, and each completion fires the on_delivery trampoline on
    // the dispatcher thread, which needs the GIL.
    Py_BEGIN_ALLOW_THREADS
    kafka_producer_Producer_flush(producer->producer, &err);
    Py_END_ALLOW_THREADS
    if (err != NULL) {
        return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)err);
    }
    return PyLong_FromLong(0);
}

// Producer partitions_for: returns (list_handle_int, error_int). The list
// handle is a kafka_consumer_PartitionInfoList_t (shared with the consumer FFI)
// that Python drains via PartitionInfoList_drain.
static PyObject* py_Producer_partitions_for(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    const char* topic;
    if (!PyArg_ParseTuple(args, "Ks", &producer_ptr, &topic)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    kafka_consumer_PartitionInfoList_t* list = NULL;
    kafka_common_KafkaError_t* err;
    // Blocking FFI call (metadata round trip): release the GIL, so a delivery
    // callback firing meanwhile can take it.
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_partitions_for(producer->producer, topic, &list);
    Py_END_ALLOW_THREADS
    return Py_BuildValue("KK",
        (unsigned long long)(uintptr_t)list,
        (unsigned long long)(uintptr_t)err);
}

// Producer_metrics -> list[dict] with keys name/group/description/tags/value/kind.
//
// Mirrors py_Consumer_metrics exactly, over the producer's namespaced
// kafka_producer_MetricMap_* accessors. A list of dicts (rather than a dict
// keyed by name) keeps the MetricName identity intact: two metrics share a name
// and group and differ only by tags. `value` is float / str / int depending on
// the kind reported by kafka_producer_MetricMap_get_value_kind (0=double,
// 1=string, 2=long, 3=int); `kind` is carried through so the caller can
// distinguish Long from Int (both surface as Python int).
static PyObject* py_Producer_metrics(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    kafka_producer_MetricMap_t* map =
        kafka_producer_Producer_metrics(producer->producer);
    if (map == NULL) Py_RETURN_NONE;  // null producer / no snapshot
    int32_t n = kafka_producer_MetricMap_count(map);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_producer_MetricMap_destroy(map); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* tags = PyDict_New();
        if (tags == NULL) goto fail;
        int32_t tn = kafka_producer_MetricMap_get_tag_count(map, i);
        for (int32_t t = 0; t < tn; t++) {
            const char* k = kafka_producer_MetricMap_get_tag_key(map, i, t);
            const char* v = kafka_producer_MetricMap_get_tag_value(map, i, t);
            PyObject* pv = PyUnicode_FromString(v ? v : "");
            if (pv == NULL) { Py_DECREF(tags); goto fail; }
            if (PyDict_SetItemString(tags, k ? k : "", pv) != 0) {
                Py_DECREF(pv); Py_DECREF(tags); goto fail;
            }
            Py_DECREF(pv);
        }
        PyObject* value = NULL;
        int32_t kind = kafka_producer_MetricMap_get_value_kind(map, i);
        switch (kind) {
            case 1: {
                const char* s = kafka_producer_MetricMap_get_value_string(map, i);
                value = PyUnicode_FromString(s ? s : "");
                break;
            }
            case 2:
                value = PyLong_FromLongLong(
                    (long long)kafka_producer_MetricMap_get_value_long(map, i));
                break;
            case 3:
                value = PyLong_FromLong((long)kafka_producer_MetricMap_get_value_int(map, i));
                break;
            default:
                value = PyFloat_FromDouble(kafka_producer_MetricMap_get_value_double(map, i));
                break;
        }
        if (value == NULL) { Py_DECREF(tags); goto fail; }
        const char* name = kafka_producer_MetricMap_get_name(map, i);
        const char* group = kafka_producer_MetricMap_get_group(map, i);
        const char* desc = kafka_producer_MetricMap_get_description(map, i);
        // "N" steals the reference to tags/value, so they are not leaked here.
        PyObject* entry = Py_BuildValue("{s:s,s:s,s:s,s:N,s:N,s:i}",
            "name", name ? name : "",
            "group", group ? group : "",
            "description", desc ? desc : "",
            "tags", tags,
            "value", value,
            "kind", (int)kind);
        if (entry == NULL) goto fail;
        PyList_SET_ITEM(out, i, entry);
    }
    kafka_producer_MetricMap_destroy(map);
    return out;
fail:
    Py_DECREF(out);
    kafka_producer_MetricMap_destroy(map);
    return NULL;
}

// Producer flush (async): submit and return; cb(error_int) fires on the
// dispatcher thread. The Python wrapper waits (threading.Event / asyncio.Future)
// so no Rust block_on parks the calling thread — mirrors the consumer.
static PyObject* py_Producer_flush_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &producer_ptr, &cb)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    Py_INCREF(cb);
    kafka_producer_Producer_flush_async(producer->producer, producer_op_trampoline, cb);
    Py_RETURN_NONE;
}

// Producer partitions_for (async): submit and return; cb(list_int, error_int)
// fires on the dispatcher thread.
static PyObject* py_Producer_partitions_for_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    const char* topic;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsO", &producer_ptr, &topic, &cb)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    Py_INCREF(cb);
    kafka_producer_Producer_partitions_for_async(producer->producer, topic,
        producer_partitions_for_trampoline, cb);
    Py_RETURN_NONE;
}

// ---- Producer transaction control ops (sync) -------------------------------
//
// Thin 1:1 bindings of the SYNCHRONOUS transaction FFI: each returns the error
// handle *directly* (null = success; Python owns and frees a non-null handle via
// KafkaError._from_c / KafkaError_destroy). The blocking ops
// (init/commit/abort/send_offsets are block_on in the FFI) release the GIL;
// begin_transaction is a pure state transition and keeps it.
//
// producer.py's transaction API is async-first and drives the *_async variants
// (further below) instead, via _run_sync / _run_async, so it no longer calls
// these. They are retained as a faithful binding of the synchronous FFI surface
// (also exercised by the C and gRPC layers), not because Python uses them.
//
// §13 (merged): a transaction-control op drains any returned async sends into
// the transaction first, exactly as flush/close do (committed on commit,
// discarded on abort); the earlier "async-in-transaction is unsupported UB" note
// is obsolete. Python's own send() is synchronous (registers before it returns)
// and Python does not expose an async/outbox send path.

static PyObject* py_Producer_init_transactions(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    kafka_common_KafkaError_t* err = NULL;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_init_transactions(producer->producer);
    Py_END_ALLOW_THREADS
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)err);
}

static PyObject* py_Producer_begin_transaction(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    // Pure state transition, never waits (Java beginTransaction) — no GIL release.
    kafka_common_KafkaError_t* err =
        kafka_producer_Producer_begin_transaction(producer->producer);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)err);
}

static PyObject* py_Producer_commit_transaction(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    kafka_common_KafkaError_t* err = NULL;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_commit_transaction(producer->producer);
    Py_END_ALLOW_THREADS
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)err);
}

static PyObject* py_Producer_abort_transaction(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    kafka_common_KafkaError_t* err = NULL;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_abort_transaction(producer->producer);
    Py_END_ALLOW_THREADS
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)err);
}

// send_offsets_to_transaction(producer, offsets, group_metadata):
//   offsets: list[(topic, partition, offset, leader_epoch|-1, metadata|None)]
//   group_metadata: a ConsumerGroupMetadata object (owns the live handle).
// Marshals the offsets into the parallel arrays the FFI expects, exactly like
// py_Consumer_commit_sync_offsets_async, then makes the blocking FFI call with
// the GIL released. The group-metadata handle is borrowed for the call; the
// object stays alive as a live argument, so it outlives the call (no
// use-after-free). An empty offsets list is a legitimate count == 0.
static PyObject* py_Producer_send_offsets_to_transaction(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* offsets;
    PyObject* gm_obj;
    if (!PyArg_ParseTuple(args, "KOO", &producer_ptr, &offsets, &gm_obj)) return NULL;

    // Extract the borrowed group-metadata handle (typecheck matches the
    // ProducerRecord precedent in py_Producer_send, and prevents dereferencing an
    // arbitrary object's memory).
    if (!PyObject_TypeCheck(gm_obj, &ConsumerGroupMetadataType)) {
        PyErr_SetString(PyExc_TypeError,
            "group_metadata must be a ConsumerGroupMetadata object");
        return NULL;
    }
    Producer* producer = (Producer*)producer_ptr;
    kafka_consumer_ConsumerGroupMetadata_t* gm =
        ((ConsumerGroupMetadataObject*)gm_obj)->handle;

    Py_ssize_t n = PySequence_Size(offsets);
    if (n < 0) return NULL;
    const char** topics = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    int32_t* parts = n > 0 ? PyMem_Malloc(n * sizeof(int32_t)) : NULL;
    int64_t* offs = n > 0 ? PyMem_Malloc(n * sizeof(int64_t)) : NULL;
    int32_t* epochs = n > 0 ? PyMem_Malloc(n * sizeof(int32_t)) : NULL;
    const char** metas = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    if (n > 0 && (!topics || !parts || !offs || !epochs || !metas)) {
        PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(offs); PyMem_Free(epochs); PyMem_Free(metas);
        return PyErr_NoMemory();
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(offsets, i);  // new ref
        const char* t = NULL; int p = 0; long long o = 0; int e = -1; PyObject* meta = Py_None;
        int ok = item && PyArg_ParseTuple(item, "siL|iO", &t, &p, &o, &e, &meta);
        if (ok) {
            // A non-None metadata that is not a str makes PyUnicode_AsUTF8 return
            // NULL and set a TypeError. Bail here (ok = 0) so the arrays are freed
            // and we return NULL *before* the FFI call — otherwise the offsets
            // would be staged with the metadata silently dropped, and the wrapper
            // would return a PyLong with an exception still pending (a confusing
            // SystemError). PyUnicode_AsUTF8("") returns a valid pointer, so a
            // legitimate empty-string metadata is unaffected. The returned pointer
            // borrows meta's internal buffer, kept alive by the caller's list for
            // the whole synchronous FFI call (the DECREF below only drops our own
            // new reference from PySequence_GetItem).
            const char* m = NULL;
            if (meta != Py_None && !(m = PyUnicode_AsUTF8(meta))) {
                ok = 0;
            } else {
                topics[i] = t; parts[i] = p; offs[i] = o; epochs[i] = e;
                metas[i] = m;
            }
        }
        Py_XDECREF(item);
        if (!ok) {
            PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(offs); PyMem_Free(epochs); PyMem_Free(metas);
            return NULL;
        }
    }
    kafka_common_KafkaError_t* err = NULL;
    Py_BEGIN_ALLOW_THREADS
    err = kafka_producer_Producer_send_offsets_to_transaction(
        producer->producer, topics, parts, offs, epochs, metas, (int32_t)n, gm);
    Py_END_ALLOW_THREADS
    PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(offs); PyMem_Free(epochs); PyMem_Free(metas);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)err);
}

// RecordMetadata destroy and copy functions
static PyObject* py_RecordMetadata_destroy(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_producer_RecordMetadata_t *m = (kafka_producer_RecordMetadata_t*)(uintptr_t)ptr;
    kafka_producer_RecordMetadata_destroy(m);
    Py_RETURN_NONE;
}

// RecordMetadata copy-via-callback
static void record_metadata_copy_callback(int64_t offset, int32_t partition,
                                          const char* topic, int64_t timestamp,
                                          void* user_data) {
    PyObject* callback = (PyObject*)user_data;
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyObject* result = PyObject_CallFunction(callback, "LisL",
                                            offset, partition, topic, timestamp);
    if (result) {
        Py_DECREF(result);
    } else {
        PyErr_Print();
    }
    PyGILState_Release(gstate);
}

static PyObject* py_RecordMetadata_copy(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    PyObject* callback;
    if (!PyArg_ParseTuple(args, "KO", &ptr, &callback)) return NULL;
    if (!PyCallable_Check(callback)) {
        PyErr_SetString(PyExc_TypeError, "callback must be callable");
        return NULL;
    }
    kafka_producer_RecordMetadata_t *m = (kafka_producer_RecordMetadata_t*)(uintptr_t)ptr;
    Py_INCREF(callback);
    kafka_producer_RecordMetadata_copy(m, record_metadata_copy_callback, (void*)callback);
    Py_DECREF(callback);
    Py_RETURN_NONE;
}

// KafkaError accessor/destroy functions
static PyObject* py_KafkaError_code(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_KafkaError_t *e = (kafka_common_KafkaError_t*)(uintptr_t)ptr;
    return PyLong_FromLong(kafka_common_KafkaError_code(e));
}

static PyObject* py_KafkaError_message(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_KafkaError_t *e = (kafka_common_KafkaError_t*)(uintptr_t)ptr;
    const char *msg = kafka_common_KafkaError_message(e);
    if (msg == NULL) Py_RETURN_NONE;
    return PyUnicode_FromString(msg);
}

static PyObject* py_KafkaError_is_retriable(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_KafkaError_t *e = (kafka_common_KafkaError_t*)(uintptr_t)ptr;
    return PyBool_FromLong(kafka_common_KafkaError_is_retriable(e) ? 1 : 0);
}

static PyObject* py_KafkaError_is_fatal(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_KafkaError_t *e = (kafka_common_KafkaError_t*)(uintptr_t)ptr;
    return PyBool_FromLong(kafka_common_KafkaError_is_fatal(e) ? 1 : 0);
}

static PyObject* py_KafkaError_txn_requires_abort(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_KafkaError_t *e = (kafka_common_KafkaError_t*)(uintptr_t)ptr;
    return PyBool_FromLong(kafka_common_KafkaError_txn_requires_abort(e) ? 1 : 0);
}

static PyObject* py_KafkaError_destroy(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_common_KafkaError_t *e = (kafka_common_KafkaError_t*)(uintptr_t)ptr;
    kafka_common_KafkaError_destroy(e);
    Py_RETURN_NONE;
}

// ===========================================================================
// Consumer — marshaling-only bridge to the Rust consumer FFI.
//
// Unlike the producer side, the consumer C layer runs NO background threads and
// holds NO business logic: all orchestration (waiting, signal handling, future
// resolution) lives in pure Python (consumer.py). Async FFI callbacks fire on
// the Rust dispatcher thread; the trampolines below re-acquire the GIL and hand
// the raw result handles back to Python as ints (the Python callback then
// schedules resolution / drains the handle). Consumer handles are passed across
// the boundary as ints, exactly like producer handles.
// ===========================================================================

// ---- _BorrowedBytes: zero-copy buffer exporter over a ConsumerRecords batch --
//
// record.key / record.value return memoryview(_BorrowedBytes(batch, ptr, len)).
// The memoryview holds a reference to the _BorrowedBytes (via the buffer
// protocol's view->obj), and _BorrowedBytes holds a strong reference to the
// owning ConsumerRecords batch — so the Rust batch buffer the slice points into
// stays alive for as long as any memoryview over it lives. Zero copy, no
// dangling.
typedef struct {
    PyObject_HEAD
    PyObject* batch;        // strong ref to the owning ConsumerRecordsObject
    const uint8_t* ptr;     // borrowed slice into the batch buffer
    Py_ssize_t len;
} BorrowedBytesObject;

static int BorrowedBytes_getbuffer(PyObject* exporter, Py_buffer* view, int flags) {
    BorrowedBytesObject* self = (BorrowedBytesObject*)exporter;
    // PyBuffer_FillInfo sets view->obj = exporter and INCREFs it, keeping the
    // _BorrowedBytes (and through it the batch) alive while the view exists.
    return PyBuffer_FillInfo(view, exporter, (void*)self->ptr, self->len,
                             1 /* readonly */, flags);
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

// Build memoryview(_BorrowedBytes(batch, ptr, len)), or None when ptr is NULL
// (absent key/value). Borrows nothing into Python beyond the batch ref.
static PyObject* borrowed_memoryview(PyObject* batch, const uint8_t* ptr, Py_ssize_t len) {
    if (ptr == NULL) {
        Py_RETURN_NONE;
    }
    BorrowedBytesObject* bb = PyObject_New(BorrowedBytesObject, &BorrowedBytesType);
    if (bb == NULL) return NULL;
    Py_INCREF(batch);
    bb->batch = batch;
    bb->ptr = ptr;
    bb->len = len;
    PyObject* mv = PyMemoryView_FromObject((PyObject*)bb);
    Py_DECREF(bb);  // the memoryview holds its own ref via the buffer protocol
    return mv;
}

// ---- ConsumerRecords: owns the Rust batch handle ---------------------------
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

// ---- ConsumerRecord: borrows a record from the batch, holds parent ref -----
typedef struct {
    PyObject_HEAD
    PyObject* batch;                            // strong ref to ConsumerRecordsObject
    const kafka_consumer_ConsumerRecord_t* rec; // borrowed from the batch
} ConsumerRecordObject;

static void ConsumerRecord_dealloc(ConsumerRecordObject* self) {
    Py_XDECREF(self->batch);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* ConsumerRecord_get_topic(ConsumerRecordObject* self, void* closure) {
    int32_t len = 0;
    const char* topic = kafka_consumer_ConsumerRecord_topic(self->rec, &len);
    if (topic == NULL) Py_RETURN_NONE;
    return PyUnicode_FromStringAndSize(topic, len);
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

static PyObject* ConsumerRecord_get_timestamp_type(ConsumerRecordObject* self, void* closure) {
    return PyLong_FromLong(kafka_consumer_ConsumerRecord_timestamp_type(self->rec));
}

static PyObject* ConsumerRecord_get_key(ConsumerRecordObject* self, void* closure) {
    int32_t len = 0;
    const uint8_t* key = kafka_consumer_ConsumerRecord_key(self->rec, &len);
    return borrowed_memoryview(self->batch, key, key == NULL ? 0 : (Py_ssize_t)len);
}

static PyObject* ConsumerRecord_get_value(ConsumerRecordObject* self, void* closure) {
    int32_t len = 0;
    const uint8_t* value = kafka_consumer_ConsumerRecord_value(self->rec, &len);
    return borrowed_memoryview(self->batch, value, value == NULL ? 0 : (Py_ssize_t)len);
}

static PyObject* ConsumerRecord_get_serialized_key_size(ConsumerRecordObject* self, void* closure) {
    return PyLong_FromLong(kafka_consumer_ConsumerRecord_serialized_key_size(self->rec));
}

static PyObject* ConsumerRecord_get_serialized_value_size(ConsumerRecordObject* self, void* closure) {
    return PyLong_FromLong(kafka_consumer_ConsumerRecord_serialized_value_size(self->rec));
}

static PyObject* ConsumerRecord_get_leader_epoch(ConsumerRecordObject* self, void* closure) {
    int32_t epoch = 0;
    if (kafka_consumer_ConsumerRecord_leader_epoch(self->rec, &epoch)) {
        return PyLong_FromLong(epoch);
    }
    Py_RETURN_NONE;
}

// headers -> list[(key:str, value:memoryview)]
static PyObject* ConsumerRecord_get_headers(ConsumerRecordObject* self, void* closure) {
    int32_t n = kafka_consumer_ConsumerRecord_header_count(self->rec);
    PyObject* list = PyList_New(n < 0 ? 0 : n);
    if (list == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        int32_t klen = 0;
        const char* hkey = kafka_consumer_ConsumerRecord_header_key(self->rec, i, &klen);
        int32_t vlen = 0;
        const uint8_t* hval = kafka_consumer_ConsumerRecord_header_value(self->rec, i, &vlen);
        PyObject* pykey = PyUnicode_FromStringAndSize(hkey ? hkey : "", hkey ? klen : 0);
        PyObject* pyval = borrowed_memoryview(self->batch, hval, hval == NULL ? 0 : (Py_ssize_t)vlen);
        if (pykey == NULL || pyval == NULL) {
            Py_XDECREF(pykey);
            Py_XDECREF(pyval);
            Py_DECREF(list);
            return NULL;
        }
        PyObject* tuple = PyTuple_Pack(2, pykey, pyval);
        Py_DECREF(pykey);
        Py_DECREF(pyval);
        if (tuple == NULL) {
            Py_DECREF(list);
            return NULL;
        }
        PyList_SET_ITEM(list, i, tuple);  // steals ref
    }
    return list;
}

static PyGetSetDef ConsumerRecord_getsetters[] = {
    {"topic", (getter)ConsumerRecord_get_topic, NULL, "Topic name", NULL},
    {"partition", (getter)ConsumerRecord_get_partition, NULL, "Partition", NULL},
    {"offset", (getter)ConsumerRecord_get_offset, NULL, "Offset", NULL},
    {"timestamp", (getter)ConsumerRecord_get_timestamp, NULL, "Timestamp", NULL},
    {"timestamp_type", (getter)ConsumerRecord_get_timestamp_type, NULL, "Timestamp type", NULL},
    {"key", (getter)ConsumerRecord_get_key, NULL, "Key (memoryview or None)", NULL},
    {"value", (getter)ConsumerRecord_get_value, NULL, "Value (memoryview or None)", NULL},
    {"serialized_key_size", (getter)ConsumerRecord_get_serialized_key_size, NULL, "Serialized key size", NULL},
    {"serialized_value_size", (getter)ConsumerRecord_get_serialized_value_size, NULL, "Serialized value size", NULL},
    {"leader_epoch", (getter)ConsumerRecord_get_leader_epoch, NULL, "Leader epoch or None", NULL},
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
    .tp_getset = ConsumerRecord_getsetters,
};

// ConsumerRecords.get(i) -> ConsumerRecord | None
static PyObject* ConsumerRecords_get(ConsumerRecordsObject* self, PyObject* args) {
    int index;
    if (!PyArg_ParseTuple(args, "i", &index)) return NULL;
    const kafka_consumer_ConsumerRecord_t* rec =
        kafka_consumer_ConsumerRecords_get(self->records, index);
    if (rec == NULL) Py_RETURN_NONE;
    ConsumerRecordObject* obj = PyObject_New(ConsumerRecordObject, &ConsumerRecordType);
    if (obj == NULL) return NULL;
    Py_INCREF((PyObject*)self);
    obj->batch = (PyObject*)self;
    obj->rec = rec;
    return (PyObject*)obj;
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
    {"get", (PyCFunction)ConsumerRecords_get, METH_VARARGS, "Record at index, or None"},
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

// Wrap an owned records handle into a ConsumerRecordsObject (NULL -> None).
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

// ---- argument marshaling helpers -------------------------------------------
//
// The char* produced below point into the Python str objects held by the
// caller's argument list, which stays alive for the whole FFI call; the FFI
// copies them into owned Rust data synchronously before returning, so freeing
// the arrays right after the call is safe.

// list[str] -> char* array. Returns count (>=0), or -1 on error (exception set).
// On success the caller must PyMem_Free(*out).
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

// list[(topic:str, partition:int)] -> parallel arrays. Returns count or -1.
// On success caller must PyMem_Free(*out_t) and PyMem_Free(*out_p).
static Py_ssize_t tp_to_arrays(PyObject* list, const char*** out_t, int32_t** out_p) {
    Py_ssize_t n = PySequence_Size(list);
    if (n < 0) return -1;
    const char** topics = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    int32_t* parts = n > 0 ? PyMem_Malloc(n * sizeof(int32_t)) : NULL;
    if (n > 0 && (topics == NULL || parts == NULL)) { PyMem_Free(topics); PyMem_Free(parts); PyErr_NoMemory(); return -1; }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(list, i);  // new ref
        const char* t = NULL; int p = 0;
        int ok = item && PyArg_ParseTuple(item, "si", &t, &p);
        Py_XDECREF(item);
        if (!ok) { PyMem_Free(topics); PyMem_Free(parts); return -1; }
        topics[i] = t; parts[i] = p;
    }
    *out_t = topics; *out_p = parts;
    return n;
}

// list[(topic:str, partition:int, offset:int, leader_epoch:int|-1, metadata:str|None)]
// -> the five parallel arrays every commit-with-offsets FFI entry point takes
// (Consumer_commit_sync_offsets_async, Consumer_commit_async_offsets,
// ConsumerHandle_commit_{sync,async}_offsets). Returns count or -1 (exception
// set); on success the caller must release the arrays with offset_arrays_free.
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

// ---- Producer transaction control ops (async) ------------------------------
//
// Async twins of the five sync transaction ops above, driving the
// kafka_producer_Producer_<op>_async FFI variants. The Python wrapper waits on
// the completion via _run_sync (threading.Event, GIL released -> the main
// thread stays responsive to SIGTERM/KeyboardInterrupt) or _run_async
// (asyncio.Future, awaited/cancellable), exactly as flush/close already do, so a
// transaction op never parks the caller inside a native block_on: the Python
// transaction API is async-first, consistent with flush/close/partitions_for.
//
// All four no-arg ops reuse producer_op_trampoline (the shared void-op
// trampoline used by flush_async/close_async): it fires cb(error_int) exactly
// once on the dispatcher thread and Py_DECREFs the callback exactly once. The
// Py_INCREF(cb) before handing it to the FFI as user_data balances that single
// DECREF (the Rust *_async path always fires the callback once -- success, op
// error, null-producer, or concurrent-modification rejection). The *_async FFI
// calls only enqueue/spawn and return immediately, so -- like close_async /
// flush_async -- they are NOT wrapped in Py_BEGIN_ALLOW_THREADS.
//
// These are placed here (rather than beside the sync txn wrappers) so
// send_offsets_to_transaction_async can reuse offsets_to_arrays above instead of
// duplicating the marshaling loop; the sync send_offsets wrapper predates that
// helper and keeps its own inline copy.

static PyObject* py_Producer_init_transactions_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &producer_ptr, &cb)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    Py_INCREF(cb);
    kafka_producer_Producer_init_transactions_async(
        producer->producer, producer_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Producer_begin_transaction_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &producer_ptr, &cb)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    Py_INCREF(cb);
    kafka_producer_Producer_begin_transaction_async(
        producer->producer, producer_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Producer_commit_transaction_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &producer_ptr, &cb)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    Py_INCREF(cb);
    kafka_producer_Producer_commit_transaction_async(
        producer->producer, producer_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Producer_abort_transaction_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &producer_ptr, &cb)) return NULL;
    Producer* producer = (Producer*)producer_ptr;
    Py_INCREF(cb);
    kafka_producer_Producer_abort_transaction_async(
        producer->producer, producer_op_trampoline, cb);
    Py_RETURN_NONE;
}

// send_offsets_to_transaction_async(producer, offsets, group_metadata, cb):
// combines the sync send_offsets marshaling (via the shared offsets_to_arrays)
// with the async callback. The parallel arrays and the borrowed group-metadata
// handle are marshaled/cloned synchronously on the calling thread by the Rust
// *_async variant before it spawns (verified against
// send_offsets_to_transaction_async in src/ffi/producer.rs), so the C
// temporaries only need to survive this synchronous call and are freed right
// after it returns -- exactly as the sync wrapper does; no extra keep-alive is
// needed for the arrays or the group-metadata handle (gm_obj is a live argument
// and the GIL is held throughout).
static PyObject* py_Producer_send_offsets_to_transaction_async(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;
    PyObject* offsets;
    PyObject* gm_obj;
    PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOOO", &producer_ptr, &offsets, &gm_obj, &cb)) return NULL;

    // Typecheck the borrowed group-metadata handle before dereferencing it
    // (matches the sync wrapper).
    if (!PyObject_TypeCheck(gm_obj, &ConsumerGroupMetadataType)) {
        PyErr_SetString(PyExc_TypeError,
            "group_metadata must be a ConsumerGroupMetadata object");
        return NULL;
    }
    Producer* producer = (Producer*)producer_ptr;
    kafka_consumer_ConsumerGroupMetadata_t* gm =
        ((ConsumerGroupMetadataObject*)gm_obj)->handle;

    offset_arrays_t a;
    Py_ssize_t n = offsets_to_arrays(offsets, &a);
    if (n < 0) return NULL;  // exception set; offsets_to_arrays already freed a.

    // Only Py_INCREF once the marshaling has succeeded and the FFI call is
    // guaranteed to run (so the single DECREF in producer_op_trampoline is
    // always balanced). An empty offsets map is a legitimate count == 0: the
    // arrays are NULL and the FFI reads none of them.
    Py_INCREF(cb);
    kafka_producer_Producer_send_offsets_to_transaction_async(
        producer->producer, a.topics, a.parts, a.offs, a.epochs, a.metas,
        (int32_t)n, gm, producer_op_trampoline, cb);
    offset_arrays_free(&a);
    Py_RETURN_NONE;
}

// ---- trampolines (run on the Rust dispatcher thread) -----------------------

// op callback: (error, user_data) -> py_cb(error_int)
static void consumer_op_trampoline(kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "K", (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

// poll callback: (records, error, user_data) -> py_cb(records_int, error_int)
static void consumer_poll_trampoline(kafka_consumer_ConsumerRecords_t* records,
                                     kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "KK",
        (unsigned long long)(uintptr_t)records,
        (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

// position callback: (i64, error, user_data) -> py_cb(position, error_int)
static void consumer_position_trampoline(int64_t position,
                                         kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "LK",
        (long long)position, (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

// Shared body for all handle-returning value callbacks (committed,
// offsets_for_times, beginning/end offsets, partitions_for, list_topics):
// hand the opaque result handle + error back to Python as ints. Python then
// drains the handle via the matching *_drain function.
static void fire_handle_cb(void* handle, kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "KK",
        (unsigned long long)(uintptr_t)handle,
        (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

static void consumer_committed_trampoline(kafka_consumer_OffsetMap_t* m,
                                          kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(m, e, ud); }
static void consumer_oft_trampoline(kafka_consumer_OffsetAndTimestampMap_t* m,
                                    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(m, e, ud); }
static void consumer_long_offsets_trampoline(kafka_consumer_LongOffsetMap_t* m,
                                             kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(m, e, ud); }
static void consumer_partitions_for_trampoline(kafka_consumer_PartitionInfoList_t* l,
                                               kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(l, e, ud); }
static void consumer_list_topics_trampoline(kafka_consumer_TopicPartitionInfoMap_t* m,
                                            kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(m, e, ud); }

// Converts + destroys an owned TopicPartitionList (defined with the state reads).
static PyObject* topic_partition_list_to_py(kafka_consumer_TopicPartitionList_t* list);

// ---- rebalance-listener trampolines (multi-shot) ---------------------------
//
// `user_data` is the Python listener adapter (consumer.py's _ListenerAdapter).
// Unlike the one-shot op trampolines above, these fire once per rebalance for
// the whole life of the registration, so they must NOT DECREF: the single
// reference taken at subscribe time is released by
// listener_user_data_destroy_trampoline when the Rust adapter is dropped.
//
// The delivered kafka_consumer_TopicPartitionList_t is owned by the callee, and
// is converted here rather than handed to Python as a handle int:
// topic_partition_list_to_py destroys it on every path (success and failure
// alike), so no ownership hand-off — and therefore no possible leak — crosses
// into Python.
//
// Returns null on success. A Python exception is converted into a
// kafka_common_KafkaError_t carrying its message verbatim — ownership passes to
// Rust, which reads and frees it, so it must NOT be destroyed here. This
// mirrors a Java listener throwing out of onPartitionsRevoked/Assigned/Lost:
// the rebalance (and the poll that drove it) fails with that message.

// Error code used for a Python listener exception. UnknownServerError (-1) is
// the closest analogue to Java wrapping an arbitrary listener throwable, and is
// what the FFI also reports for a rejected reentrant call.
#define PY_LISTENER_ERROR_CODE (-1)

// Convert the currently set Python exception into an error handle, clearing the
// indicator (it must be clean before returning into Rust). The GIL must be held.
static kafka_common_KafkaError_t* error_from_py_exception(const char* fallback) {
    PyObject *type = NULL, *value = NULL, *tb = NULL;
    PyErr_Fetch(&type, &value, &tb);  // clears the indicator
    PyErr_NormalizeException(&type, &value, &tb);
    PyObject* text = value ? PyObject_Str(value) : NULL;
    const char* msg = text ? PyUnicode_AsUTF8(text) : NULL;
    kafka_common_KafkaError_t* err =
        kafka_common_KafkaError_new(PY_LISTENER_ERROR_CODE, msg ? msg : fallback);
    Py_XDECREF(text);
    Py_XDECREF(type); Py_XDECREF(value); Py_XDECREF(tb);
    PyErr_Clear();  // defensive: PyObject_Str / NormalizeException may re-set it
    return err;
}

static kafka_common_KafkaError_t* listener_invoke(const char* method,
                                                  kafka_consumer_TopicPartitionList_t* list,
                                                  void* user_data) {
    PyObject* adapter = (PyObject*)user_data;
    kafka_common_KafkaError_t* err = NULL;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* partitions = topic_partition_list_to_py(list);  // destroys `list`
    if (partitions == NULL) {
        err = error_from_py_exception("failed to convert the rebalance partitions");
    } else {
        PyObject* r = PyObject_CallMethod(adapter, method, "O", partitions);
        Py_DECREF(partitions);
        if (r) {
            Py_DECREF(r);
        } else {
            err = error_from_py_exception("rebalance listener raised an exception");
        }
    }
    PyGILState_Release(g);
    return err;
}

static kafka_common_KafkaError_t* listener_on_revoked_trampoline(
        kafka_consumer_TopicPartitionList_t* list, void* ud) {
    return listener_invoke("_on_revoked", list, ud);
}

static kafka_common_KafkaError_t* listener_on_assigned_trampoline(
        kafka_consumer_TopicPartitionList_t* list, void* ud) {
    return listener_invoke("_on_assigned", list, ud);
}

static kafka_common_KafkaError_t* listener_on_lost_trampoline(
        kafka_consumer_TopicPartitionList_t* list, void* ud) {
    return listener_invoke("_on_lost", list, ud);
}

// Release the listener-adapter reference taken at subscribe time. Fired from the
// Rust adapter's Drop, i.e. when a later subscribe* replaces the registration or
// when the consumer is destroyed — NOT on unsubscribe()/close(), which leave the
// listener registered (Java's SubscriptionState.unsubscribe() does the same).
// May run on any thread, hence PyGILState_Ensure.
static void listener_user_data_destroy_trampoline(void* user_data) {
    PyObject* adapter = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    Py_DECREF(adapter);
    PyGILState_Release(g);
}

// ---- commit-callback trampolines -------------------------------------------
//
// commit callback: (offsets, error, user_data) -> py_cb(offset_map_int, error_int)
// One-shot per commit call, but the reference is released through the destroy
// hook rather than here, so it is dropped exactly once even when the commit
// never completes (e.g. the consumer is destroyed first). Java's
// OffsetCommitCallback.onComplete returns void and has nowhere to report a
// failure, so an exception is only printed.
static void consumer_commit_callback_trampoline(kafka_consumer_OffsetMap_t* offsets,
                                                kafka_common_KafkaError_t* error,
                                                void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "KK",
        (unsigned long long)(uintptr_t)offsets,
        (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    PyGILState_Release(g);
}

static void commit_callback_user_data_destroy_trampoline(void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

// Used for commit_async(offsets) with no user callback: the FFI exposes the
// offsets variant only in its callback-taking form and its `callback` parameter
// is not nullable, so supply one that just releases the delivered handles.
// Equivalent to Java's commitAsync(offsets, null) — the commit happens, nothing
// is reported back. Touches no Python objects, so it needs no GIL.
static void consumer_commit_discard_trampoline(kafka_consumer_OffsetMap_t* offsets,
                                               kafka_common_KafkaError_t* error,
                                               void* user_data) {
    (void)user_data;
    if (offsets) kafka_consumer_OffsetMap_destroy(offsets);
    if (error) kafka_common_KafkaError_destroy(error);
}

// ---- constructors / lifecycle ----------------------------------------------
static PyObject* py_Consumer_MockConsumer_new(PyObject* self, PyObject* args) {
    const char* auto_offset_reset;
    if (!PyArg_ParseTuple(args, "s", &auto_offset_reset)) return NULL;
    kafka_consumer_Consumer_t* c = kafka_consumer_MockConsumer_new(auto_offset_reset);
    if (c == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create MockConsumer");
        return NULL;
    }
    return PyLong_FromVoidPtr(c);
}

static PyObject* py_Consumer_KafkaConsumer_new(PyObject* self, PyObject* args) {
    PyObject* config_dict;
    if (!PyArg_ParseTuple(args, "O", &config_dict)) return NULL;
    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }
    kafka_consumer_ConsumerProperties_t* props = kafka_consumer_ConsumerProperties_new();
    if (props == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ConsumerProperties");
        return NULL;
    }
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_AsUTF8(key);
        const char* v = PyUnicode_AsUTF8(value);
        if (k == NULL || v == NULL) {
            kafka_consumer_ConsumerProperties_destroy(props);
            PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
        kafka_consumer_ConsumerProperties_put(props, k, v);
    }
    kafka_common_KafkaError_t* err = NULL;
    kafka_consumer_Consumer_t* c = kafka_consumer_KafkaConsumer_new(props, &err);
    kafka_consumer_ConsumerProperties_destroy(props);
    if (c == NULL) {
        const char* msg = err ? kafka_common_KafkaError_message(err) : NULL;
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create KafkaConsumer");
        if (err) kafka_common_KafkaError_destroy(err);
        return NULL;
    }
    return PyLong_FromVoidPtr(c);
}

static PyObject* py_Consumer_destroy(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_Consumer_t* c = (kafka_consumer_Consumer_t*)(uintptr_t)h;
    Py_BEGIN_ALLOW_THREADS
    kafka_consumer_Consumer_destroy(c);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_wakeup(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_Consumer_wakeup((kafka_consumer_Consumer_t*)(uintptr_t)h);
    Py_RETURN_NONE;
}

// ---- async submit functions (each holds the GIL: the submit is non-blocking;
// the await runs on the tokio runtime, the callback fires later on the
// dispatcher thread) --------------------------------------------------------

static PyObject* py_Consumer_poll_async(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &timeout_ms, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_poll_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
                                       timeout_ms, consumer_poll_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_subscribe_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* topics; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &topics, &cb)) return NULL;
    const char** arr = NULL;
    Py_ssize_t n = topics_to_array(topics, &arr);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_subscribe_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
                                            arr, (int32_t)n, consumer_op_trampoline, cb);
    PyMem_Free(arr);
    Py_RETURN_NONE;
}

// subscribe_with_listener_async(consumer, list[str], listener_adapter, cb):
// Java's subscribe(Collection<String>, ConsumerRebalanceListener). All three
// rebalance trampolines are always passed; the Python adapter supplies Java's
// on_partitions_lost -> on_partitions_revoked default, so the FFI never needs
// its own null-lost delegation.
static PyObject* py_Consumer_subscribe_with_listener_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* topics; PyObject* listener; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOOO", &h, &topics, &listener, &cb)) return NULL;
    const char** arr = NULL;
    Py_ssize_t n = topics_to_array(topics, &arr);
    if (n < 0) return NULL;
    // The reference below is owned by the Rust adapter and released by
    // listener_user_data_destroy_trampoline when that adapter is dropped.
    Py_INCREF(listener);
    kafka_consumer_ConsumerRebalanceListener_t* l =
        kafka_consumer_ConsumerRebalanceListener_new(
            listener_on_revoked_trampoline,
            listener_on_assigned_trampoline,
            listener_on_lost_trampoline,
            listener,
            listener_user_data_destroy_trampoline);
    if (l == NULL) {
        Py_DECREF(listener);
        PyMem_Free(arr);
        PyErr_SetString(PyExc_RuntimeError,
                        "Failed to create ConsumerRebalanceListener");
        return NULL;
    }
    Py_INCREF(cb);
    // The listener handle is consumed unconditionally — error paths included —
    // so it must never be destroyed here.
    kafka_consumer_Consumer_subscribe_with_listener_async(
        (kafka_consumer_Consumer_t*)(uintptr_t)h, arr, (int32_t)n, l,
        consumer_op_trampoline, cb);
    PyMem_Free(arr);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_unsubscribe_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_unsubscribe_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
                                              consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

// assign / pause / resume / seek_to_beginning / seek_to_end all take a list of
// (topic, partition) and the op callback.
static PyObject* tp_op_async(PyObject* args,
                             void (*ffi)(const kafka_consumer_Consumer_t*, const char* const*,
                                         const int32_t*, int32_t,
                                         kafka_consumer_Consumer_op_callback_t, void*)) {
    unsigned long long h; PyObject* tps; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &tps, &cb)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    ffi((kafka_consumer_Consumer_t*)(uintptr_t)h, topics, parts, (int32_t)n,
        consumer_op_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(parts);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_assign_async(PyObject* self, PyObject* args) {
    return tp_op_async(args, kafka_consumer_Consumer_assign_async);
}
static PyObject* py_Consumer_pause_async(PyObject* self, PyObject* args) {
    return tp_op_async(args, kafka_consumer_Consumer_pause_async);
}
static PyObject* py_Consumer_resume_async(PyObject* self, PyObject* args) {
    return tp_op_async(args, kafka_consumer_Consumer_resume_async);
}
static PyObject* py_Consumer_seek_to_beginning_async(PyObject* self, PyObject* args) {
    return tp_op_async(args, kafka_consumer_Consumer_seek_to_beginning_async);
}
static PyObject* py_Consumer_seek_to_end_async(PyObject* self, PyObject* args) {
    return tp_op_async(args, kafka_consumer_Consumer_seek_to_end_async);
}

static PyObject* py_Consumer_commit_sync_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_commit_sync_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
                                              consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

// commit_sync_offsets_async: list[(topic, partition, offset, leader_epoch|-1, metadata|None)]
static PyObject* py_Consumer_commit_sync_offsets_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* offsets; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &offsets, &cb)) return NULL;
    offset_arrays_t a;
    Py_ssize_t n = offsets_to_arrays(offsets, &a);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_commit_sync_offsets_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        a.topics, a.parts, a.offs, a.epochs, a.metas, (int32_t)n, consumer_op_trampoline, cb);
    offset_arrays_free(&a);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_close_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_close_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
                                        consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_position_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsiO", &h, &topic, &partition, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_position_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topic, partition, consumer_position_trampoline, cb);
    Py_RETURN_NONE;
}

// committed / beginning_offsets / end_offsets: list[(topic, partition)] +
// cb(handle, error). Written explicitly (rather than through one generic
// helper) so each call uses its exact typed callback — no function-pointer
// casts.
static PyObject* py_Consumer_committed_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &tps, &cb)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_committed_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topics, parts, (int32_t)n, consumer_committed_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(parts);
    Py_RETURN_NONE;
}
static PyObject* py_Consumer_beginning_offsets_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &tps, &cb)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_beginning_offsets_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topics, parts, (int32_t)n, consumer_long_offsets_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(parts);
    Py_RETURN_NONE;
}
static PyObject* py_Consumer_end_offsets_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &tps, &cb)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_end_offsets_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topics, parts, (int32_t)n, consumer_long_offsets_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(parts);
    Py_RETURN_NONE;
}

// offsets_for_times: list[(topic, partition, timestamp)] + cb(handle, error)
static PyObject* py_Consumer_offsets_for_times_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &spec, &cb)) return NULL;
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    const char** topics = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    int32_t* parts = n > 0 ? PyMem_Malloc(n * sizeof(int32_t)) : NULL;
    int64_t* tss = n > 0 ? PyMem_Malloc(n * sizeof(int64_t)) : NULL;
    if (n > 0 && (!topics || !parts || !tss)) {
        PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(tss);
        return PyErr_NoMemory();
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);
        const char* t = NULL; int p = 0; long long ts = 0;
        int ok = item && PyArg_ParseTuple(item, "siL", &t, &p, &ts);
        if (ok) { topics[i] = t; parts[i] = p; tss[i] = ts; }
        Py_XDECREF(item);
        if (!ok) { PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(tss); return NULL; }
    }
    Py_INCREF(cb);
    kafka_consumer_Consumer_offsets_for_times_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topics, parts, tss, (int32_t)n, consumer_oft_trampoline, cb);
    PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(tss);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_partitions_for_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsO", &h, &topic, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_partitions_for_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topic, consumer_partitions_for_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_list_topics_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_list_topics_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        consumer_list_topics_trampoline, cb);
    Py_RETURN_NONE;
}

// seek / seek_with_metadata: single partition + the op callback.
//
// These use the async entry points like every other op that blocks in Rust. The
// sync kafka_consumer_Consumer_seek[_with_metadata] must NOT be called from here:
// AsyncKafkaConsumer::seek submits a SeekUnvalidatedEvent and drains background
// events, so it can invoke the rebalance listener, whose trampoline needs the GIL
// on the dispatcher thread — a sync call would hold the GIL inside block_on and
// deadlock the interpreter (and, on the asyncio consumer, occupy the very loop a
// coroutine listener has to run on).
static PyObject* py_Consumer_seek_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsiLO", &h, &topic, &partition, &offset, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_seek_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topic, partition, offset, consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_Consumer_seek_with_metadata_async(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset;
    int leader_epoch; const char* metadata; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KsiLisO", &h, &topic, &partition, &offset,
                          &leader_epoch, &metadata, &cb))
        return NULL;
    Py_INCREF(cb);
    kafka_consumer_Consumer_seek_with_metadata_async((kafka_consumer_Consumer_t*)(uintptr_t)h,
        topic, partition, offset, leader_epoch, metadata, consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

// ---- sync local ops (return error handle int, 0 on success) ----------------
static PyObject* py_Consumer_enforce_rebalance(PyObject* self, PyObject* args) {
    unsigned long long h; const char* reason;  // None -> ""
    if (!PyArg_ParseTuple(args, "Kz", &h, &reason)) return NULL;
    kafka_common_KafkaError_t* e = kafka_consumer_Consumer_enforce_rebalance(
        (kafka_consumer_Consumer_t*)(uintptr_t)h, reason);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// commit_async(consumer[, callback]) — Java's commitAsync() / commitAsync(cb).
// Without a callback this stays the plain fire-and-forget FFI call; with one it
// routes to the callback-taking variant, which owns the reference until it fires
// the destroy hook.
//
// The GIL must be released around the call: these are synchronous FFI entry
// points (they block in block_on) and the commit callback fires on the
// dispatcher thread, which needs the GIL for its trampoline. On a MockConsumer
// the callback is even awaited inline inside this very call, so holding the GIL
// here would deadlock.
static PyObject* py_Consumer_commit_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb = Py_None;
    if (!PyArg_ParseTuple(args, "K|O", &h, &cb)) return NULL;
    const kafka_consumer_Consumer_t* c = (const kafka_consumer_Consumer_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    if (cb == Py_None) {
        Py_BEGIN_ALLOW_THREADS
        e = kafka_consumer_Consumer_commit_async(c);
        Py_END_ALLOW_THREADS
    } else {
        Py_INCREF(cb);
        Py_BEGIN_ALLOW_THREADS
        e = kafka_consumer_Consumer_commit_async_with_callback(
            c, consumer_commit_callback_trampoline, cb,
            commit_callback_user_data_destroy_trampoline);
        Py_END_ALLOW_THREADS
    }
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// commit_async_offsets(consumer, list[(topic, partition, offset, epoch|-1,
// metadata|None)][, callback]) — Java's commitAsync(Map) / commitAsync(Map, cb).
static PyObject* py_Consumer_commit_async_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* offsets; PyObject* cb = Py_None;
    if (!PyArg_ParseTuple(args, "KO|O", &h, &offsets, &cb)) return NULL;
    offset_arrays_t a;
    Py_ssize_t n = offsets_to_arrays(offsets, &a);
    if (n < 0) return NULL;
    const kafka_consumer_Consumer_t* c = (const kafka_consumer_Consumer_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    if (cb == Py_None) {
        Py_BEGIN_ALLOW_THREADS
        e = kafka_consumer_Consumer_commit_async_offsets_with_callback(
            c, a.topics, a.parts, a.offs, a.epochs, a.metas, (int32_t)n,
            consumer_commit_discard_trampoline, NULL, NULL);
        Py_END_ALLOW_THREADS
    } else {
        Py_INCREF(cb);
        Py_BEGIN_ALLOW_THREADS
        e = kafka_consumer_Consumer_commit_async_offsets_with_callback(
            c, a.topics, a.parts, a.offs, a.epochs, a.metas, (int32_t)n,
            consumer_commit_callback_trampoline, cb,
            commit_callback_user_data_destroy_trampoline);
        Py_END_ALLOW_THREADS
    }
    offset_arrays_free(&a);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// ---- sync state reads (return Python objects directly) ---------------------
static PyObject* topic_partition_list_to_py(kafka_consumer_TopicPartitionList_t* list) {
    if (list == NULL) Py_RETURN_NONE;  // guard rejected (concurrent access)
    int32_t n = kafka_consumer_TopicPartitionList_count(list);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_consumer_TopicPartitionList_destroy(list); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_consumer_TopicPartition_t* tp = kafka_consumer_TopicPartitionList_get(list, i);
        const char* topic = kafka_consumer_TopicPartition_topic(tp);
        int32_t part = kafka_consumer_TopicPartition_partition(tp);
        PyObject* t = Py_BuildValue("(si)", topic, part);
        if (t == NULL) { Py_DECREF(out); kafka_consumer_TopicPartitionList_destroy(list); return NULL; }
        PyList_SET_ITEM(out, i, t);
    }
    kafka_consumer_TopicPartitionList_destroy(list);
    return out;
}

static PyObject* py_Consumer_assignment(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return topic_partition_list_to_py(
        kafka_consumer_Consumer_assignment((kafka_consumer_Consumer_t*)(uintptr_t)h));
}

static PyObject* py_Consumer_paused(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return topic_partition_list_to_py(
        kafka_consumer_Consumer_paused((kafka_consumer_Consumer_t*)(uintptr_t)h));
}

// Consumer_metrics -> list[dict] with keys name/group/description/tags/value,
// or None if the single-owner guard rejected the call.
//
// A list of dicts (rather than a dict keyed by the metric name) keeps the
// MetricName identity intact: two metrics share a name and group and differ only
// by tags, so no single scalar key is unique.
//
// `value` is float / str / int depending on the kind reported by
// kafka_consumer_MetricMap_get_value_kind (0=double, 1=string, 2=long, 3=int).
static PyObject* py_Consumer_metrics(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_MetricMap_t* map =
        kafka_consumer_Consumer_metrics((kafka_consumer_Consumer_t*)(uintptr_t)h);
    if (map == NULL) Py_RETURN_NONE;  // guard rejected (concurrent access)
    int32_t n = kafka_consumer_MetricMap_count(map);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_consumer_MetricMap_destroy(map); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* tags = PyDict_New();
        if (tags == NULL) goto fail;
        int32_t tn = kafka_consumer_MetricMap_get_tag_count(map, i);
        for (int32_t t = 0; t < tn; t++) {
            const char* k = kafka_consumer_MetricMap_get_tag_key(map, i, t);
            const char* v = kafka_consumer_MetricMap_get_tag_value(map, i, t);
            PyObject* pv = PyUnicode_FromString(v ? v : "");
            if (pv == NULL) { Py_DECREF(tags); goto fail; }
            if (PyDict_SetItemString(tags, k ? k : "", pv) != 0) {
                Py_DECREF(pv); Py_DECREF(tags); goto fail;
            }
            Py_DECREF(pv);
        }
        PyObject* value = NULL;
        int32_t kind = kafka_consumer_MetricMap_get_value_kind(map, i);
        switch (kind) {
            case 1: {
                const char* s = kafka_consumer_MetricMap_get_value_string(map, i);
                value = PyUnicode_FromString(s ? s : "");
                break;
            }
            case 2:
                value = PyLong_FromLongLong(
                    (long long)kafka_consumer_MetricMap_get_value_long(map, i));
                break;
            case 3:
                value = PyLong_FromLong((long)kafka_consumer_MetricMap_get_value_int(map, i));
                break;
            default:
                value = PyFloat_FromDouble(kafka_consumer_MetricMap_get_value_double(map, i));
                break;
        }
        if (value == NULL) { Py_DECREF(tags); goto fail; }
        const char* name = kafka_consumer_MetricMap_get_name(map, i);
        const char* group = kafka_consumer_MetricMap_get_group(map, i);
        const char* desc = kafka_consumer_MetricMap_get_description(map, i);
        // "N" steals the reference to tags/value, so they are not leaked here.
        // `kind` is carried through so the caller can distinguish Long from Int,
        // which both surface as Python `int` and would otherwise collapse.
        PyObject* entry = Py_BuildValue("{s:s,s:s,s:s,s:N,s:N,s:i}",
            "name", name ? name : "",
            "group", group ? group : "",
            "description", desc ? desc : "",
            "tags", tags,
            "value", value,
            "kind", (int)kind);
        if (entry == NULL) goto fail;
        PyList_SET_ITEM(out, i, entry);
    }
    kafka_consumer_MetricMap_destroy(map);
    return out;
fail:
    Py_DECREF(out);
    kafka_consumer_MetricMap_destroy(map);
    return NULL;
}

static PyObject* string_list_to_py(kafka_consumer_StringList_t* list) {
    if (list == NULL) Py_RETURN_NONE;  // guard rejected (concurrent access)
    int32_t n = kafka_consumer_StringList_count(list);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_consumer_StringList_destroy(list); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const char* s = kafka_consumer_StringList_get(list, i);
        PyObject* ps = PyUnicode_FromString(s ? s : "");
        if (ps == NULL) { Py_DECREF(out); kafka_consumer_StringList_destroy(list); return NULL; }
        PyList_SET_ITEM(out, i, ps);
    }
    kafka_consumer_StringList_destroy(list);
    return out;
}

static PyObject* py_Consumer_subscription(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return string_list_to_py(
        kafka_consumer_Consumer_subscription((kafka_consumer_Consumer_t*)(uintptr_t)h));
}

// Returns a ConsumerGroupMetadata object that RETAINS the live Rust handle
// (freed later in its tp_dealloc), or None on a concurrent-access rejection.
// The retained handle is what send_offsets_to_transaction needs; the FFI clones
// internally, so each call yields a fresh owned handle (no aliasing) — see §6.1.
static PyObject* py_Consumer_group_metadata(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerGroupMetadata_t* m =
        kafka_consumer_Consumer_group_metadata((kafka_consumer_Consumer_t*)(uintptr_t)h);
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
    char* s = kafka_consumer_Consumer_client_id((kafka_consumer_Consumer_t*)(uintptr_t)h);
    if (s == NULL) Py_RETURN_NONE;
    PyObject* out = PyUnicode_FromString(s);
    kafka_consumer_string_destroy(s);
    return out;
}

static PyObject* py_Consumer_current_lag(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition;
    if (!PyArg_ParseTuple(args, "Ksi", &h, &topic, &partition)) return NULL;
    int64_t lag = 0;
    if (kafka_consumer_Consumer_current_lag((kafka_consumer_Consumer_t*)(uintptr_t)h, topic, partition, &lag)) {
        return PyLong_FromLongLong(lag);
    }
    Py_RETURN_NONE;
}

// ---- map / list drain helpers (called by consumer.py after a value cb) -----
static PyObject* node_to_py(const kafka_common_Node_t* node) {
    if (node == NULL) Py_RETURN_NONE;
    int32_t id = kafka_common_Node_id(node);
    int32_t host_len = 0;
    const char* host = kafka_common_Node_host(node, &host_len);
    int32_t port = kafka_common_Node_port(node);
    int32_t rack_len = 0;
    const char* rack = kafka_common_Node_rack(node, &rack_len);
    PyObject* py_host = PyUnicode_FromStringAndSize(host ? host : "", host ? host_len : 0);
    PyObject* py_rack = rack ? PyUnicode_FromStringAndSize(rack, rack_len) : (Py_INCREF(Py_None), Py_None);
    if (py_host == NULL || py_rack == NULL) { Py_XDECREF(py_host); Py_XDECREF(py_rack); return NULL; }
    PyObject* out = Py_BuildValue("(iOiO)", id, py_host, port, py_rack);
    Py_DECREF(py_host); Py_DECREF(py_rack);
    return out;
}

static PyObject* partition_info_to_py(const kafka_consumer_PartitionInfo_t* info) {
    int32_t r = 0;
    const char* topic = kafka_consumer_PartitionInfo_topic(info);  // NUL-terminated
    int32_t partition = kafka_consumer_PartitionInfo_partition(info);
    PyObject* leader = node_to_py(kafka_consumer_PartitionInfo_leader(info));
    if (leader == NULL) return NULL;
    int32_t nrep = kafka_consumer_PartitionInfo_replica_count(info);
    int32_t nisr = kafka_consumer_PartitionInfo_in_sync_replica_count(info);
    int32_t noff = kafka_consumer_PartitionInfo_offline_replica_count(info);
    PyObject* replicas = PyList_New(nrep < 0 ? 0 : nrep);
    PyObject* isr = PyList_New(nisr < 0 ? 0 : nisr);
    PyObject* offline = PyList_New(noff < 0 ? 0 : noff);
    if (!replicas || !isr || !offline) { Py_XDECREF(replicas); Py_XDECREF(isr); Py_XDECREF(offline); Py_DECREF(leader); return NULL; }
    for (r = 0; r < nrep; r++) {
        PyObject* n = node_to_py(kafka_consumer_PartitionInfo_replica(info, r));
        if (!n) goto fail;
        PyList_SET_ITEM(replicas, r, n);
    }
    for (r = 0; r < nisr; r++) {
        PyObject* n = node_to_py(kafka_consumer_PartitionInfo_in_sync_replica(info, r));
        if (!n) goto fail;
        PyList_SET_ITEM(isr, r, n);
    }
    for (r = 0; r < noff; r++) {
        PyObject* n = node_to_py(kafka_consumer_PartitionInfo_offline_replica(info, r));
        if (!n) goto fail;
        PyList_SET_ITEM(offline, r, n);
    }
    PyObject* py_topic = PyUnicode_FromString(topic ? topic : "");
    if (!py_topic) goto fail;
    PyObject* out = Py_BuildValue("(NiNNNN)", py_topic, partition, leader, replicas, isr, offline);
    return out;  // Py_BuildValue "N" steals refs to py_topic/leader/replicas/isr/offline
fail:
    Py_DECREF(leader); Py_DECREF(replicas); Py_DECREF(isr); Py_DECREF(offline);
    return NULL;
}

static PyObject* py_OffsetMap_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_consumer_OffsetMap_t* m = (kafka_consumer_OffsetMap_t*)(uintptr_t)ptr;
    int32_t n = kafka_consumer_OffsetMap_count(m);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_consumer_OffsetMap_destroy(m); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_consumer_TopicPartition_t* k = kafka_consumer_OffsetMap_get_key(m, i);
        const kafka_consumer_OffsetAndMetadata_t* v = kafka_consumer_OffsetMap_get_value(m, i);
        int32_t epoch = 0;
        int has_epoch = kafka_consumer_OffsetAndMetadata_leader_epoch(v, &epoch);
        PyObject* key = Py_BuildValue("(si)", kafka_consumer_TopicPartition_topic(k),
                                      kafka_consumer_TopicPartition_partition(k));
        PyObject* val = Py_BuildValue("(LsO)", kafka_consumer_OffsetAndMetadata_offset(v),
                                      kafka_consumer_OffsetAndMetadata_metadata(v),
                                      has_epoch ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None));
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_consumer_OffsetMap_destroy(m); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_consumer_OffsetMap_destroy(m);
    return d;
}

static PyObject* py_OffsetAndTimestampMap_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_consumer_OffsetAndTimestampMap_t* m = (kafka_consumer_OffsetAndTimestampMap_t*)(uintptr_t)ptr;
    int32_t n = kafka_consumer_OffsetAndTimestampMap_count(m);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_consumer_OffsetAndTimestampMap_destroy(m); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_consumer_TopicPartition_t* k = kafka_consumer_OffsetAndTimestampMap_get_key(m, i);
        const kafka_consumer_OffsetAndTimestamp_t* v = kafka_consumer_OffsetAndTimestampMap_get_value(m, i);
        int32_t epoch = 0;
        int has_epoch = kafka_consumer_OffsetAndTimestamp_leader_epoch(v, &epoch);
        PyObject* key = Py_BuildValue("(si)", kafka_consumer_TopicPartition_topic(k),
                                      kafka_consumer_TopicPartition_partition(k));
        PyObject* val = Py_BuildValue("(LLO)", kafka_consumer_OffsetAndTimestamp_offset(v),
                                      kafka_consumer_OffsetAndTimestamp_timestamp(v),
                                      has_epoch ? PyLong_FromLong(epoch) : (Py_INCREF(Py_None), Py_None));
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_consumer_OffsetAndTimestampMap_destroy(m); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_consumer_OffsetAndTimestampMap_destroy(m);
    return d;
}

static PyObject* py_LongOffsetMap_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_consumer_LongOffsetMap_t* m = (kafka_consumer_LongOffsetMap_t*)(uintptr_t)ptr;
    int32_t n = kafka_consumer_LongOffsetMap_count(m);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_consumer_LongOffsetMap_destroy(m); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_consumer_TopicPartition_t* k = kafka_consumer_LongOffsetMap_get_key(m, i);
        PyObject* key = Py_BuildValue("(si)", kafka_consumer_TopicPartition_topic(k),
                                      kafka_consumer_TopicPartition_partition(k));
        PyObject* val = PyLong_FromLongLong(kafka_consumer_LongOffsetMap_get_value(m, i));
        if (!key || !val || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_consumer_LongOffsetMap_destroy(m); return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_consumer_LongOffsetMap_destroy(m);
    return d;
}

static PyObject* py_PartitionInfoList_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_consumer_PartitionInfoList_t* list = (kafka_consumer_PartitionInfoList_t*)(uintptr_t)ptr;
    int32_t n = kafka_consumer_PartitionInfoList_count(list);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_consumer_PartitionInfoList_destroy(list); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* pi = partition_info_to_py(kafka_consumer_PartitionInfoList_get(list, i));
        if (pi == NULL) { Py_DECREF(out); kafka_consumer_PartitionInfoList_destroy(list); return NULL; }
        PyList_SET_ITEM(out, i, pi);
    }
    kafka_consumer_PartitionInfoList_destroy(list);
    return out;
}

static PyObject* py_TopicPartitionInfoMap_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_consumer_TopicPartitionInfoMap_t* m = (kafka_consumer_TopicPartitionInfoMap_t*)(uintptr_t)ptr;
    int32_t n = kafka_consumer_TopicPartitionInfoMap_count(m);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_consumer_TopicPartitionInfoMap_destroy(m); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const char* topic = kafka_consumer_TopicPartitionInfoMap_get_topic(m, i);
        const kafka_consumer_PartitionInfoList_t* infos =
            kafka_consumer_TopicPartitionInfoMap_get_partitions(m, i);
        int32_t pn = kafka_consumer_PartitionInfoList_count(infos);
        PyObject* plist = PyList_New(pn < 0 ? 0 : pn);
        if (plist == NULL) { Py_DECREF(d); kafka_consumer_TopicPartitionInfoMap_destroy(m); return NULL; }
        for (int32_t j = 0; j < pn; j++) {
            PyObject* pi = partition_info_to_py(kafka_consumer_PartitionInfoList_get(infos, j));
            if (pi == NULL) { Py_DECREF(plist); Py_DECREF(d); kafka_consumer_TopicPartitionInfoMap_destroy(m); return NULL; }
            PyList_SET_ITEM(plist, j, pi);
        }
        PyObject* key = PyUnicode_FromString(topic ? topic : "");
        if (!key || PyDict_SetItem(d, key, plist) < 0) {
            Py_XDECREF(key); Py_DECREF(plist); Py_DECREF(d);
            kafka_consumer_TopicPartitionInfoMap_destroy(m); return NULL;
        }
        Py_DECREF(key); Py_DECREF(plist);
    }
    kafka_consumer_TopicPartitionInfoMap_destroy(m);
    return d;
}

// records handle int -> ConsumerRecords object (used by poll callback path)
static PyObject* py_ConsumerRecords_wrap(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    return wrap_records((kafka_consumer_ConsumerRecords_t*)(uintptr_t)ptr);
}

// ---- ConsumerHandle (reentrancy handle) ------------------------------------
//
// kafka_consumer_ConsumerHandle_t bypasses the consumer's single-owner access
// guard, so these are the operations a rebalance listener / commit callback may
// call while the consumer op that triggered it is still in flight (the plain
// kafka_consumer_Consumer_* ops would be rejected with ConcurrentModification).
//
// Every op except wakeup and the three state getters blocks in a native
// block_on, so each releases the GIL for the duration — otherwise a listener
// running on the dispatcher thread could not make progress, and the callback
// trampolines (which need the GIL) would deadlock. The state getters only take
// a short lock and return immediately, matching the Consumer_* getters above.

static PyObject* py_Consumer_handle(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerHandle_t* handle =
        kafka_consumer_Consumer_handle((kafka_consumer_Consumer_t*)(uintptr_t)h);
    if (handle == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ConsumerHandle");
        return NULL;
    }
    return PyLong_FromVoidPtr(handle);
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
    kafka_consumer_ConsumerHandle_wakeup((kafka_consumer_ConsumerHandle_t*)(uintptr_t)h);
    Py_RETURN_NONE;
}

static PyObject* py_ConsumerHandle_assignment(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return topic_partition_list_to_py(kafka_consumer_ConsumerHandle_assignment(
        (kafka_consumer_ConsumerHandle_t*)(uintptr_t)h));
}

static PyObject* py_ConsumerHandle_subscription(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return string_list_to_py(kafka_consumer_ConsumerHandle_subscription(
        (kafka_consumer_ConsumerHandle_t*)(uintptr_t)h));
}

static PyObject* py_ConsumerHandle_paused(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    return topic_partition_list_to_py(kafka_consumer_ConsumerHandle_paused(
        (kafka_consumer_ConsumerHandle_t*)(uintptr_t)h));
}

// assign / seek_to_beginning / seek_to_end / pause / resume: (handle,
// list[(topic, partition)]) -> error_int.
static PyObject* handle_tp_op(PyObject* args,
        kafka_common_KafkaError_t* (*ffi)(const kafka_consumer_ConsumerHandle_t*,
                                         const char* const*, const int32_t*, int32_t)) {
    unsigned long long h; PyObject* tps;
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = ffi(handle, topics, parts, (int32_t)n);
    Py_END_ALLOW_THREADS
    PyMem_Free(topics); PyMem_Free(parts);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_assign(PyObject* self, PyObject* args) {
    return handle_tp_op(args, kafka_consumer_ConsumerHandle_assign);
}
static PyObject* py_ConsumerHandle_seek_to_beginning(PyObject* self, PyObject* args) {
    return handle_tp_op(args, kafka_consumer_ConsumerHandle_seek_to_beginning);
}
static PyObject* py_ConsumerHandle_seek_to_end(PyObject* self, PyObject* args) {
    return handle_tp_op(args, kafka_consumer_ConsumerHandle_seek_to_end);
}
static PyObject* py_ConsumerHandle_pause(PyObject* self, PyObject* args) {
    return handle_tp_op(args, kafka_consumer_ConsumerHandle_pause);
}
static PyObject* py_ConsumerHandle_resume(PyObject* self, PyObject* args) {
    return handle_tp_op(args, kafka_consumer_ConsumerHandle_resume);
}

static PyObject* py_ConsumerHandle_seek(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset;
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &offset)) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_seek(handle, topic, partition, offset);
    Py_END_ALLOW_THREADS
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_seek_with_metadata(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset;
    int leader_epoch; const char* metadata;
    if (!PyArg_ParseTuple(args, "KsiLis", &h, &topic, &partition, &offset,
                          &leader_epoch, &metadata)) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_seek_with_metadata(handle, topic, partition, offset,
                                                        leader_epoch, metadata);
    Py_END_ALLOW_THREADS
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// position / position_timeout -> (position, error_int)
static PyObject* py_ConsumerHandle_position(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition;
    if (!PyArg_ParseTuple(args, "Ksi", &h, &topic, &partition)) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    int64_t pos = 0;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_position(handle, topic, partition, &pos);
    Py_END_ALLOW_THREADS
    return Py_BuildValue("(LK)", (long long)pos, (unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_position_timeout(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long timeout_ms;
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &timeout_ms)) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    int64_t pos = 0;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_position_timeout(handle, topic, partition,
                                                      timeout_ms, &pos);
    Py_END_ALLOW_THREADS
    return Py_BuildValue("(LK)", (long long)pos, (unsigned long long)(uintptr_t)e);
}

// committed / beginning_offsets / end_offsets -> (map_handle_int, error_int).
// Written out per method (rather than through one helper) so each call uses its
// exact typed out-parameter — no function-pointer casts, matching the
// Consumer_*_async wrappers above.
static PyObject* py_ConsumerHandle_committed(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps;
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_consumer_OffsetMap_t* map = NULL;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_committed(handle, topics, parts, (int32_t)n, &map);
    Py_END_ALLOW_THREADS
    PyMem_Free(topics); PyMem_Free(parts);
    return Py_BuildValue("(KK)", (unsigned long long)(uintptr_t)map,
                         (unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_beginning_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps;
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_consumer_LongOffsetMap_t* map = NULL;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_beginning_offsets(handle, topics, parts, (int32_t)n, &map);
    Py_END_ALLOW_THREADS
    PyMem_Free(topics); PyMem_Free(parts);
    return Py_BuildValue("(KK)", (unsigned long long)(uintptr_t)map,
                         (unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_end_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps;
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_consumer_LongOffsetMap_t* map = NULL;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_end_offsets(handle, topics, parts, (int32_t)n, &map);
    Py_END_ALLOW_THREADS
    PyMem_Free(topics); PyMem_Free(parts);
    return Py_BuildValue("(KK)", (unsigned long long)(uintptr_t)map,
                         (unsigned long long)(uintptr_t)e);
}

// offsets_for_times: list[(topic, partition, timestamp)] -> (map_int, error_int)
static PyObject* py_ConsumerHandle_offsets_for_times(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* spec;
    if (!PyArg_ParseTuple(args, "KO", &h, &spec)) return NULL;
    Py_ssize_t n = PySequence_Size(spec);
    if (n < 0) return NULL;
    const char** topics = n > 0 ? PyMem_Malloc(n * sizeof(char*)) : NULL;
    int32_t* parts = n > 0 ? PyMem_Malloc(n * sizeof(int32_t)) : NULL;
    int64_t* tss = n > 0 ? PyMem_Malloc(n * sizeof(int64_t)) : NULL;
    if (n > 0 && (!topics || !parts || !tss)) {
        PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(tss);
        return PyErr_NoMemory();
    }
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* item = PySequence_GetItem(spec, i);
        const char* t = NULL; int p = 0; long long ts = 0;
        int ok = item && PyArg_ParseTuple(item, "siL", &t, &p, &ts);
        if (ok) { topics[i] = t; parts[i] = p; tss[i] = ts; }
        Py_XDECREF(item);
        if (!ok) { PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(tss); return NULL; }
    }
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_consumer_OffsetAndTimestampMap_t* map = NULL;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_offsets_for_times(handle, topics, parts, tss,
                                                       (int32_t)n, &map);
    Py_END_ALLOW_THREADS
    PyMem_Free(topics); PyMem_Free(parts); PyMem_Free(tss);
    return Py_BuildValue("(KK)", (unsigned long long)(uintptr_t)map,
                         (unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_commit_sync(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_commit_sync(handle);
    Py_END_ALLOW_THREADS
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_commit_async(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_commit_async(handle);
    Py_END_ALLOW_THREADS
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// commit_sync_offsets / commit_async_offsets: the 5-array offsets shape.
// The handle exposes no callback-taking commit (matching the core handle), so a
// listener that needs a completion notification uses the consumer's
// commit_async(callback=...) before the rebalance.
static PyObject* py_ConsumerHandle_commit_sync_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* offsets;
    if (!PyArg_ParseTuple(args, "KO", &h, &offsets)) return NULL;
    offset_arrays_t a;
    Py_ssize_t n = offsets_to_arrays(offsets, &a);
    if (n < 0) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_commit_sync_offsets(handle, a.topics, a.parts,
                                                         a.offs, a.epochs, a.metas,
                                                         (int32_t)n);
    Py_END_ALLOW_THREADS
    offset_arrays_free(&a);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_ConsumerHandle_commit_async_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* offsets;
    if (!PyArg_ParseTuple(args, "KO", &h, &offsets)) return NULL;
    offset_arrays_t a;
    Py_ssize_t n = offsets_to_arrays(offsets, &a);
    if (n < 0) return NULL;
    const kafka_consumer_ConsumerHandle_t* handle =
        (const kafka_consumer_ConsumerHandle_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_ConsumerHandle_commit_async_offsets(handle, a.topics, a.parts,
                                                          a.offs, a.epochs, a.metas,
                                                          (int32_t)n);
    Py_END_ALLOW_THREADS
    offset_arrays_free(&a);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// ---- mock drivers ----------------------------------------------------------

// Drive a rebalance to the given assignment, invoking the registered rebalance
// listener inline (Java's MockConsumer.rebalance). Blocks until the listener
// callbacks have returned, so the GIL must be released — the listener
// trampolines run on the dispatcher thread and need it.
static PyObject* py_MockConsumer_rebalance(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* tps;
    if (!PyArg_ParseTuple(args, "KO", &h, &tps)) return NULL;
    const char** topics = NULL; int32_t* parts = NULL;
    Py_ssize_t n = tp_to_arrays(tps, &topics, &parts);
    if (n < 0) return NULL;
    const kafka_consumer_Consumer_t* c = (const kafka_consumer_Consumer_t*)(uintptr_t)h;
    kafka_common_KafkaError_t* e;
    Py_BEGIN_ALLOW_THREADS
    e = kafka_consumer_MockConsumer_rebalance(c, topics, parts, (int32_t)n);
    Py_END_ALLOW_THREADS
    PyMem_Free(topics); PyMem_Free(parts);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_MockConsumer_add_record(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset;
    Py_buffer key = {0}, value = {0};
    PyObject* key_obj; PyObject* value_obj;
    if (!PyArg_ParseTuple(args, "KsiLOO", &h, &topic, &partition, &offset, &key_obj, &value_obj))
        return NULL;
    const uint8_t* key_ptr = NULL; int32_t key_len = -1;
    const uint8_t* val_ptr = NULL; int32_t val_len = -1;
    int have_key = 0, have_val = 0;
    if (key_obj != Py_None) {
        if (PyObject_GetBuffer(key_obj, &key, PyBUF_SIMPLE) < 0) return NULL;
        have_key = 1; key_ptr = (const uint8_t*)key.buf; key_len = (int32_t)key.len;
    }
    if (value_obj != Py_None) {
        if (PyObject_GetBuffer(value_obj, &value, PyBUF_SIMPLE) < 0) {
            if (have_key) PyBuffer_Release(&key);
            return NULL;
        }
        have_val = 1; val_ptr = (const uint8_t*)value.buf; val_len = (int32_t)value.len;
    }
    kafka_common_KafkaError_t* e = kafka_consumer_MockConsumer_add_record(
        (kafka_consumer_Consumer_t*)(uintptr_t)h, topic, partition, offset,
        key_ptr, key_len, val_ptr, val_len);
    if (have_key) PyBuffer_Release(&key);
    if (have_val) PyBuffer_Release(&value);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_MockConsumer_update_end_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset;
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &offset)) return NULL;
    kafka_common_KafkaError_t* e = kafka_consumer_MockConsumer_update_end_offsets(
        (kafka_consumer_Consumer_t*)(uintptr_t)h, topic, partition, offset);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_MockConsumer_update_beginning_offsets(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset;
    if (!PyArg_ParseTuple(args, "KsiL", &h, &topic, &partition, &offset)) return NULL;
    kafka_common_KafkaError_t* e = kafka_consumer_MockConsumer_update_beginning_offsets(
        (kafka_consumer_Consumer_t*)(uintptr_t)h, topic, partition, offset);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_MockConsumer_update_partitions(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition_count;
    int leader_id; const char* leader_host; int leader_port;
    if (!PyArg_ParseTuple(args, "Ksiisi", &h, &topic, &partition_count, &leader_id, &leader_host, &leader_port))
        return NULL;
    kafka_common_KafkaError_t* e = kafka_consumer_MockConsumer_update_partitions(
        (kafka_consumer_Consumer_t*)(uintptr_t)h, topic, partition_count, leader_id, leader_host, leader_port);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_MockConsumer_set_poll_error(PyObject* self, PyObject* args) {
    unsigned long long h; const char* message;
    if (!PyArg_ParseTuple(args, "Ks", &h, &message)) return NULL;
    kafka_common_KafkaError_t* e = kafka_consumer_MockConsumer_set_poll_error(
        (kafka_consumer_Consumer_t*)(uintptr_t)h, message);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
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
static void admin_op_trampoline(kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "K", (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

static void admin_create_topics_trampoline(kafka_admin_CreateTopicsResult_t* r,
                                          kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_topics_trampoline(kafka_admin_DeleteTopicsResult_t* r,
                                          kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_topics_trampoline(kafka_admin_ListTopicsResult_t* r,
                                        kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_topics_trampoline(kafka_admin_DescribeTopicsResult_t* r,
                                            kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_create_partitions_trampoline(kafka_admin_CreatePartitionsResult_t* r,
                                              kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_records_trampoline(kafka_admin_DeleteRecordsResult_t* r,
                                           kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }

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
    kafka_common_KafkaError_t* err = NULL;
    kafka_admin_AdminClient_t* a = kafka_admin_AdminClient_new(props, &err);
    kafka_admin_AdminClientProperties_destroy(props);
    if (a == NULL) {
        const char* msg = err ? kafka_common_KafkaError_message(err) : NULL;
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create AdminClient");
        if (err) kafka_common_KafkaError_destroy(err);
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
    kafka_common_KafkaError_t* e = kafka_admin_MockAdminClient_timeout_next_request(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, n);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// Mock: seed beginning/end offsets. `spec` is a sequence of
// (topic:str, partition:int, offset:int); returns the error handle as an int.
static PyObject* mock_update_offsets(PyObject* args,
                                     kafka_common_KafkaError_t* (*update)(
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
    kafka_common_KafkaError_t* e = update((kafka_admin_AdminClient_t*)(uintptr_t)h, topics,
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
static PyObject* borrowed_error_to_py(const kafka_common_KafkaError_t* e) {
    if (e == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(isii)", kafka_common_KafkaError_code(e),
                         kafka_common_KafkaError_message(e),
                         kafka_common_KafkaError_is_retriable(e) ? 1 : 0,
                         kafka_common_KafkaError_is_fatal(e) ? 1 : 0);
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

// Builds a list of `count` node tuples via `get(index)`.
static PyObject* admin_node_list_to_py(const kafka_admin_TopicPartitionInfo_t* info, int32_t count,
                                       const kafka_common_Node_t* (*get)(const kafka_admin_TopicPartitionInfo_t*,
                                                                        int32_t)) {
    PyObject* out = PyList_New(count);
    if (out == NULL) return NULL;
    for (int32_t i = 0; i < count; i++) {
        PyObject* n = node_to_py(get(info, i));
        if (n == NULL) { Py_DECREF(out); return NULL; }
        PyList_SET_ITEM(out, i, n);
    }
    return out;
}

// (partition, leader, replicas, isr, elr, last_known_elr)
static PyObject* topic_partition_info_to_py(const kafka_admin_TopicPartitionInfo_t* info) {
    PyObject* leader = node_to_py(kafka_admin_TopicPartitionInfo_leader(info));
    PyObject* replicas = admin_node_list_to_py(info,
        kafka_admin_TopicPartitionInfo_replica_count(info), kafka_admin_TopicPartitionInfo_replica);
    PyObject* isr = admin_node_list_to_py(info,
        kafka_admin_TopicPartitionInfo_isr_count(info), kafka_admin_TopicPartitionInfo_isr);
    // Java's elr()/lastKnownElr() are null when the broker did not report the
    // set, which stays distinct from a reported-but-empty one.
    PyObject* elr = kafka_admin_TopicPartitionInfo_has_elr(info)
        ? admin_node_list_to_py(info, kafka_admin_TopicPartitionInfo_elr_count(info),
                                kafka_admin_TopicPartitionInfo_elr)
        : (Py_INCREF(Py_None), Py_None);
    PyObject* last_elr = kafka_admin_TopicPartitionInfo_has_last_known_elr(info)
        ? admin_node_list_to_py(info, kafka_admin_TopicPartitionInfo_last_known_elr_count(info),
                                kafka_admin_TopicPartitionInfo_last_known_elr)
        : (Py_INCREF(Py_None), Py_None);
    if (!leader || !replicas || !isr || !elr || !last_elr) {
        Py_XDECREF(leader); Py_XDECREF(replicas); Py_XDECREF(isr);
        Py_XDECREF(elr); Py_XDECREF(last_elr);
        return NULL;
    }
    return Py_BuildValue("(iNNNNN)", kafka_admin_TopicPartitionInfo_partition(info),
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
                                              kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_configs_trampoline(kafka_admin_DescribeConfigsResult_t* r,
                                              kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_configs_trampoline(kafka_admin_AlterConfigsResult_t* r,
                                           kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_config_resources_trampoline(kafka_admin_ListConfigResourcesResult_t* r,
                                                   kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_client_metrics_trampoline(kafka_admin_ListClientMetricsResourcesResult_t* r,
                                                 kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_log_dirs_trampoline(kafka_admin_DescribeLogDirsResult_t* r,
                                               kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_replica_log_dirs_trampoline(kafka_admin_AlterReplicaLogDirsResult_t* r,
                                                    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_replica_log_dirs_trampoline(kafka_admin_DescribeReplicaLogDirsResult_t* r,
                                                       kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }

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

static PyObject* py_Admin_list_client_metrics_resources_async(PyObject* self, PyObject* args) {
    unsigned long long h; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KiO", &h, &timeout_ms, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_admin_AdminClient_list_client_metrics_resources_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, timeout_ms,
        admin_list_client_metrics_trampoline, cb);
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

// [name]
static PyObject* py_ListClientMetricsResourcesResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListClientMetricsResourcesResult_t* r =
        (kafka_admin_ListClientMetricsResourcesResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_admin_ListClientMetricsResourcesResult_count(r);
    PyObject* out = PyList_New(n < 0 ? 0 : n);
    if (out == NULL) { kafka_admin_ListClientMetricsResourcesResult_destroy(r); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        PyObject* name = PyUnicode_FromString(
            kafka_admin_ListClientMetricsResourcesResult_get_name(r, i));
        if (name == NULL) { Py_DECREF(out); kafka_admin_ListClientMetricsResourcesResult_destroy(r); return NULL; }
        PyList_SET_ITEM(out, i, name);
    }
    kafka_admin_ListClientMetricsResourcesResult_destroy(r);
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
                                           kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_partition_reassignments_trampoline(
    kafka_admin_AlterPartitionReassignmentsResult_t* r,
    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_partition_reassignments_trampoline(
    kafka_admin_ListPartitionReassignmentsResult_t* r,
    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_offsets_trampoline(kafka_admin_ListOffsetsResult_t* r,
                                          kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }

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
                                         kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_consumer_groups_trampoline(kafka_admin_ListConsumerGroupsResult_t* r,
                                                  kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_consumer_groups_trampoline(kafka_admin_DescribeConsumerGroupsResult_t* r,
                                                      kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_classic_groups_trampoline(kafka_admin_DescribeClassicGroupsResult_t* r,
                                                     kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_consumer_group_offsets_trampoline(kafka_admin_ListConsumerGroupOffsetsResult_t* r,
                                                         kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_consumer_group_offsets_trampoline(kafka_admin_AlterConsumerGroupOffsetsResult_t* r,
                                                          kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_consumer_group_offsets_trampoline(kafka_admin_DeleteConsumerGroupOffsetsResult_t* r,
                                                           kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_consumer_groups_trampoline(kafka_admin_DeleteConsumerGroupsResult_t* r,
                                                    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_remove_members_trampoline(kafka_admin_RemoveMembersFromConsumerGroupResult_t* r,
                                            kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }

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

static PyObject* py_Admin_list_consumer_groups_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* states; PyObject* types; int timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOOiO", &h, &states, &types, &timeout_ms, &cb)) return NULL;

    const char** s = NULL; const char** t = NULL;
    Py_ssize_t ns = build_string_array(states, &s);
    if (ns < 0) return NULL;
    Py_ssize_t nt = build_string_array(types, &t);
    if (nt < 0) { PyMem_Free(s); return NULL; }

    Py_INCREF(cb);
    kafka_admin_AdminClient_list_consumer_groups_async(
        (kafka_admin_AdminClient_t*)(uintptr_t)h, s, (int32_t)ns, t, (int32_t)nt, timeout_ms,
        admin_list_consumer_groups_trampoline, cb);
    PyMem_Free(s); PyMem_Free(t);
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

// (group_id, is_simple, members, partition_assignor, group_type, state,
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
        // Eleven format units for eleven arguments, in the order
        // `_to_consumer_group_description` unpacks them:
        //   s     O          N        s                   s           s
        //   group is_simple  members  partition_assignor  group_type  state
        //   s            N            N     N            N
        //   group_state  coordinator  acls  group_epoch  target_epoch
        // Worth counting by hand: no test can reach this branch, because Java's
        // own MockAdminClient throws for describeConsumerGroups, so a wrong
        // arity would surface only against a real broker.
        "(sONssssNNNN)", kafka_admin_ConsumerGroupDescription_group_id(d),
        kafka_admin_ConsumerGroupDescription_is_simple_consumer_group(d) ? Py_True : Py_False,
        members, kafka_admin_ConsumerGroupDescription_partition_assignor(d),
        kafka_admin_ConsumerGroupDescription_group_type(d),
        kafka_admin_ConsumerGroupDescription_state(d),
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

// ([(group_id, is_simple, group_state, state, group_type), ...], [error, ...])
static PyObject* py_ListConsumerGroupsResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_admin_ListConsumerGroupsResult_t* r =
        (kafka_admin_ListConsumerGroupsResult_t*)(uintptr_t)ptr;
    int32_t nv = kafka_admin_ListConsumerGroupsResult_valid_count(r);
    int32_t ne = kafka_admin_ListConsumerGroupsResult_error_count(r);
    PyObject* valid = PyList_New(nv < 0 ? 0 : nv);
    PyObject* errors = PyList_New(ne < 0 ? 0 : ne);
    if (!valid || !errors) goto fail;
    for (int32_t i = 0; i < nv; i++) {
        const kafka_admin_ConsumerGroupListing_t* g =
            kafka_admin_ListConsumerGroupsResult_get_valid(r, i);
        PyObject* group_state =
            optional_str_to_py(kafka_admin_ConsumerGroupListing_group_state(g));
        PyObject* state = optional_str_to_py(kafka_admin_ConsumerGroupListing_state(g));
        PyObject* type = optional_str_to_py(kafka_admin_ConsumerGroupListing_group_type(g));
        if (!group_state || !state || !type) {
            Py_XDECREF(group_state); Py_XDECREF(state); Py_XDECREF(type); goto fail;
        }
        PyObject* row = Py_BuildValue(
            "(sONNN)", kafka_admin_ConsumerGroupListing_group_id(g),
            kafka_admin_ConsumerGroupListing_is_simple_consumer_group(g) ? Py_True : Py_False,
            group_state, state, type);
        if (row == NULL) goto fail;
        PyList_SET_ITEM(valid, i, row);
    }
    for (int32_t i = 0; i < ne; i++) {
        PyObject* e = borrowed_error_to_py(kafka_admin_ListConsumerGroupsResult_get_error(r, i));
        if (e == NULL) goto fail;
        PyList_SET_ITEM(errors, i, e);
    }
    kafka_admin_ListConsumerGroupsResult_destroy(r);
    return Py_BuildValue("(NN)", valid, errors);
fail:
    Py_XDECREF(valid); Py_XDECREF(errors);
    kafka_admin_ListConsumerGroupsResult_destroy(r);
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
// arity is instead checked statically by `cargo xtask check-bindings`, and the
// field *order* by review against the matching `_to_*` unpacker in admin.py.
// ---------------------------------------------------------------------------

static void admin_create_acls_trampoline(kafka_admin_CreateAclsResult_t* r,
                                         kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_acls_trampoline(kafka_admin_DescribeAclsResult_t* r,
                                           kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_delete_acls_trampoline(kafka_admin_DeleteAclsResult_t* r,
                                         kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_client_quotas_trampoline(kafka_admin_DescribeClientQuotasResult_t* r,
                                                    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_client_quotas_trampoline(kafka_admin_AlterClientQuotasResult_t* r,
                                                 kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }

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
        // non-literal format is unverifiable by `cargo xtask check-bindings`,
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
static PyObject* acl_binding_to_py(const kafka_common_AclBinding_t* b) {
    if (b == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(isissii)",
                         kafka_common_AclBinding_resource_type(b),
                         kafka_common_AclBinding_resource_name(b),
                         kafka_common_AclBinding_pattern_type(b),
                         kafka_common_AclBinding_principal(b),
                         kafka_common_AclBinding_host(b),
                         kafka_common_AclBinding_operation(b),
                         kafka_common_AclBinding_permission_type(b));
}

// Same seven fields, but the three strings use 'z' so a NULL (Java's "match
// any") becomes None rather than crashing on PyUnicode_FromString(NULL).
static PyObject* acl_binding_filter_to_py(const kafka_common_AclBindingFilter_t* f) {
    if (f == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(izizzii)",
                         kafka_common_AclBindingFilter_resource_type(f),
                         kafka_common_AclBindingFilter_resource_name(f),
                         kafka_common_AclBindingFilter_pattern_type(f),
                         kafka_common_AclBindingFilter_principal(f),
                         kafka_common_AclBindingFilter_host(f),
                         kafka_common_AclBindingFilter_operation(f),
                         kafka_common_AclBindingFilter_permission_type(f));
}

// [(entity_type, entity_name_or_None)] — a None name is Java's null map value,
// the built-in default entity, which is not the empty name.
static PyObject* client_quota_entity_to_py(const kafka_common_ClientQuotaEntity_t* e) {
    if (e == NULL) Py_RETURN_NONE;
    int32_t n = kafka_common_ClientQuotaEntity_entry_count(e);
    PyObject* pairs = PyTuple_New(n < 0 ? 0 : n);
    if (pairs == NULL) return NULL;
    for (int32_t i = 0; i < n; i++) {
        PyObject* pair = Py_BuildValue("(sz)",
                                       kafka_common_ClientQuotaEntity_get_entry_type(e, i),
                                       kafka_common_ClientQuotaEntity_get_entry_name(e, i));
        if (pair == NULL) { Py_DECREF(pairs); return NULL; }
        PyTuple_SET_ITEM(pairs, i, pair);
    }
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
// statically by `cargo xtask check-bindings` and their field order by review
// against the matching `_to_*` unpacker in admin.py. The mock *does* implement
// the four delegation-token RPCs and both feature RPCs, so those drains are
// exercised end to end by test_admin.py.
// ---------------------------------------------------------------------------

static void admin_describe_user_scram_credentials_trampoline(
    kafka_admin_DescribeUserScramCredentialsResult_t* r,
    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_alter_user_scram_credentials_trampoline(
    kafka_admin_AlterUserScramCredentialsResult_t* r,
    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_create_delegation_token_trampoline(kafka_admin_CreateDelegationTokenResult_t* r,
                                                     kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_renew_delegation_token_trampoline(kafka_admin_RenewDelegationTokenResult_t* r,
                                                    kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_expire_delegation_token_trampoline(kafka_admin_ExpireDelegationTokenResult_t* r,
                                                     kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_delegation_token_trampoline(kafka_admin_DescribeDelegationTokenResult_t* r,
                                                       kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_features_trampoline(kafka_admin_DescribeFeaturesResult_t* r,
                                               kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_update_features_trampoline(kafka_admin_UpdateFeaturesResult_t* r,
                                             kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }

// ---- value converters -------------------------------------------------------

// (principal_type, name, token_authenticated) — the field order
// `_to_kafka_principal` unpacks.
static PyObject* kafka_principal_to_py(const kafka_common_KafkaPrincipal_t* p) {
    if (p == NULL) Py_RETURN_NONE;
    return Py_BuildValue("(ssO)", kafka_common_KafkaPrincipal_principal_type(p),
                         kafka_common_KafkaPrincipal_name(p),
                         kafka_common_KafkaPrincipal_token_authenticated(p) ? Py_True : Py_False);
}

// (token_id, owner, requester, [renewers], issue_ts, expiry_ts, max_ts,
//  hmac_bytes, hmac_base64) — the field order `_to_delegation_token` unpacks.
// The HMAC crosses as `bytes` rather than `str`: it is a raw MAC and can
// contain interior NULs, so it needs the explicit length that `y#` carries.
static PyObject* delegation_token_to_py(const kafka_common_DelegationToken_t* t) {
    if (t == NULL) Py_RETURN_NONE;
    const kafka_common_TokenInformation_t* info = kafka_common_DelegationToken_token_info(t);
    int32_t renewer_count = kafka_common_TokenInformation_renewer_count(info);
    PyObject* renewers = PyList_New(renewer_count < 0 ? 0 : renewer_count);
    if (renewers == NULL) return NULL;
    for (int32_t i = 0; i < renewer_count; i++) {
        PyObject* renewer = kafka_principal_to_py(kafka_common_TokenInformation_get_renewer(info, i));
        if (renewer == NULL) { Py_DECREF(renewers); return NULL; }
        PyList_SET_ITEM(renewers, i, renewer);
    }
    PyObject* owner = kafka_principal_to_py(kafka_common_TokenInformation_owner(info));
    PyObject* requester = kafka_principal_to_py(kafka_common_TokenInformation_token_requester(info));
    if (owner == NULL || requester == NULL) {
        Py_XDECREF(owner); Py_XDECREF(requester); Py_DECREF(renewers);
        return NULL;
    }
    int32_t hmac_len = 0;
    const uint8_t* hmac = kafka_common_DelegationToken_hmac(t, &hmac_len);
    // 'O' (not 'N') plus an explicit, unconditional Py_DECREF below: 'N'
    // steals its reference only when do_mkvalue actually runs for that item,
    // which do_mktuple skips entirely if its own PyTuple_New fails (OOM) —
    // in that case an 'N' argument is never consumed and leaks. 'O' plus a
    // decref that runs regardless of Py_BuildValue's outcome makes ownership
    // independent of that internal control flow. See node_to_py() above for
    // the established precedent of this pattern in this file.
    PyObject* out = Py_BuildValue("(sOOOLLLy#s)", kafka_common_TokenInformation_token_id(info),
                         owner, requester, renewers,
                         (long long)kafka_common_TokenInformation_issue_timestamp(info),
                         (long long)kafka_common_TokenInformation_expiry_timestamp(info),
                         (long long)kafka_common_TokenInformation_max_timestamp(info),
                         (const char*)hmac, (Py_ssize_t)(hmac_len < 0 ? 0 : hmac_len),
                         kafka_common_DelegationToken_hmac_as_base64_string(t));
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
    kafka_common_KafkaError_t* e = kafka_admin_MockAdminClient_set_feature_levels(
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
// `cargo xtask check-bindings` and their field order by review against the
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
                                                kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_describe_transactions_trampoline(kafka_admin_DescribeTransactionsResult_t* r,
                                                   kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_fence_producers_trampoline(kafka_admin_FenceProducersResult_t* r,
                                             kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }
static void admin_list_transactions_trampoline(kafka_admin_ListTransactionsResult_t* r,
                                               kafka_common_KafkaError_t* e, void* ud) { fire_handle_cb(r, e, ud); }

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
    {"Producer_new", py_Producer_new, METH_VARARGS, "Create batching mock producer"},
    {"KafkaProducer_new", py_KafkaProducer_new, METH_VARARGS, "Create batching Kafka producer"},
    {"Producer_send", py_Producer_send, METH_VARARGS, "Send record to batch"},
    {"Producer_on_space_available", py_Producer_on_space_available, METH_VARARGS,
     "Register a callback fired when buffer space frees; returns True if "
     "space is already available"},
    {"Producer_test_set_paused", py_Producer_test_set_paused, METH_VARARGS,
     "Test-only: pause/resume the send task to exercise backpressure"},
    {"Producer_shutdown", py_Producer_shutdown, METH_VARARGS,
     "Stop/join the C batching threads (step 1 of close)"},
    {"Producer_close_async", py_Producer_close_async, METH_VARARGS,
     "Async Rust-side close; cb(error_int) (step 2 of close)"},
    {"Producer_destroy", py_Producer_destroy, METH_VARARGS,
     "Free the Rust handle + C struct (step 3 of close)"},
    {"Producer_flush", py_Producer_flush, METH_VARARGS, "Flush producer"},
    {"Producer_metrics", py_Producer_metrics, METH_VARARGS,
     "Point-in-time metrics snapshot; returns list[dict] or None"},
    {"Producer_partitions_for", py_Producer_partitions_for, METH_VARARGS,
     "Partition metadata for a topic; returns (PartitionInfoList_handle, error)"},
    {"Producer_flush_async", py_Producer_flush_async, METH_VARARGS,
     "Async flush; cb(error_int)"},
    {"Producer_partitions_for_async", py_Producer_partitions_for_async, METH_VARARGS,
     "Async partitions_for; cb(PartitionInfoList_handle_int, error_int)"},
    {"Producer_init_transactions", py_Producer_init_transactions, METH_VARARGS,
     "initTransactions (blocking); returns error_int (0 = success)"},
    {"Producer_begin_transaction", py_Producer_begin_transaction, METH_VARARGS,
     "beginTransaction (non-blocking state transition); returns error_int"},
    {"Producer_commit_transaction", py_Producer_commit_transaction, METH_VARARGS,
     "commitTransaction (blocking); returns error_int"},
    {"Producer_abort_transaction", py_Producer_abort_transaction, METH_VARARGS,
     "abortTransaction (blocking); returns error_int"},
    {"Producer_send_offsets_to_transaction", py_Producer_send_offsets_to_transaction, METH_VARARGS,
     "sendOffsetsToTransaction(offsets, group_metadata) (blocking); returns error_int"},
    {"Producer_init_transactions_async", py_Producer_init_transactions_async, METH_VARARGS,
     "Async initTransactions; cb(error_int)"},
    {"Producer_begin_transaction_async", py_Producer_begin_transaction_async, METH_VARARGS,
     "Async beginTransaction; cb(error_int)"},
    {"Producer_commit_transaction_async", py_Producer_commit_transaction_async, METH_VARARGS,
     "Async commitTransaction; cb(error_int)"},
    {"Producer_abort_transaction_async", py_Producer_abort_transaction_async, METH_VARARGS,
     "Async abortTransaction; cb(error_int)"},
    {"Producer_send_offsets_to_transaction_async", py_Producer_send_offsets_to_transaction_async, METH_VARARGS,
     "Async sendOffsetsToTransaction(offsets, group_metadata, cb); cb(error_int)"},
    {"MockProducer_complete_next", py_MockProducer_complete_next, METH_VARARGS,
     "Complete the next pending send successfully"},
    {"MockProducer_error_next", py_MockProducer_error_next, METH_VARARGS,
     "Complete the next pending send with an error"},
    {"MockProducer_history_count", py_MockProducer_history_count, METH_VARARGS,
     "Return the number of records in the sent history"},
    {"MockProducer_clear", py_MockProducer_clear, METH_VARARGS,
     "Clear the sent history and pending completions"},
    {"MockProducer_set_commit_transaction_error", py_MockProducer_set_commit_transaction_error,
     METH_VARARGS, "Test hook: install/clear the mock's commitTransaction error"},
    {"MockProducer_sent_offsets", py_MockProducer_sent_offsets, METH_VARARGS,
     "Test hook: whether offsets were staged in the current transaction"},
    {"MockProducer_committed_offset", py_MockProducer_committed_offset, METH_VARARGS,
     "Test hook: committed offset for (group_id, topic, partition) or None"},
    {"RecordMetadata_destroy", py_RecordMetadata_destroy, METH_VARARGS,
     "Destroy RecordMetadata handle"},
    {"RecordMetadata_copy", py_RecordMetadata_copy, METH_VARARGS,
     "Copy all fields via callback and destroy handle"},
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
    {"Consumer_MockConsumer_new", py_Consumer_MockConsumer_new, METH_VARARGS, "Create a MockConsumer"},
    {"Consumer_KafkaConsumer_new", py_Consumer_KafkaConsumer_new, METH_VARARGS, "Create a KafkaConsumer"},
    {"Consumer_destroy", py_Consumer_destroy, METH_VARARGS, "Destroy a consumer handle"},
    {"Consumer_wakeup", py_Consumer_wakeup, METH_VARARGS, "Wake up a blocked operation"},
    {"Consumer_poll_async", py_Consumer_poll_async, METH_VARARGS, "Async poll; cb(records_int, error_int)"},
    {"Consumer_subscribe_async", py_Consumer_subscribe_async, METH_VARARGS, "Async subscribe; cb(error_int)"},
    {"Consumer_subscribe_with_listener_async", py_Consumer_subscribe_with_listener_async, METH_VARARGS,
     "Async subscribe with a rebalance-listener adapter object; cb(error_int)"},
    {"Consumer_unsubscribe_async", py_Consumer_unsubscribe_async, METH_VARARGS, "Async unsubscribe; cb(error_int)"},
    {"Consumer_assign_async", py_Consumer_assign_async, METH_VARARGS, "Async assign; cb(error_int)"},
    {"Consumer_pause_async", py_Consumer_pause_async, METH_VARARGS, "Async pause; cb(error_int)"},
    {"Consumer_resume_async", py_Consumer_resume_async, METH_VARARGS, "Async resume; cb(error_int)"},
    {"Consumer_seek_to_beginning_async", py_Consumer_seek_to_beginning_async, METH_VARARGS, "Async seek_to_beginning; cb(error_int)"},
    {"Consumer_seek_to_end_async", py_Consumer_seek_to_end_async, METH_VARARGS, "Async seek_to_end; cb(error_int)"},
    {"Consumer_commit_sync_async", py_Consumer_commit_sync_async, METH_VARARGS, "Async commit (current positions); cb(error_int)"},
    {"Consumer_commit_sync_offsets_async", py_Consumer_commit_sync_offsets_async, METH_VARARGS, "Async commit (offsets); cb(error_int)"},
    {"Consumer_close_async", py_Consumer_close_async, METH_VARARGS, "Async close; cb(error_int)"},
    {"Consumer_position_async", py_Consumer_position_async, METH_VARARGS, "Async position; cb(position, error_int)"},
    {"Consumer_committed_async", py_Consumer_committed_async, METH_VARARGS, "Async committed; cb(offset_map_int, error_int)"},
    {"Consumer_offsets_for_times_async", py_Consumer_offsets_for_times_async, METH_VARARGS, "Async offsets_for_times; cb(map_int, error_int)"},
    {"Consumer_beginning_offsets_async", py_Consumer_beginning_offsets_async, METH_VARARGS, "Async beginning_offsets; cb(map_int, error_int)"},
    {"Consumer_end_offsets_async", py_Consumer_end_offsets_async, METH_VARARGS, "Async end_offsets; cb(map_int, error_int)"},
    {"Consumer_partitions_for_async", py_Consumer_partitions_for_async, METH_VARARGS, "Async partitions_for; cb(list_int, error_int)"},
    {"Consumer_list_topics_async", py_Consumer_list_topics_async, METH_VARARGS, "Async list_topics; cb(map_int, error_int)"},
    {"Consumer_seek_async", py_Consumer_seek_async, METH_VARARGS, "Async seek; cb(error_int)"},
    {"Consumer_seek_with_metadata_async", py_Consumer_seek_with_metadata_async, METH_VARARGS, "Async seek with metadata; cb(error_int)"},
    {"Consumer_enforce_rebalance", py_Consumer_enforce_rebalance, METH_VARARGS, "Sync enforce_rebalance; returns error_int"},
    {"Consumer_commit_async", py_Consumer_commit_async, METH_VARARGS,
     "commitAsync([callback]); callback(offset_map_int, error_int); returns error_int"},
    {"Consumer_commit_async_offsets", py_Consumer_commit_async_offsets, METH_VARARGS,
     "commitAsync(offsets[, callback]); returns error_int"},
    {"Consumer_assignment", py_Consumer_assignment, METH_VARARGS, "Current assignment as list[(topic, partition)]"},
    {"Consumer_subscription", py_Consumer_subscription, METH_VARARGS, "Current subscription as list[str]"},
    {"Consumer_paused", py_Consumer_paused, METH_VARARGS, "Paused partitions as list[(topic, partition)]"},
    {"Consumer_metrics", py_Consumer_metrics, METH_VARARGS,
     "Metrics snapshot as list[dict] with name/group/description/tags/value"},
    {"Consumer_group_metadata", py_Consumer_group_metadata, METH_VARARGS, "Group metadata tuple"},
    {"Consumer_client_id", py_Consumer_client_id, METH_VARARGS, "Client id string"},
    {"Consumer_current_lag", py_Consumer_current_lag, METH_VARARGS, "Current lag int or None"},
    // ---- ConsumerHandle (reentrancy handle; safe from inside callbacks) ----
    {"Consumer_handle", py_Consumer_handle, METH_VARARGS, "New reentrancy handle for a consumer"},
    {"ConsumerHandle_destroy", py_ConsumerHandle_destroy, METH_VARARGS, "Destroy a reentrancy handle"},
    {"ConsumerHandle_wakeup", py_ConsumerHandle_wakeup, METH_VARARGS, "Wake up the owning consumer"},
    {"ConsumerHandle_assignment", py_ConsumerHandle_assignment, METH_VARARGS, "Assignment as list[(topic, partition)]"},
    {"ConsumerHandle_subscription", py_ConsumerHandle_subscription, METH_VARARGS, "Subscription as list[str]"},
    {"ConsumerHandle_paused", py_ConsumerHandle_paused, METH_VARARGS, "Paused partitions as list[(topic, partition)]"},
    {"ConsumerHandle_assign", py_ConsumerHandle_assign, METH_VARARGS, "Assign; returns error_int"},
    {"ConsumerHandle_seek", py_ConsumerHandle_seek, METH_VARARGS, "Seek; returns error_int"},
    {"ConsumerHandle_seek_with_metadata", py_ConsumerHandle_seek_with_metadata, METH_VARARGS, "Seek with metadata; returns error_int"},
    {"ConsumerHandle_seek_to_beginning", py_ConsumerHandle_seek_to_beginning, METH_VARARGS, "Seek to beginning; returns error_int"},
    {"ConsumerHandle_seek_to_end", py_ConsumerHandle_seek_to_end, METH_VARARGS, "Seek to end; returns error_int"},
    {"ConsumerHandle_pause", py_ConsumerHandle_pause, METH_VARARGS, "Pause; returns error_int"},
    {"ConsumerHandle_resume", py_ConsumerHandle_resume, METH_VARARGS, "Resume; returns error_int"},
    {"ConsumerHandle_position", py_ConsumerHandle_position, METH_VARARGS, "Position; returns (position, error_int)"},
    {"ConsumerHandle_position_timeout", py_ConsumerHandle_position_timeout, METH_VARARGS, "Position with timeout; returns (position, error_int)"},
    {"ConsumerHandle_committed", py_ConsumerHandle_committed, METH_VARARGS, "Committed; returns (OffsetMap_int, error_int)"},
    {"ConsumerHandle_beginning_offsets", py_ConsumerHandle_beginning_offsets, METH_VARARGS, "Beginning offsets; returns (LongOffsetMap_int, error_int)"},
    {"ConsumerHandle_end_offsets", py_ConsumerHandle_end_offsets, METH_VARARGS, "End offsets; returns (LongOffsetMap_int, error_int)"},
    {"ConsumerHandle_offsets_for_times", py_ConsumerHandle_offsets_for_times, METH_VARARGS, "Offsets for times; returns (OffsetAndTimestampMap_int, error_int)"},
    {"ConsumerHandle_commit_sync", py_ConsumerHandle_commit_sync, METH_VARARGS, "Commit current positions; returns error_int"},
    {"ConsumerHandle_commit_sync_offsets", py_ConsumerHandle_commit_sync_offsets, METH_VARARGS, "Commit offsets; returns error_int"},
    {"ConsumerHandle_commit_async", py_ConsumerHandle_commit_async, METH_VARARGS, "Async commit current positions; returns error_int"},
    {"ConsumerHandle_commit_async_offsets", py_ConsumerHandle_commit_async_offsets, METH_VARARGS, "Async commit offsets; returns error_int"},
    {"ConsumerRecords_wrap", py_ConsumerRecords_wrap, METH_VARARGS, "Wrap a records handle int into a ConsumerRecords"},
    {"OffsetMap_drain", py_OffsetMap_drain, METH_VARARGS, "Drain+destroy an OffsetMap handle into a dict"},
    {"OffsetAndTimestampMap_drain", py_OffsetAndTimestampMap_drain, METH_VARARGS, "Drain+destroy an OffsetAndTimestampMap handle into a dict"},
    {"LongOffsetMap_drain", py_LongOffsetMap_drain, METH_VARARGS, "Drain+destroy a LongOffsetMap handle into a dict"},
    {"PartitionInfoList_drain", py_PartitionInfoList_drain, METH_VARARGS, "Drain+destroy a PartitionInfoList handle into a list"},
    {"TopicPartitionInfoMap_drain", py_TopicPartitionInfoMap_drain, METH_VARARGS, "Drain+destroy a TopicPartitionInfoMap handle into a dict"},
    {"MockConsumer_rebalance", py_MockConsumer_rebalance, METH_VARARGS, "Mock: drive a rebalance to an assignment; returns error_int"},
    {"MockConsumer_add_record", py_MockConsumer_add_record, METH_VARARGS, "Mock: add a record; returns error_int"},
    {"MockConsumer_update_end_offsets", py_MockConsumer_update_end_offsets, METH_VARARGS, "Mock: set end offsets; returns error_int"},
    {"MockConsumer_update_beginning_offsets", py_MockConsumer_update_beginning_offsets, METH_VARARGS, "Mock: set beginning offsets; returns error_int"},
    {"MockConsumer_update_partitions", py_MockConsumer_update_partitions, METH_VARARGS, "Mock: register partition metadata; returns error_int"},
    {"MockConsumer_set_poll_error", py_MockConsumer_set_poll_error, METH_VARARGS, "Mock: inject a poll error; returns error_int"},
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
    {"Admin_list_client_metrics_resources_async", py_Admin_list_client_metrics_resources_async, METH_VARARGS,
     "Async listClientMetricsResources; cb(result_int, error_int)"},
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
    {"ListClientMetricsResourcesResult_drain", py_ListClientMetricsResourcesResult_drain, METH_VARARGS,
     "Drain+destroy a ListClientMetricsResourcesResult handle into a list"},
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
    {"Admin_list_consumer_groups_async", py_Admin_list_consumer_groups_async, METH_VARARGS,
     "Async listConsumerGroups; cb(result_int, error_int)"},
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
    {"ListConsumerGroupsResult_drain", py_ListConsumerGroupsResult_drain, METH_VARARGS,
     "Drain+destroy a ListConsumerGroupsResult handle into (valid, errors)"},
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
    if (PyType_Ready(&BorrowedBytesType) < 0) {
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
