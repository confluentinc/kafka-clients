#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <confluent_kafka.h>
#include <threads.h>
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

static PyObject* py_Producer_flush(PyObject* self, PyObject* args) {
    unsigned long long producer_ptr;

    if (!PyArg_ParseTuple(args, "K", &producer_ptr)) {
        return NULL;
    }

    Producer* producer = (Producer*)producer_ptr;
    kafka_common_KafkaError_t *err = NULL;
    kafka_producer_Producer_flush(producer->producer, &err);
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
    kafka_common_KafkaError_t* err =
        kafka_producer_Producer_partitions_for(producer->producer, topic, &list);
    return Py_BuildValue("KK",
        (unsigned long long)(uintptr_t)list,
        (unsigned long long)(uintptr_t)err);
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

static PyObject* py_Consumer_group_metadata(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ConsumerGroupMetadata_t* m =
        kafka_consumer_Consumer_group_metadata((kafka_consumer_Consumer_t*)(uintptr_t)h);
    if (m == NULL) Py_RETURN_NONE;
    const char* group_id = kafka_consumer_ConsumerGroupMetadata_group_id(m);
    int32_t gen = kafka_consumer_ConsumerGroupMetadata_generation_id(m);
    const char* member = kafka_consumer_ConsumerGroupMetadata_member_id(m);
    const char* instance = kafka_consumer_ConsumerGroupMetadata_group_instance_id(m);
    PyObject* out = Py_BuildValue("(sisz)", group_id ? group_id : "", gen,
                                  member ? member : "", instance);
    kafka_consumer_ConsumerGroupMetadata_destroy(m);
    return out;
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

// Method definitions
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
    {"Producer_partitions_for", py_Producer_partitions_for, METH_VARARGS,
     "Partition metadata for a topic; returns (PartitionInfoList_handle, error)"},
    {"Producer_flush_async", py_Producer_flush_async, METH_VARARGS,
     "Async flush; cb(error_int)"},
    {"Producer_partitions_for_async", py_Producer_partitions_for_async, METH_VARARGS,
     "Async partitions_for; cb(PartitionInfoList_handle_int, error_int)"},
    {"MockProducer_complete_next", py_MockProducer_complete_next, METH_VARARGS,
     "Complete the next pending send successfully"},
    {"MockProducer_error_next", py_MockProducer_error_next, METH_VARARGS,
     "Complete the next pending send with an error"},
    {"MockProducer_history_count", py_MockProducer_history_count, METH_VARARGS,
     "Return the number of records in the sent history"},
    {"MockProducer_clear", py_MockProducer_clear, METH_VARARGS,
     "Clear the sent history and pending completions"},
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

    return m;
}
