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

static PyObject* py_Producer_close(PyObject* self, PyObject* args) {
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
    kafka_producer_Producer_close(producer->producer, NULL);
    kafka_producer_Producer_destroy(producer->producer);
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
    {"Producer_close", py_Producer_close, METH_VARARGS, "Close batching producer"},
    {"Producer_flush", py_Producer_flush, METH_VARARGS, "Flush producer"},
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

    return m;
}
