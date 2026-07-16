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

        if (producer->next_batches_to_send == NULL) {
            mtx_unlock(&producer->record_batches_mutex);
            continue;
        }

        head_batch_node = producer->next_batches_to_send;
        tail_batch_node = producer->last_accumulating_batch;
        producer->next_batches_to_send = NULL;
        producer->last_accumulating_batch = NULL;
        mtx_unlock(&producer->record_batches_mutex);

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

    if (producer->last_accumulating_batch->count >= PRODUCER_RECORD_SLOT_THRESHOLD) {
        cnd_signal(&producer->record_batches_new_record_cnd);
    }
    mtx_unlock(&producer->record_batches_mutex);
    Py_END_ALLOW_THREADS
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
    Py_BEGIN_ALLOW_THREADS
    mtx_lock(&producer->record_batches_mutex);
    producer->closed = 1;
    cnd_signal(&producer->record_batches_new_record_cnd);
    mtx_unlock(&producer->record_batches_mutex);
    thrd_join(producer->send_thread, NULL);
    Py_END_ALLOW_THREADS

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

// ===========================================================================
// Consumer records + share consumer — marshaling-only bridge to the Rust FFI.
//
// Like the sibling consumer bindings, this half runs NO background threads and
// holds NO business logic: all orchestration (waiting, signal handling, future
// resolution) lives in pure Python (share_consumer.py). Async FFI callbacks
// fire on the Rust dispatcher thread; the trampolines below re-acquire the GIL
// and hand the raw result handles back to Python as ints (the Python callback
// then schedules resolution / drains the handle). Handles cross the boundary as
// ints, exactly like producer handles.
//
// The share-consumer poll returns the same kafka_consumer_ConsumerRecords_t as
// the regular consumer, so the zero-copy record machinery (borrowed memoryviews
// with the batch-keepalive chain) is shared unchanged.
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

// KIP-932: how many times this record has been delivered (None when the broker
// did not report it — e.g. the mock share consumer).
static PyObject* ConsumerRecord_get_delivery_count(ConsumerRecordObject* self, void* closure) {
    int32_t count = 0;
    if (kafka_consumer_ConsumerRecord_delivery_count(self->rec, &count)) {
        return PyLong_FromLong(count);
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
    {"delivery_count", (getter)ConsumerRecord_get_delivery_count, NULL, "Delivery count or None (KIP-932)", NULL},
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

// records handle int -> ConsumerRecords object (used by the poll callback path)
static PyObject* py_ConsumerRecords_wrap(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    return wrap_records((kafka_consumer_ConsumerRecords_t*)(uintptr_t)ptr);
}

// ---- argument marshaling helper --------------------------------------------
//
// The char* produced below point into the Python str objects held by the
// caller's argument list, which stays alive for the whole FFI call; the FFI
// copies them into owned Rust data synchronously before returning, so freeing
// the array right after the call is safe.
//
// list[str] -> char* array. Returns count (>=0), or -1 on error (exception
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

// ---- trampolines (run on the Rust dispatcher thread) -----------------------

// op callback: (error, user_data) -> py_cb(error_int). Shared by every
// void-returning async share op (subscribe/unsubscribe/close/commit_async).
static void consumer_op_trampoline(kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* r = PyObject_CallFunction(cb, "K", (unsigned long long)(uintptr_t)error);
    if (r) Py_DECREF(r); else PyErr_Print();
    Py_DECREF(cb);
    PyGILState_Release(g);
}

// poll callback: (records, error, user_data) -> py_cb(records_int, error_int).
// The share poll callback typedef is byte-identical, so this is reused for it.
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

// Shared body for handle-returning value callbacks (here: commit): hand the
// opaque result handle + error back to Python as ints. Python then drains the
// handle via the matching *_drain function.
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

// commit_sync callback: (ShareCommitResult*, error, user_data). Two owned
// handles; forwards to fire_handle_cb with the exact typed signature (no
// function-pointer cast).
static void share_commit_trampoline(kafka_consumer_ShareCommitResult_t* result,
                                    kafka_common_KafkaError_t* error, void* ud) {
    fire_handle_cb(result, error, ud);
}

// ---- KafkaError / TopicIdPartition marshaling helpers ----------------------

// Build a Python view of a KafkaError: None if e is NULL, else a
// (code, message, is_retriable, is_fatal) tuple. When `owned` is nonzero the
// handle is destroyed after reading (for handles the callback owns); when zero
// it is left intact (for borrowed sub-handles like ShareCommitResult errors,
// which the owning result frees). Extracting the fields eagerly here avoids
// handing Python a pointer that dangles once the owning container is destroyed.
static PyObject* kafka_error_to_py_fields(const kafka_common_KafkaError_t* e, int owned) {
    if (e == NULL) {
        Py_RETURN_NONE;
    }
    int code = (int)kafka_common_KafkaError_code(e);
    const char* msg = kafka_common_KafkaError_message(e);
    PyObject* retriable = PyBool_FromLong(kafka_common_KafkaError_is_retriable(e) ? 1 : 0);
    PyObject* fatal = PyBool_FromLong(kafka_common_KafkaError_is_fatal(e) ? 1 : 0);
    PyObject* out = Py_BuildValue("(isNN)", code, msg ? msg : "", retriable, fatal);
    if (owned) {
        kafka_common_KafkaError_destroy((kafka_common_KafkaError_t*)e);
    }
    return out;
}

// A borrowed TopicIdPartition -> (topic:str, topic_id:bytes[16], partition:int)
// tuple; hashable, so it can key the drained dicts. The topic id is 16 raw
// big-endian bytes.
static PyObject* topic_id_partition_to_py(const kafka_common_TopicIdPartition_t* tip) {
    if (tip == NULL) Py_RETURN_NONE;
    const char* topic = kafka_common_TopicIdPartition_topic(tip);
    const uint8_t* tid = kafka_common_TopicIdPartition_topic_id(tip);
    int32_t part = kafka_common_TopicIdPartition_partition(tip);
    PyObject* py_topic = PyUnicode_FromString(topic ? topic : "");
    PyObject* py_tid = PyBytes_FromStringAndSize(tid ? (const char*)tid : "", tid ? 16 : 0);
    if (py_topic == NULL || py_tid == NULL) {
        Py_XDECREF(py_topic);
        Py_XDECREF(py_tid);
        return NULL;
    }
    return Py_BuildValue("(NNi)", py_topic, py_tid, part);
}

// ShareAcknowledgeOffsets -> dict[(topic, topic_id, partition) -> set[int]].
// Reads the borrowed sub-handles; does NOT destroy the offsets container (the
// caller owns that decision).
static PyObject* share_ack_offsets_to_dict(const kafka_consumer_ShareAcknowledgeOffsets_t* offsets) {
    int32_t np = kafka_consumer_ShareAcknowledgeOffsets_partition_count(offsets);
    PyObject* d = PyDict_New();
    if (d == NULL) return NULL;
    for (int32_t i = 0; i < np; i++) {
        const kafka_common_TopicIdPartition_t* tip =
            kafka_consumer_ShareAcknowledgeOffsets_get_partition(offsets, i);
        PyObject* key = topic_id_partition_to_py(tip);
        int32_t no = kafka_consumer_ShareAcknowledgeOffsets_offset_count(offsets, i);
        PyObject* s = PySet_New(NULL);
        if (key == NULL || s == NULL) {
            Py_XDECREF(key); Py_XDECREF(s); Py_DECREF(d);
            return NULL;
        }
        for (int32_t j = 0; j < no; j++) {
            int64_t off = kafka_consumer_ShareAcknowledgeOffsets_get_offset(offsets, i, j);
            PyObject* po = PyLong_FromLongLong(off);
            if (po == NULL || PySet_Add(s, po) < 0) {
                Py_XDECREF(po); Py_DECREF(key); Py_DECREF(s); Py_DECREF(d);
                return NULL;
            }
            Py_DECREF(po);
        }
        if (PyDict_SetItem(d, key, s) < 0) {
            Py_DECREF(key); Py_DECREF(s); Py_DECREF(d);
            return NULL;
        }
        Py_DECREF(key); Py_DECREF(s);
    }
    return d;
}

// Registered acknowledgement-commit callback (persistent). Fires on the Rust
// dispatcher thread for every ack-commit completion. `user_data` is the stored
// Python callable, kept alive by an INCREF at registration time — it is NOT
// DECREF'd here (that happens only on clear/replace), so the callable survives
// across invocations. The offsets handle is OWNED by this callback and must be
// destroyed after marshaling; the error handle is likewise owned.
static void ack_commit_callback_trampoline(const kafka_consumer_ShareAcknowledgeOffsets_t* offsets,
                                           const kafka_common_KafkaError_t* error, void* user_data) {
    PyObject* cb = (PyObject*)user_data;
    PyGILState_STATE g = PyGILState_Ensure();
    PyObject* d = share_ack_offsets_to_dict(offsets);
    // We've marshaled everything we need; the callback owns the container.
    kafka_consumer_ShareAcknowledgeOffsets_destroy(
        (kafka_consumer_ShareAcknowledgeOffsets_t*)offsets);
    PyObject* err_obj = kafka_error_to_py_fields(error, 1 /* owned -> destroy */);
    if (d != NULL && err_obj != NULL) {
        PyObject* r = PyObject_CallFunctionObjArgs(cb, d, err_obj, NULL);
        if (r) Py_DECREF(r); else PyErr_Print();
    } else {
        PyErr_Print();
    }
    Py_XDECREF(d);
    Py_XDECREF(err_obj);
    PyGILState_Release(g);
}

// ---- share consumer: constructors / lifecycle ------------------------------
static PyObject* py_KafkaShareConsumer_new(PyObject* self, PyObject* args) {
    PyObject* config_dict;
    if (!PyArg_ParseTuple(args, "O", &config_dict)) return NULL;
    if (!PyDict_Check(config_dict)) {
        PyErr_SetString(PyExc_TypeError, "config must be a dict");
        return NULL;
    }
    kafka_consumer_ShareConsumerProperties_t* props =
        kafka_consumer_ShareConsumerProperties_new();
    if (props == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ShareConsumerProperties");
        return NULL;
    }
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(config_dict, &pos, &key, &value)) {
        const char* k = PyUnicode_AsUTF8(key);
        const char* v = PyUnicode_AsUTF8(value);
        if (k == NULL || v == NULL) {
            kafka_consumer_ShareConsumerProperties_destroy(props);
            PyErr_SetString(PyExc_TypeError, "config keys and values must be strings");
            return NULL;
        }
        kafka_consumer_ShareConsumerProperties_put(props, k, v);
    }
    kafka_common_KafkaError_t* err = NULL;
    kafka_consumer_ShareConsumer_t* c = kafka_consumer_KafkaShareConsumer_new(props, &err);
    kafka_consumer_ShareConsumerProperties_destroy(props);
    if (c == NULL) {
        const char* msg = err ? kafka_common_KafkaError_message(err) : NULL;
        PyErr_SetString(PyExc_RuntimeError, msg ? msg : "Failed to create KafkaShareConsumer");
        if (err) kafka_common_KafkaError_destroy(err);
        return NULL;
    }
    return PyLong_FromVoidPtr(c);
}

static PyObject* py_MockShareConsumer_new(PyObject* self, PyObject* args) {
    kafka_consumer_ShareConsumer_t* c = kafka_consumer_MockShareConsumer_new();
    if (c == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create MockShareConsumer");
        return NULL;
    }
    return PyLong_FromVoidPtr(c);
}

static PyObject* py_ShareConsumer_destroy(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ShareConsumer_t* c = (kafka_consumer_ShareConsumer_t*)(uintptr_t)h;
    Py_BEGIN_ALLOW_THREADS
    kafka_consumer_ShareConsumer_destroy(c);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject* py_ShareConsumer_wakeup(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_consumer_ShareConsumer_wakeup((kafka_consumer_ShareConsumer_t*)(uintptr_t)h);
    Py_RETURN_NONE;
}

// ---- share consumer: async submit functions --------------------------------
static PyObject* py_ShareConsumer_poll_async(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &timeout_ms, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_ShareConsumer_poll_async((kafka_consumer_ShareConsumer_t*)(uintptr_t)h,
                                            timeout_ms, consumer_poll_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_ShareConsumer_subscribe_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* topics; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &topics, &cb)) return NULL;
    const char** arr = NULL;
    Py_ssize_t n = topics_to_array(topics, &arr);
    if (n < 0) return NULL;
    Py_INCREF(cb);
    kafka_consumer_ShareConsumer_subscribe_async((kafka_consumer_ShareConsumer_t*)(uintptr_t)h,
                                                 arr, (int32_t)n, consumer_op_trampoline, cb);
    PyMem_Free(arr);
    Py_RETURN_NONE;
}

static PyObject* py_ShareConsumer_unsubscribe_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_ShareConsumer_unsubscribe_async((kafka_consumer_ShareConsumer_t*)(uintptr_t)h,
                                                   consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_ShareConsumer_close_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_ShareConsumer_close_async((kafka_consumer_ShareConsumer_t*)(uintptr_t)h,
                                             consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_ShareConsumer_commit_sync_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_ShareConsumer_commit_sync_async((kafka_consumer_ShareConsumer_t*)(uintptr_t)h,
                                                   share_commit_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_ShareConsumer_commit_sync_timeout_async(PyObject* self, PyObject* args) {
    unsigned long long h; long long timeout_ms; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KLO", &h, &timeout_ms, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_ShareConsumer_commit_sync_timeout_async(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, timeout_ms, share_commit_trampoline, cb);
    Py_RETURN_NONE;
}

static PyObject* py_ShareConsumer_commit_async_async(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* cb;
    if (!PyArg_ParseTuple(args, "KO", &h, &cb)) return NULL;
    Py_INCREF(cb);
    kafka_consumer_ShareConsumer_commit_async_async((kafka_consumer_ShareConsumer_t*)(uintptr_t)h,
                                                    consumer_op_trampoline, cb);
    Py_RETURN_NONE;
}

// ---- share consumer: sync-local ops (return error handle int, 0 on success) -
static PyObject* py_ShareConsumer_commit_async(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_common_KafkaError_t* e = kafka_consumer_ShareConsumer_commit_async(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_ShareConsumer_acknowledge(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* rec_obj;
    if (!PyArg_ParseTuple(args, "KO", &h, &rec_obj)) return NULL;
    if (!PyObject_TypeCheck(rec_obj, &ConsumerRecordType)) {
        PyErr_SetString(PyExc_TypeError, "record must be a ConsumerRecord");
        return NULL;
    }
    ConsumerRecordObject* r = (ConsumerRecordObject*)rec_obj;
    kafka_common_KafkaError_t* e = kafka_consumer_ShareConsumer_acknowledge(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, r->rec);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_ShareConsumer_acknowledge_with_type(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* rec_obj; int ack_type;
    if (!PyArg_ParseTuple(args, "KOi", &h, &rec_obj, &ack_type)) return NULL;
    if (!PyObject_TypeCheck(rec_obj, &ConsumerRecordType)) {
        PyErr_SetString(PyExc_TypeError, "record must be a ConsumerRecord");
        return NULL;
    }
    ConsumerRecordObject* r = (ConsumerRecordObject*)rec_obj;
    kafka_common_KafkaError_t* e = kafka_consumer_ShareConsumer_acknowledge_with_type(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, r->rec, ack_type);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_ShareConsumer_acknowledge_by_offset(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset; int ack_type;
    if (!PyArg_ParseTuple(args, "KsiLi", &h, &topic, &partition, &offset, &ack_type)) return NULL;
    kafka_common_KafkaError_t* e = kafka_consumer_ShareConsumer_acknowledge_by_offset(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, topic, partition, offset, ack_type);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

// ---- share consumer: sync state reads --------------------------------------
static PyObject* py_ShareConsumer_subscription(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    kafka_common_KafkaError_t* err = NULL;
    kafka_consumer_StringList_t* list =
        kafka_consumer_ShareConsumer_subscription((kafka_consumer_ShareConsumer_t*)(uintptr_t)h, &err);
    if (list == NULL) {
        // Surface the error handle (concurrent-access rejection / closed) as an
        // int so the Python side can raise; 0 for the plain-null case.
        return Py_BuildValue("(OK)", Py_None, (unsigned long long)(uintptr_t)err);
    }
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
    return Py_BuildValue("(NK)", out, (unsigned long long)0);
}

// acquisition_lock_timeout_ms -> (value_or_None, error_int)
static PyObject* py_ShareConsumer_acquisition_lock_timeout_ms(PyObject* self, PyObject* args) {
    unsigned long long h;
    if (!PyArg_ParseTuple(args, "K", &h)) return NULL;
    int32_t out_ms = 0;
    kafka_common_KafkaError_t* err = NULL;
    bool present = kafka_consumer_ShareConsumer_acquisition_lock_timeout_ms(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, &out_ms, &err);
    PyObject* value = present ? PyLong_FromLong(out_ms) : (Py_INCREF(Py_None), Py_None);
    return Py_BuildValue("(NK)", value, (unsigned long long)(uintptr_t)err);
}

// ---- share consumer: registered ack-commit callback (persistent) -----------
//
// set_acknowledgement_commit_callback(handle, new_cb_or_None, old_cb_or_None):
//   * SET: INCREF new_cb and hand it to Rust as user_data.
//   * On Rust rejection: undo the INCREF and return the error int (old stays
//     registered, so old_cb is NOT touched).
//   * On success: the previously registered callable (old_cb, if any) is no
//     longer referenced by Rust, so DECREF it. This realizes "INCREF on set,
//     DECREF on clear/replace" without a per-invocation DECREF (which would
//     free the callable while it is still registered). The wrapper tracks the
//     currently-registered bridge and passes it as old_cb.
static PyObject* py_ShareConsumer_set_acknowledgement_commit_callback(PyObject* self, PyObject* args) {
    unsigned long long h; PyObject* new_cb; PyObject* old_cb;
    if (!PyArg_ParseTuple(args, "KOO", &h, &new_cb, &old_cb)) return NULL;

    kafka_consumer_ShareConsumer_AcknowledgementCommitCallback_t fn = NULL;
    void* ud = NULL;
    int incremented = 0;
    if (new_cb != Py_None) {
        if (!PyCallable_Check(new_cb)) {
            PyErr_SetString(PyExc_TypeError, "callback must be callable or None");
            return NULL;
        }
        Py_INCREF(new_cb);
        incremented = 1;
        fn = ack_commit_callback_trampoline;
        ud = (void*)new_cb;
    }
    kafka_common_KafkaError_t* e = kafka_consumer_ShareConsumer_set_acknowledgement_commit_callback(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, fn, ud);
    if (e != NULL) {
        if (incremented) Py_DECREF(new_cb);  // registration failed; old still active
        return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
    }
    if (old_cb != Py_None) {
        Py_DECREF(old_cb);  // Rust dropped the previous registration
    }
    return PyLong_FromLong(0);
}

// ---- share consumer: drain helpers -----------------------------------------
// ShareCommitResult handle int -> dict[(topic, topic_id, partition) ->
// None | (code, message, retriable, fatal)]; destroys the handle.
static PyObject* py_ShareCommitResult_drain(PyObject* self, PyObject* args) {
    unsigned long long ptr;
    if (!PyArg_ParseTuple(args, "K", &ptr)) return NULL;
    kafka_consumer_ShareCommitResult_t* result = (kafka_consumer_ShareCommitResult_t*)(uintptr_t)ptr;
    int32_t n = kafka_consumer_ShareCommitResult_count(result);
    PyObject* d = PyDict_New();
    if (d == NULL) { kafka_consumer_ShareCommitResult_destroy(result); return NULL; }
    for (int32_t i = 0; i < n; i++) {
        const kafka_common_TopicIdPartition_t* tip =
            kafka_consumer_ShareCommitResult_get_partition(result, i);
        const kafka_common_KafkaError_t* err =
            kafka_consumer_ShareCommitResult_get_error(result, i);  // borrowed
        PyObject* key = topic_id_partition_to_py(tip);
        PyObject* val = kafka_error_to_py_fields(err, 0 /* borrowed -> keep */);
        if (key == NULL || val == NULL || PyDict_SetItem(d, key, val) < 0) {
            Py_XDECREF(key); Py_XDECREF(val); Py_DECREF(d);
            kafka_consumer_ShareCommitResult_destroy(result);
            return NULL;
        }
        Py_DECREF(key); Py_DECREF(val);
    }
    kafka_consumer_ShareCommitResult_destroy(result);
    return d;
}

// ---- mock share consumer drivers -------------------------------------------
// NOTE: the FFI takes key/value BEFORE offset (differs from the regular
// consumer's add_record); this Python-visible entry point mirrors that FFI
// order exactly, so the wrapper — not this C code — does the reorder.
static PyObject* py_MockShareConsumer_add_record(PyObject* self, PyObject* args) {
    unsigned long long h; const char* topic; int partition; long long offset;
    PyObject* key_obj; PyObject* value_obj;
    if (!PyArg_ParseTuple(args, "KsiOOL", &h, &topic, &partition, &key_obj, &value_obj, &offset))
        return NULL;
    Py_buffer key = {0}, value = {0};
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
    kafka_common_KafkaError_t* e = NULL;
    kafka_consumer_MockShareConsumer_add_record(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, topic, partition,
        key_ptr, key_len, val_ptr, val_len, offset, &e);
    if (have_key) PyBuffer_Release(&key);
    if (have_val) PyBuffer_Release(&value);
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)e);
}

static PyObject* py_MockShareConsumer_set_client_instance_id(PyObject* self, PyObject* args) {
    unsigned long long h; Py_buffer id = {0};
    if (!PyArg_ParseTuple(args, "Ky*", &h, &id)) return NULL;
    if (id.len != 16) {
        PyBuffer_Release(&id);
        PyErr_SetString(PyExc_ValueError, "client instance id must be exactly 16 bytes");
        return NULL;
    }
    kafka_consumer_MockShareConsumer_set_client_instance_id(
        (kafka_consumer_ShareConsumer_t*)(uintptr_t)h, (const uint8_t*)id.buf);
    PyBuffer_Release(&id);
    Py_RETURN_NONE;
}

// Method definitions
static PyMethodDef ProducerNativeMethods[] = {
    {"Producer_new", py_Producer_new, METH_VARARGS, "Create batching mock producer"},
    {"KafkaProducer_new", py_KafkaProducer_new, METH_VARARGS, "Create batching Kafka producer"},
    {"Producer_send", py_Producer_send, METH_VARARGS, "Send record to batch"},
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
    // ---- Consumer records (shared with the share consumer poll path) ----
    {"ConsumerRecords_wrap", py_ConsumerRecords_wrap, METH_VARARGS,
     "Wrap a records handle int into a ConsumerRecords"},
    // ---- Share consumer (KIP-932) ----
    {"KafkaShareConsumer_new", py_KafkaShareConsumer_new, METH_VARARGS, "Create a KafkaShareConsumer"},
    {"MockShareConsumer_new", py_MockShareConsumer_new, METH_VARARGS, "Create a MockShareConsumer"},
    {"ShareConsumer_destroy", py_ShareConsumer_destroy, METH_VARARGS, "Destroy a share consumer handle"},
    {"ShareConsumer_wakeup", py_ShareConsumer_wakeup, METH_VARARGS, "Wake up a blocked operation"},
    {"ShareConsumer_poll_async", py_ShareConsumer_poll_async, METH_VARARGS, "Async poll; cb(records_int, error_int)"},
    {"ShareConsumer_subscribe_async", py_ShareConsumer_subscribe_async, METH_VARARGS, "Async subscribe; cb(error_int)"},
    {"ShareConsumer_unsubscribe_async", py_ShareConsumer_unsubscribe_async, METH_VARARGS, "Async unsubscribe; cb(error_int)"},
    {"ShareConsumer_close_async", py_ShareConsumer_close_async, METH_VARARGS, "Async close; cb(error_int)"},
    {"ShareConsumer_commit_sync_async", py_ShareConsumer_commit_sync_async, METH_VARARGS, "Async commit_sync; cb(result_int, error_int)"},
    {"ShareConsumer_commit_sync_timeout_async", py_ShareConsumer_commit_sync_timeout_async, METH_VARARGS, "Async commit_sync with timeout; cb(result_int, error_int)"},
    {"ShareConsumer_commit_async_async", py_ShareConsumer_commit_async_async, METH_VARARGS, "Async dispatch of commit_async; cb(error_int)"},
    {"ShareConsumer_commit_async", py_ShareConsumer_commit_async, METH_VARARGS, "Fire-and-forget commit_async; returns error_int"},
    {"ShareConsumer_acknowledge", py_ShareConsumer_acknowledge, METH_VARARGS, "Acknowledge (ACCEPT) a polled record; returns error_int"},
    {"ShareConsumer_acknowledge_with_type", py_ShareConsumer_acknowledge_with_type, METH_VARARGS, "Acknowledge a polled record with a type; returns error_int"},
    {"ShareConsumer_acknowledge_by_offset", py_ShareConsumer_acknowledge_by_offset, METH_VARARGS, "Acknowledge (topic, partition, offset) with a type; returns error_int"},
    {"ShareConsumer_subscription", py_ShareConsumer_subscription, METH_VARARGS, "Current subscription as (list[str] | None, error_int)"},
    {"ShareConsumer_acquisition_lock_timeout_ms", py_ShareConsumer_acquisition_lock_timeout_ms, METH_VARARGS, "Acquisition lock timeout as (int | None, error_int)"},
    {"ShareConsumer_set_acknowledgement_commit_callback", py_ShareConsumer_set_acknowledgement_commit_callback, METH_VARARGS, "Register/replace/clear the persistent ack-commit callback; returns error_int"},
    {"ShareCommitResult_drain", py_ShareCommitResult_drain, METH_VARARGS, "Drain+destroy a ShareCommitResult handle into a dict"},
    {"MockShareConsumer_add_record", py_MockShareConsumer_add_record, METH_VARARGS, "Mock: add a record (key/value before offset); returns error_int"},
    {"MockShareConsumer_set_client_instance_id", py_MockShareConsumer_set_client_instance_id, METH_VARARGS, "Mock: set the client instance id (16 raw bytes)"},
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

    // AcknowledgeType constants (KIP-932), mirroring the C ABI enum values so
    // the wrapper's Pythonic enum has a single source of truth.
    if (PyModule_AddIntConstant(m, "AcknowledgeType_ACCEPT",
                                kafka_consumer_AcknowledgeType_t_ACCEPT) < 0 ||
        PyModule_AddIntConstant(m, "AcknowledgeType_RELEASE",
                                kafka_consumer_AcknowledgeType_t_RELEASE) < 0 ||
        PyModule_AddIntConstant(m, "AcknowledgeType_REJECT",
                                kafka_consumer_AcknowledgeType_t_REJECT) < 0 ||
        PyModule_AddIntConstant(m, "AcknowledgeType_RENEW",
                                kafka_consumer_AcknowledgeType_t_RENEW) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
