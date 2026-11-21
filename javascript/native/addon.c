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

// The entry point of the Node-API addon `confluent-kafka-javascript`: the instance
// data, the helpers every client uses, and the registration of every native
// function. The public JS surface lives in src/; the addon exports only what
// src/ calls, all of it registered here.

// dladdr and Dl_info (addon_pin_loaded) are a GNU extension that glibc's
// <dlfcn.h> declares only under _GNU_SOURCE, and binding.gyp compiles with
// -std=c11, which defines no feature macro: without this the addon does not
// compile on Linux. It must come before the first #include. macOS declares them
// without it.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

AddonData* addon_data(napi_env env) {
    void* data = NULL;
    if (napi_get_instance_data(env, &data) != napi_ok) return NULL;
    return (AddonData*)data;
}

void js_throw_last_napi_error(napi_env env, const char* what) {
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) return;
    const napi_extended_error_info* info = NULL;
    napi_get_last_error_info(env, &info);
    char message[512];
    snprintf(message, sizeof(message), "Node-API call failed: %s (%s)", what,
             (info != NULL && info->error_message != NULL) ? info->error_message : "unknown error");
    napi_throw_error(env, NULL, message);
}

void delete_ref(napi_env env, napi_ref* ref) {
    if (*ref != NULL) {
        napi_delete_reference(env, *ref);
        *ref = NULL;
    }
}

napi_value call_ref(napi_env env, napi_ref ref, size_t argc, napi_value* argv, char** thrown) {
    napi_value fn, global, result = NULL;
    if (napi_get_reference_value(env, ref, &fn) != napi_ok || fn == NULL || napi_get_global(env, &global) != napi_ok)
        return NULL;
    napi_status status = napi_call_function(env, global, fn, argc, argv, &result);
    if (status == napi_ok) return result;
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) {
        if (thrown == NULL) {
            js_warn_thrown(env);
            return NULL;
        }
        napi_value exc, text;
        napi_get_and_clear_last_exception(env, &exc);
        if (napi_coerce_to_string(env, exc, &text) == napi_ok) {
            size_t len = 0;
            if (napi_get_value_string_utf8(env, text, NULL, 0, &len) == napi_ok) {
                char* s = (char*)malloc(len + 1);
                if (s != NULL && napi_get_value_string_utf8(env, text, s, len + 1, &len) == napi_ok) *thrown = s;
                else free(s);
            }
        }
    }
    return NULL;
}

void js_warn_thrown(napi_env env) {
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) != napi_ok || !pending) return;
    napi_value exc;
    if (napi_get_and_clear_last_exception(env, &exc) != napi_ok) return;
    AddonData* data = addon_data(env);
    napi_value fn, global, ignored;
    if (data == NULL || data->warn == NULL || napi_get_reference_value(env, data->warn, &fn) != napi_ok || fn == NULL
        || napi_get_global(env, &global) != napi_ok)
        return;
    if (napi_call_function(env, global, fn, 1, &exc, &ignored) != napi_ok) {
        // The warning itself failed: nothing further can report it.
        napi_get_and_clear_last_exception(env, &ignored);
    }
}

static void addon_data_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    AddonData* d = (AddonData*)data;
    if (d->error_factory != NULL) napi_delete_reference(env, d->error_factory);
    if (d->metadata_factory != NULL) napi_delete_reference(env, d->metadata_factory);
    if (d->warn != NULL) napi_delete_reference(env, d->warn);
    free(d);
}

// probe() -> true: proves the addon loaded and its link to libconfluent_kafka
// resolves, by building and freeing an empty kafka_Map_t.
static napi_value js_probe(napi_env env, napi_callback_info info) {
    (void)info;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    err = FFI_ERR(kafka_Map_destroy_err(map, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, true, &result), NULL);
    return result;
}

// previewNoticeDue() -> true the first time any environment of the process
// (the main thread or a worker: the addon is loaded once per process, and
// stays loaded, addon_pin_loaded) asks,
// false after: src/internal/lifecycle.ts emits the preview notice once per
// process.
static napi_value js_preview_notice_due(napi_env env, napi_callback_info info) {
    (void)info;
    static atomic_int asked = 0;
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, atomic_exchange(&asked, 1) == 0, &result), NULL);
    return result;
}

atomic_int fault_armed = 0;

// The failures armed for each step, and the names faultNext() takes, by FaultStep.
static atomic_int fault_left[FAULT_STEPS];
static const char* const FAULT_NAMES[FAULT_STEPS] = {NULL, "notify", "metadata", "error", "slot", "partitions", "close", "enqueue", "failure"};

int fault_take(FaultStep step) {
    int left = atomic_load(&fault_left[step]);
    while (left > 0) {
        if (atomic_compare_exchange_weak(&fault_left[step], &left, left - 1)) {
            atomic_fetch_sub(&fault_armed, 1);
            return 1;
        }
    }
    return 0;
}

// faultNext(step, times): arms `times` (1 when left out) more failures of
// `step` (notify, metadata, error, slot, partitions, close, enqueue or
// failure), each failing the next run of its site. faultNext(null) disarms
// every step and returns the failures still armed, a name each, so a test
// sees that the steps it armed ran. A process-wide switch. Test-only.
static napi_value js_fault_next(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    napi_valuetype type = napi_undefined;
    if (argc >= 1) NAPI_CHECK(env, napi_typeof(env, argv[0], &type), NULL);
    int step = type == napi_null && argc == 1 ? FAULT_NONE : -1;
    int32_t times = 1;
    if (type == napi_string && argc <= 2) {
        char name[16];
        size_t len = 0;
        NAPI_CHECK(env, napi_get_value_string_utf8(env, argv[0], name, sizeof(name), &len), NULL);
        for (int i = 1; i < FAULT_STEPS; i++)
            if (strcmp(name, FAULT_NAMES[i]) == 0) step = i;
        if (argc == 2 && (napi_get_value_int32(env, argv[1], &times) != napi_ok || times < 1)) step = -1;
    }
    if (step < 0) {
        napi_throw_type_error(env, NULL,
                              "faultNext(step, times): step null, or notify, metadata, error, slot, partitions, close, "
                              "enqueue or failure; times a positive count");
        return NULL;
    }
    if (step != FAULT_NONE) {
        atomic_fetch_add(&fault_left[step], times);
        atomic_fetch_add(&fault_armed, times);
        return NULL;
    }
    napi_value left;
    NAPI_CHECK(env, napi_create_array(env, &left), NULL);
    uint32_t n = 0;
    for (int i = 1; i < FAULT_STEPS; i++) {
        int armed = atomic_exchange(&fault_left[i], 0);
        atomic_fetch_sub(&fault_armed, armed);
        for (; armed > 0; armed--) {
            napi_value name;
            NAPI_CHECK(env, napi_create_string_utf8(env, FAULT_NAMES[i], NAPI_AUTO_LENGTH, &name), NULL);
            NAPI_CHECK(env, napi_set_element(env, left, n++, name), NULL);
        }
    }
    return left;
}

// Keeps the addon loaded until the process ends. Node unloads (dlclose) an
// addon once the last environment that loaded it ends, a worker that loaded it
// first included; the core's process-wide state (its runtime, its threads) and
// the once-per-process notice above must not be unloaded or reset with it.
// Best effort: a failure leaves Node's default.
static void addon_pin_loaded(void) {
    static atomic_int pinned = 0;
    if (atomic_exchange(&pinned, 1) != 0) return;
    Dl_info info;
    if (dladdr((void*)&addon_pin_loaded, &info) == 0 || info.dli_fname == NULL) return;
    void* self = dlopen(info.dli_fname, RTLD_LAZY | RTLD_NOLOAD | RTLD_NODELETE);
    (void)self;  // deliberately never closed
}

static napi_value addon_init(napi_env env, napi_value exports) {
    addon_pin_loaded();
    AddonData* data = (AddonData*)calloc(1, sizeof(AddonData));
    if (data == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    if (napi_set_instance_data(env, data, addon_data_finalize, NULL) != napi_ok) {
        free(data);
        js_throw_last_napi_error(env, "napi_set_instance_data");
        return NULL;
    }
    napi_property_descriptor props[] = {
        {"probe", NULL, js_probe, NULL, NULL, NULL, napi_default, NULL},
        {"setFactories", NULL, js_set_factories, NULL, NULL, NULL, napi_default, NULL},
        {"errorStats", NULL, js_error_stats, NULL, NULL, NULL, napi_default, NULL},
        {"previewNoticeDue", NULL, js_preview_notice_due, NULL, NULL, NULL, napi_default, NULL},
        {"faultNext", NULL, js_fault_next, NULL, NULL, NULL, napi_default, NULL},
        // producer.c
        {"producerCreate", NULL, js_producer_create, NULL, NULL, NULL, napi_default, NULL},
        {"producerSend", NULL, js_producer_send, NULL, NULL, NULL, napi_default, NULL},
        {"producerFlush", NULL, js_producer_flush, NULL, NULL, NULL, napi_default, NULL},
        {"producerPartitionsFor", NULL, js_producer_partitions_for, NULL, NULL, NULL, napi_default, NULL},
        {"producerMetrics", NULL, js_producer_metrics, NULL, NULL, NULL, napi_default, NULL},
        {"producerClose", NULL, js_producer_close, NULL, NULL, NULL, napi_default, NULL},
        {"producerPump", NULL, js_producer_pump, NULL, NULL, NULL, napi_default, NULL},
        {"mockProducerCreate", NULL, js_mock_producer_create, NULL, NULL, NULL, napi_default, NULL},
        {"mockProducerSend", NULL, js_mock_producer_send, NULL, NULL, NULL, napi_default, NULL},
        {"mockProducerHistory", NULL, js_mock_producer_history, NULL, NULL, NULL, napi_default, NULL},
        {"mockProducerClear", NULL, js_mock_producer_clear, NULL, NULL, NULL, napi_default, NULL},
        {"mockProducerSetError", NULL, js_mock_producer_set_error, NULL, NULL, NULL, napi_default, NULL},
        {"mockProducerCompleteNext", NULL, js_mock_producer_complete_next, NULL, NULL, NULL, napi_default, NULL},
        {"mockProducerState", NULL, js_mock_producer_state, NULL, NULL, NULL, napi_default, NULL},
        {"recordFutureGet", NULL, js_record_future_get, NULL, NULL, NULL, napi_default, NULL},
        {"producerStats", NULL, js_producer_stats, NULL, NULL, NULL, napi_default, NULL},
        // metrics.c
        {"metricsViewSize", NULL, js_metrics_view_size, NULL, NULL, NULL, napi_default, NULL},
        {"metricsViewHas", NULL, js_metrics_view_has, NULL, NULL, NULL, napi_default, NULL},
        {"metricsViewGet", NULL, js_metrics_view_get, NULL, NULL, NULL, napi_default, NULL},
        {"metricsViewEntries", NULL, js_metrics_view_entries, NULL, NULL, NULL, napi_default, NULL},
        {"metricValue", NULL, js_metric_value, NULL, NULL, NULL, napi_default, NULL},
        {"metricIsMeasurable", NULL, js_metric_is_measurable, NULL, NULL, NULL, napi_default, NULL},
        {"metricsStats", NULL, js_metrics_stats, NULL, NULL, NULL, napi_default, NULL},
        // admin.c, admin_values.c
        {"adminCreate", NULL, js_admin_create, NULL, NULL, NULL, napi_default, NULL},
        {"adminClose", NULL, js_admin_close, NULL, NULL, NULL, napi_default, NULL},
        {"adminResultFuture", NULL, js_admin_result_future, NULL, NULL, NULL, napi_default, NULL},
        {"adminResultFutures", NULL, js_admin_result_futures, NULL, NULL, NULL, napi_default, NULL},
        {"adminFutureWait", NULL, js_admin_future_wait, NULL, NULL, NULL, napi_default, NULL},
        {"adminFutureGet", NULL, js_admin_future_get, NULL, NULL, NULL, napi_default, NULL},
        {"adminDescribeCluster", NULL, js_admin_describe_cluster, NULL, NULL, NULL, napi_default, NULL},
        {"adminListTopics", NULL, js_admin_list_topics, NULL, NULL, NULL, napi_default, NULL},
        {"adminCreateTopics", NULL, js_admin_create_topics, NULL, NULL, NULL, napi_default, NULL},
        {"adminDeleteTopics", NULL, js_admin_delete_topics, NULL, NULL, NULL, napi_default, NULL},
        {"adminDescribeTopics", NULL, js_admin_describe_topics, NULL, NULL, NULL, napi_default, NULL},
        {"adminCreatePartitions", NULL, js_admin_create_partitions, NULL, NULL, NULL, napi_default, NULL},
        {"adminDeleteRecords", NULL, js_admin_delete_records, NULL, NULL, NULL, napi_default, NULL},
        {"adminDescribeConfigs", NULL, js_admin_describe_configs, NULL, NULL, NULL, napi_default, NULL},
        {"adminIncrementalAlterConfigs", NULL, js_admin_incremental_alter_configs, NULL, NULL, NULL, napi_default, NULL},
        {"adminListConfigResources", NULL, js_admin_list_config_resources, NULL, NULL, NULL, napi_default, NULL},
        {"adminDescribeLogDirs", NULL, js_admin_describe_log_dirs, NULL, NULL, NULL, napi_default, NULL},
        {"adminAlterReplicaLogDirs", NULL, js_admin_alter_replica_log_dirs, NULL, NULL, NULL, napi_default, NULL},
        {"adminDescribeReplicaLogDirs", NULL, js_admin_describe_replica_log_dirs, NULL, NULL, NULL, napi_default, NULL},
        {"adminElectLeaders", NULL, js_admin_elect_leaders, NULL, NULL, NULL, napi_default, NULL},
        {"adminAlterPartitionReassignments", NULL, js_admin_alter_partition_reassignments, NULL, NULL, NULL, napi_default, NULL},
        {"adminListPartitionReassignments", NULL, js_admin_list_partition_reassignments, NULL, NULL, NULL, napi_default, NULL},
        {"adminListOffsets", NULL, js_admin_list_offsets, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminUpdateOffsets", NULL, js_mock_admin_update_offsets, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminAddTopic", NULL, js_mock_admin_add_topic, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminMarkTopicForDeletion", NULL, js_mock_admin_mark_topic_for_deletion, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminSetFetchesRemainingUntilVisible", NULL, js_mock_admin_set_fetches_remaining_until_visible, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminCreate", NULL, js_mock_admin_create, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminController", NULL, js_mock_admin_controller, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminTimeoutNextRequest", NULL, js_mock_admin_timeout_next_request, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminBuilderNew", NULL, js_mock_admin_builder_new, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminBuilderSet", NULL, js_mock_admin_builder_set, NULL, NULL, NULL, napi_default, NULL},
        {"mockAdminBuilderBuild", NULL, js_mock_admin_builder_build, NULL, NULL, NULL, napi_default, NULL},
        {"adminStats", NULL, js_admin_stats, NULL, NULL, NULL, napi_default, NULL},
        // headers.c
        {"headerStats", NULL, js_header_stats, NULL, NULL, NULL, napi_default, NULL},
        // record_spy.c
        {"recordSpy", NULL, js_record_spy, NULL, NULL, NULL, napi_default, NULL},
    };
    NAPI_CHECK(env, napi_define_properties(env, exports, sizeof(props) / sizeof(props[0]), props), NULL);
    return exports;
}

NAPI_MODULE_INIT() {
    return addon_init(env, exports);
}
