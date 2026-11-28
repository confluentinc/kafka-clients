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

// The producer's metrics, read when asked: Java's metrics() is an
// unmodifiable live view, and a KafkaMetric kept from it measures on each
// metricValue(). The view a client's metrics() returns
// (`kafka_producer_Producer_metrics`) and each metric taken from it
// (`kafka_common_metrics_MetricsView_get`) are owned handles, each held by a
// JS object of its own whose finalizer destroys it: the view stays valid after
// the producer is destroyed, and a metric after the view is
// (src/internal/live_metrics.ts builds the map and the metrics over them).

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

// The counts metricsStats() reads, for the memory tests.
static atomic_long stat_views_made;
static atomic_long stat_views_freed;
static atomic_long stat_metrics_made;
static atomic_long stat_metrics_freed;

// The type tags of the two handle objects, so a handle of one kind is never
// read as the other.
static const napi_type_tag VIEW_TAG = {0x6b61666b616d7669ULL, 0x6577686e646c3031ULL};
static const napi_type_tag METRIC_TAG = {0x6b61666b616d6d65ULL, 0x747269636831ULL};

// ---- values -----------------------------------------------------------------

// A metric value as JS: a double or an int a number, a long a bigint (Java's
// long, javascript/CLAUDE.md, JavaScript Binding Conventions, Types), a string a
// string; the value freed.
static napi_value metric_value_take_to_js(napi_env env, kafka_common_MetricValue_t* value) {
    kafka_common_Error_t* ffi_err = NULL;
    napi_value v = NULL;
    if (value == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &v), NULL);
        return v;
    }
    int kind = kafka_common_MetricValue_DOUBLE;
    error_warn(env, 1, FFI_SET(kind, (int)kafka_common_MetricValue__enum_err(value, &ffi_err)));
    napi_status status = napi_ok;
    if (kind == kafka_common_MetricValue_STRING) {
        const char* text = NULL;
        error_warn(env, 1, FFI_SET(text, kafka_common_MetricValue_as_string_err(value, &ffi_err)));
        status = napi_create_string_utf8(env, text != NULL ? text : "", NAPI_AUTO_LENGTH, &v);
    } else if (kind == kafka_common_MetricValue_LONG) {
        int64_t n = 0;
        error_warn(env, 1, FFI_SET(n, kafka_common_MetricValue_as_long_err(value, &ffi_err)));
        status = napi_create_bigint_int64(env, n, &v);
    } else if (kind == kafka_common_MetricValue_INT) {
        int32_t n = 0;
        error_warn(env, 1, FFI_SET(n, kafka_common_MetricValue_as_int_err(value, &ffi_err)));
        status = napi_create_int32(env, n, &v);
    } else {
        double d = 0;
        error_warn(env, 1, FFI_SET(d, kafka_common_MetricValue_as_double_err(value, &ffi_err)));
        status = napi_create_double(env, d, &v);
    }
    error_warn(env, 1, FFI_ERR(kafka_common_MetricValue_destroy_err(value, &ffi_err)));
    return status == napi_ok ? v : NULL;
}

// A metric name's tags, the map of char * to char * sorted by key, as an array
// of [key, value] pairs in that order; the map freed.
static napi_value tags_take_to_js(napi_env env, kafka_Map_t* tags) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    if (tags != NULL) error_warn(env, 1, FFI_SET(size, kafka_Map_size_err(tags, &ffi_err)));
    napi_value arr;
    napi_value result = NULL;
    if (napi_create_array_with_length(env, (size_t)size, &arr) == napi_ok) {
        result = arr;
        for (int32_t i = 0; i < size && result != NULL; i++) {
            const char* key = NULL;
            const char* value = NULL;
            error_warn(env, 1, FFI_SET(key, (const char*)kafka_Map_key_err(tags, i, &ffi_err)));
            error_warn(env, 1, FFI_SET(value, (const char*)kafka_Map_value_err(tags, i, &ffi_err)));
            napi_value pair, k, v;
            if (napi_create_array_with_length(env, 2, &pair) != napi_ok
                || napi_create_string_utf8(env, key != NULL ? key : "", NAPI_AUTO_LENGTH, &k) != napi_ok
                || napi_create_string_utf8(env, value != NULL ? value : "", NAPI_AUTO_LENGTH, &v) != napi_ok
                || napi_set_element(env, pair, 0, k) != napi_ok || napi_set_element(env, pair, 1, v) != napi_ok
                || napi_set_element(env, arr, (uint32_t)i, pair) != napi_ok)
                result = NULL;
        }
    }
    if (tags != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(tags, &ffi_err)));
    return result;
}

// A borrowed metric name as { name, group, description, tags }, its tags an
// array of [key, value] pairs sorted by key.
static napi_value metric_name_to_js(napi_env env, const kafka_common_MetricName_t* name) {
    kafka_common_Error_t* ffi_err = NULL;
    const char* n = NULL;
    const char* g = NULL;
    const char* d = NULL;
    kafka_Map_t* tags = NULL;
    error_warn(env, 1, FFI_SET(n, kafka_common_MetricName_name_err(name, &ffi_err)));
    error_warn(env, 1, FFI_SET(g, kafka_common_MetricName_group_err(name, &ffi_err)));
    error_warn(env, 1, FFI_SET(d, kafka_common_MetricName_description_err(name, &ffi_err)));
    error_warn(env, 1, FFI_SET(tags, kafka_common_MetricName_tags_err(name, &ffi_err)));
    napi_value t = tags_take_to_js(env, tags);
    if (t == NULL) return NULL;
    napi_value obj, x;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, n != NULL ? n : "", NAPI_AUTO_LENGTH, &x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "name", x), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, g != NULL ? g : "", NAPI_AUTO_LENGTH, &x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "group", x), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, d != NULL ? d : "", NAPI_AUTO_LENGTH, &x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "description", x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "tags", t), NULL);
    return obj;
}

// ---- the handle objects -------------------------------------------------------

// The finalizer of a view's handle object: destroys the view. No JS runs.
static void view_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 0, FFI_ERR(kafka_common_metrics_MetricsView_destroy_err((kafka_common_metrics_MetricsView_t*)data, &ffi_err)));
    atomic_fetch_add(&stat_views_freed, 1);
}

// The finalizer of a metric's handle object: destroys the handle (the metric
// stays registered). No JS runs.
static void metric_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 0, FFI_ERR(kafka_common_metrics_KafkaMetric_destroy_err((kafka_common_metrics_KafkaMetric_t*)data, &ffi_err)));
    atomic_fetch_add(&stat_metrics_freed, 1);
}

// A handle object owning `ptr`, destroyed by `finalize` once the object is
// collected; on failure `ptr` is destroyed here, and NULL returned with a JS
// exception pending.
static napi_value handle_wrap(napi_env env, void* ptr, const napi_type_tag* tag, napi_finalize finalize) {
    napi_value obj;
    if (napi_create_object(env, &obj) != napi_ok || napi_type_tag_object(env, obj, tag) != napi_ok) {
        js_throw_last_napi_error(env, "creating a metrics handle");
        finalize(env, ptr, NULL);
        return NULL;
    }
    if (napi_wrap(env, obj, ptr, finalize, NULL, NULL) != napi_ok) {
        js_throw_last_napi_error(env, "napi_wrap");
        finalize(env, ptr, NULL);
        return NULL;
    }
    return obj;
}

napi_value metrics_view_take_to_js(napi_env env, kafka_common_metrics_MetricsView_t* view) {
    atomic_fetch_add(&stat_views_made, 1);
    return handle_wrap(env, view, &VIEW_TAG, view_finalize);
}

static napi_value metric_take_to_js(napi_env env, kafka_common_metrics_KafkaMetric_t* metric) {
    atomic_fetch_add(&stat_metrics_made, 1);
    return handle_wrap(env, metric, &METRIC_TAG, metric_finalize);
}

// The pointer a handle object of `tag` holds, or NULL with a TypeError
// pending.
static void* handle_unwrap(napi_env env, napi_value handle, const napi_type_tag* tag, const char* what) {
    bool tagged = false;
    void* data = NULL;
    napi_valuetype type = napi_undefined;
    if (napi_typeof(env, handle, &type) != napi_ok || type != napi_object
        || napi_check_object_type_tag(env, handle, tag, &tagged) != napi_ok || !tagged
        || napi_unwrap(env, handle, &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, what);
        return NULL;
    }
    return data;
}

// A metric handle as [fields, handle]: its name's fields, read through the
// metric, and the handle object, which now owns it. NULL with a JS exception
// pending on failure, the metric destroyed.
static napi_value metric_entry_take_to_js(napi_env env, kafka_common_metrics_KafkaMetric_t* metric) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_Metric_t* as_metric = NULL;
    const kafka_common_MetricName_t* name = NULL;
    napi_value handle = metric_take_to_js(env, metric);
    if (handle == NULL) return NULL;
    error_warn(env, 1, FFI_SET(as_metric, kafka_common_metrics_KafkaMetric__as_Metric_err(metric, &ffi_err)));
    if (as_metric != NULL) error_warn(env, 1, FFI_SET(name, kafka_common_Metric_metric_name_err(as_metric, &ffi_err)));
    if (name == NULL) {
        napi_throw_error(env, NULL, "a metric without a name");
        return NULL;
    }
    napi_value fields = metric_name_to_js(env, name);
    if (fields == NULL) return NULL;
    napi_value entry;
    NAPI_CHECK(env, napi_create_array_with_length(env, 2, &entry), NULL);
    NAPI_CHECK(env, napi_set_element(env, entry, 0, fields), NULL);
    NAPI_CHECK(env, napi_set_element(env, entry, 1, handle), NULL);
    return entry;
}

// ---- metric names from JS -----------------------------------------------------

// A JS string as a malloc'ed UTF-8 copy, or NULL. `*nul` is set when the text
// holds a NUL, which no registered name can be read as through the C ABI:
// such a name is absent. NULL with a TypeError pending when it is not a
// string.
static char* name_string(napi_env env, napi_value value, int* nul) {
    size_t len = 0;
    if (napi_get_value_string_utf8(env, value, NULL, 0, &len) != napi_ok) {
        napi_throw_type_error(env, NULL, "a metric name's fields must be strings");
        return NULL;
    }
    char* text = (char*)malloc(len + 1);
    if (text == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    if (napi_get_value_string_utf8(env, value, text, len + 1, &len) != napi_ok) {
        free(text);
        js_throw_last_napi_error(env, "napi_get_value_string_utf8");
        return NULL;
    }
    if (strlen(text) != len) *nul = 1;
    return text;
}

// The native name of (name, group, tags), tags an array of [key, value]
// strings: an owned MetricName with an empty description (equality reads the
// group, the name and the tags, as Java's). NULL with `*absent` set when a
// field holds a NUL; NULL with a JS exception pending on failure.
static kafka_common_MetricName_t* metric_name_build(napi_env env, napi_value name, napi_value group, napi_value tags,
                                                    int* absent) {
    uint32_t count = 0;
    if (napi_get_array_length(env, tags, &count) != napi_ok) {
        napi_throw_type_error(env, NULL, "tags must be an array of [key, value] pairs");
        return NULL;
    }
    char** strings = (char**)calloc((size_t)count * 2 + 2, sizeof(char*));
    if (strings == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    int nul = 0;
    int thrown = 0;
    strings[0] = name_string(env, name, &nul);
    strings[1] = strings[0] != NULL ? name_string(env, group, &nul) : NULL;
    if (strings[1] == NULL) thrown = 1;
    for (uint32_t i = 0; !thrown && i < count; i++) {
        napi_value pair, k, v;
        if (napi_get_element(env, tags, i, &pair) != napi_ok || napi_get_element(env, pair, 0, &k) != napi_ok
            || napi_get_element(env, pair, 1, &v) != napi_ok) {
            js_throw_last_napi_error(env, "reading a tag");
            thrown = 1;
            break;
        }
        strings[2 + 2 * i] = name_string(env, k, &nul);
        strings[3 + 2 * i] = strings[2 + 2 * i] != NULL ? name_string(env, v, &nul) : NULL;
        if (strings[3 + 2 * i] == NULL) thrown = 1;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_MetricName_t* result = NULL;
    if (!thrown && !nul) {
        // A C-built kafka_Map_t borrows its char * keys and values: they are
        // freed after the map; MetricName_new copies everything.
        kafka_Map_t* map = NULL;
        kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
        for (uint32_t i = 0; err == NULL && i < count; i++)
            err = FFI_ERR(kafka_Map_put_err(map, strings[2 + 2 * i], strings[3 + 2 * i], &ffi_err));
        if (err == NULL) err = FFI_SET(result, kafka_common_MetricName_new_err(strings[0], strings[1], "", map, &ffi_err));
        if (map != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
        if (err != NULL) {
            error_throw_owned(env, err);
            result = NULL;
        }
    }
    for (uint32_t i = 0; i < count * 2 + 2; i++) free(strings[i]);
    free(strings);
    if (!thrown && nul) *absent = 1;
    return result;
}

// ---- the native functions -------------------------------------------------------

// Reads `want` arguments into argv, a view handle first; returns the view.
static kafka_common_metrics_MetricsView_t* view_args(napi_env env, napi_callback_info info, size_t want, napi_value* argv,
                                                     const char* usage) {
    size_t argc = want;
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != want) {
        napi_throw_type_error(env, NULL, usage);
        return NULL;
    }
    return (kafka_common_metrics_MetricsView_t*)handle_unwrap(env, argv[0], &VIEW_TAG, "not a metrics view handle");
}

// metricsViewSize(view): the number of metrics registered now.
napi_value js_metrics_view_size(napi_env env, napi_callback_info info) {
    napi_value argv[1];
    kafka_common_metrics_MetricsView_t* view = view_args(env, info, 1, argv, "metricsViewSize(view)");
    if (view == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    kafka_common_Error_t* err = FFI_SET(size, kafka_common_metrics_MetricsView_len_err(view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_create_int32(env, size, &result), NULL);
    return result;
}

// metricsViewHas(view, name, group, tags): whether a metric is registered now
// under that name.
napi_value js_metrics_view_has(napi_env env, napi_callback_info info) {
    napi_value argv[4];
    kafka_common_metrics_MetricsView_t* view = view_args(env, info, 4, argv, "metricsViewHas(view, name, group, tags)");
    if (view == NULL) return NULL;
    int absent = 0;
    kafka_common_MetricName_t* name = metric_name_build(env, argv[1], argv[2], argv[3], &absent);
    if (name == NULL && !absent) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    int8_t found = 0;
    kafka_common_Error_t* err = NULL;
    if (name != NULL) {
        err = FFI_SET(found, kafka_common_metrics_MetricsView_contains_key_err(view, name, &ffi_err));
        error_warn(env, 1, FFI_ERR(kafka_common_MetricName_destroy_err(name, &ffi_err)));
    }
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, found != 0, &result), NULL);
    return result;
}

// metricsViewGet(view, name, group, tags): the metric registered now under
// that name, as [fields, handle], or null.
napi_value js_metrics_view_get(napi_env env, napi_callback_info info) {
    napi_value argv[4];
    kafka_common_metrics_MetricsView_t* view = view_args(env, info, 4, argv, "metricsViewGet(view, name, group, tags)");
    if (view == NULL) return NULL;
    int absent = 0;
    kafka_common_MetricName_t* name = metric_name_build(env, argv[1], argv[2], argv[3], &absent);
    if (name == NULL && !absent) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_metrics_KafkaMetric_t* metric = NULL;
    kafka_common_Error_t* err = NULL;
    if (name != NULL) {
        err = FFI_SET(metric, kafka_common_metrics_MetricsView_get_err(view, name, &ffi_err));
        error_warn(env, 1, FFI_ERR(kafka_common_MetricName_destroy_err(name, &ffi_err)));
    }
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (metric == NULL) {
        napi_value result;
        NAPI_CHECK(env, napi_get_null(env, &result), NULL);
        return result;
    }
    return metric_entry_take_to_js(env, metric);
}

// metricsViewEntries(view): the metrics registered now, each as [fields,
// handle], from an iterator over the view as it is now; each metric is taken
// from the view while its entry is current, so it outlives the iterator.
napi_value js_metrics_view_entries(napi_env env, napi_callback_info info) {
    napi_value argv[1];
    kafka_common_metrics_MetricsView_t* view = view_args(env, info, 1, argv, "metricsViewEntries(view)");
    if (view == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Iterator_t* it = NULL;
    kafka_common_Error_t* err = FFI_SET(it, kafka_common_metrics_MetricsView_iterator_err(view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value arr;
    napi_value result = NULL;
    if (napi_create_array(env, &arr) == napi_ok) {
        result = arr;
        uint32_t i = 0;
        for (;;) {
            int8_t more = 0;
            error_warn(env, 1, FFI_SET(more, kafka_Iterator_has_next_err(it, &ffi_err)));
            if (!more) break;
            const kafka_Map_Entry_t* entry = NULL;
            err = FFI_SET(entry, (const kafka_Map_Entry_t*)kafka_Iterator_next_err(it, &ffi_err));
            if (err != NULL || entry == NULL) {
                error_warn(env, 1, err);
                break;
            }
            const kafka_common_MetricName_t* name = NULL;
            error_warn(env, 1, FFI_SET(name, (const kafka_common_MetricName_t*)kafka_Map_Entry_key_err(entry, &ffi_err)));
            if (name == NULL) continue;
            kafka_common_metrics_KafkaMetric_t* metric = NULL;
            error_warn(env, 1, FFI_SET(metric, kafka_common_metrics_MetricsView_get_err(view, name, &ffi_err)));
            // Removed since the iterator was taken: left out.
            if (metric == NULL) continue;
            napi_value v = metric_entry_take_to_js(env, metric);
            if (v == NULL || napi_set_element(env, arr, i++, v) != napi_ok) {
                result = NULL;
                break;
            }
        }
    }
    error_warn(env, 1, FFI_ERR(kafka_Iterator_destroy_err(it, &ffi_err)));
    return result;
}

// Reads argv: one metric handle.
static kafka_common_metrics_KafkaMetric_t* metric_arg(napi_env env, napi_callback_info info, const char* usage) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, usage);
        return NULL;
    }
    return (kafka_common_metrics_KafkaMetric_t*)handle_unwrap(env, argv[0], &METRIC_TAG, "not a metric handle");
}

// metricValue(metric): the metric's value, measured now.
napi_value js_metric_value(napi_env env, napi_callback_info info) {
    kafka_common_metrics_KafkaMetric_t* metric = metric_arg(env, info, "metricValue(metric)");
    if (metric == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_Metric_t* as_metric = NULL;
    kafka_common_MetricValue_t* value = NULL;
    kafka_common_Error_t* err = FFI_SET(as_metric, kafka_common_metrics_KafkaMetric__as_Metric_err(metric, &ffi_err));
    if (err == NULL && as_metric != NULL) err = FFI_SET(value, kafka_common_Metric_metric_value_err(as_metric, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return metric_value_take_to_js(env, value);
}

// metricIsMeasurable(metric): whether the metric's provider is a Measurable.
napi_value js_metric_is_measurable(napi_env env, napi_callback_info info) {
    kafka_common_metrics_KafkaMetric_t* metric = metric_arg(env, info, "metricIsMeasurable(metric)");
    if (metric == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    int8_t measurable = 0;
    kafka_common_Error_t* err = FFI_SET(measurable, kafka_common_metrics_KafkaMetric_is_measurable_err(metric, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, measurable != 0, &result), NULL);
    return result;
}

// metricsStats(): the counts of the view and metric handles made and
// destroyed. Test-only.
napi_value js_metrics_stats(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    const char* names[4] = {"viewsMade", "viewsFreed", "metricsMade", "metricsFreed"};
    long counts[4] = {atomic_load(&stat_views_made), atomic_load(&stat_views_freed), atomic_load(&stat_metrics_made),
                      atomic_load(&stat_metrics_freed)};
    for (int i = 0; i < 4; i++) {
        napi_value n;
        NAPI_CHECK(env, napi_create_int64(env, (int64_t)counts[i], &n), NULL);
        NAPI_CHECK(env, napi_set_named_property(env, obj, names[i], n), NULL);
    }
    return obj;
}
