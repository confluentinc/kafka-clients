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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the table of the FFI's error views in rust/xtask/src/error_hierarchy/js.rs;
// `cargo xtask check-generated` checks it is up to date.
//
// The payload of an error the core reports (root CLAUDE.md, JavaScript Binding
// Conventions, Errors, Q39): the FFI's view of its class
// (kafka_common_Error_<class>, NULL for another class), then the view's
// accessors, each field set on a plain object by native/error.c's payload_*
// helper, keyed by its Java field name. NULL when the class has none.

#include "util.h"

napi_value error_payload_to_js(napi_env env, int code, const kafka_common_Error_t* e) {
    kafka_common_Error_t* ffi_err = NULL;
    napi_value obj = NULL;
    switch (code) {
    case kafka_common_Error_TOPIC_AUTHORIZATION_FAILED: {
        const kafka_common_TopicAuthorizationError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_topic_authorization_error_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            kafka_List_t* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_TopicAuthorizationError_unauthorized_topics_err(view, &ffi_err));
            if (payload_string_list(env, obj, "unauthorizedTopics", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_INVALID_TOPIC_ERROR: {
        const kafka_common_InvalidTopicError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_invalid_topic_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            kafka_List_t* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_InvalidTopicError_invalid_topics_err(view, &ffi_err));
            if (payload_string_list(env, obj, "invalidTopics", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_GROUP_AUTHORIZATION_FAILED: {
        const kafka_common_GroupAuthorizationError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_group_authorization_error_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            const char* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_GroupAuthorizationError_group_id_err(view, &ffi_err));
            if (payload_string(env, obj, "groupId", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_THROTTLING_QUOTA_EXCEEDED: {
        const kafka_common_ThrottlingQuotaExceededError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_throttling_quota_exceeded_error_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            int32_t value = 0;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_ThrottlingQuotaExceededError_throttle_time_ms_err(view, &ffi_err));
            if (payload_int32(env, obj, "throttleTimeMs", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_MESSAGE_TOO_LARGE: {
        const kafka_common_RecordTooLargeError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_record_too_large_error_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            kafka_Map_t* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_RecordTooLargeError_record_too_large_partitions_err(view, &ffi_err));
            if (payload_topic_partition_long_map(env, obj, "recordTooLargePartitions", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_RESOURCE_NOT_FOUND: {
        const kafka_common_ResourceNotFoundError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_resource_not_found_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            const char* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_ResourceNotFoundError_resource_err(view, &ffi_err));
            if (payload_string(env, obj, "resource", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_DUPLICATE_RESOURCE: {
        const kafka_common_DuplicateResourceError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_duplicate_resource_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            const char* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_DuplicateResourceError_resource_err(view, &ffi_err));
            if (payload_string(env, obj, "resource", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_CONSUMER_OFFSET_OUT_OF_RANGE: {
        const kafka_common_ConsumerOffsetOutOfRangeError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_consumer_offset_out_of_range_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            kafka_Map_t* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions_err(view, &ffi_err));
            if (payload_topic_partition_long_map(env, obj, "offsetOutOfRangePartitions", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_CONSUMER_NO_OFFSET_FOR_PARTITION: {
        const kafka_common_ConsumerNoOffsetForPartitionError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_consumer_no_offset_for_partition_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            kafka_List_t* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_ConsumerNoOffsetForPartitionError_partitions_err(view, &ffi_err));
            if (payload_topic_partition_list(env, obj, "partitions", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_CONSUMER_LOG_TRUNCATION: {
        const kafka_common_ConsumerLogTruncationError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_consumer_log_truncation_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            kafka_Map_t* value = NULL;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions_err(view, &ffi_err));
            if (payload_topic_partition_long_map(env, obj, "offsetOutOfRangePartitions", value, failed)) return NULL;
        }
        return obj;
    }
    case kafka_common_Error_QUOTA_VIOLATION: {
        const kafka_common_QuotaViolationError_t* view = NULL;
        error_discard(FFI_SET(view, kafka_common_Error_quota_violation_err(e, &ffi_err)));
        if (view == NULL || napi_create_object(env, &obj) != napi_ok) return NULL;
        {
            double value = 0.0;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_QuotaViolationError_value_err(view, &ffi_err));
            if (payload_double(env, obj, "value", value, failed)) return NULL;
        }
        {
            double value = 0.0;
            kafka_common_Error_t* failed = FFI_SET(value, kafka_common_QuotaViolationError_bound_err(view, &ffi_err));
            if (payload_double(env, obj, "bound", value, failed)) return NULL;
        }
        return obj;
    }
    default:
        return NULL;
    }
}
