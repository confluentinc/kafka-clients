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

using System;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

// The payload views of a core error and their accessors. A view is a const pointer borrowed from its error, NULL when
// the error is another class, and is never destroyed. A list or map accessor returns an owned container, which owns its
// elements; a string accessor returns a borrowed string.
internal static partial class NativeMethods
{
    [DllImport(DllName, EntryPoint = "kafka_common_Error_topic_authorization_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorTopicAuthorizationView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_group_authorization_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorGroupAuthorizationView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_invalid_topic_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorInvalidTopicView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_resource_not_found_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorResourceNotFoundView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_duplicate_resource_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorDuplicateResourceView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_record_too_large_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorRecordTooLargeView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_throttling_quota_exceeded_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorThrottlingQuotaExceededView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_quota_violation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorQuotaViolationView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_consumer_offset_out_of_range_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorConsumerOffsetOutOfRangeView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_consumer_log_truncation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorConsumerLogTruncationView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_consumer_no_offset_for_partition_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorConsumerNoOffsetForPartitionView(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicAuthorizationError_unauthorized_topics_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicAuthorizationErrorUnauthorizedTopics(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_GroupAuthorizationError_group_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr GroupAuthorizationErrorGroupId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_InvalidTopicError_invalid_topics_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr InvalidTopicErrorInvalidTopics(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ResourceNotFoundError_resource_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ResourceNotFoundErrorResource(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_DuplicateResourceError_resource_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DuplicateResourceErrorResource(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_RecordTooLargeError_record_too_large_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RecordTooLargeErrorRecordTooLargePartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ThrottlingQuotaExceededError_throttle_time_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ThrottlingQuotaExceededErrorThrottleTimeMs(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_QuotaViolationError_metric_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr QuotaViolationErrorMetricName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_QuotaViolationError_value_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern double QuotaViolationErrorValue(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_QuotaViolationError_bound_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern double QuotaViolationErrorBound(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerOffsetOutOfRangeErrorOffsetOutOfRangePartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerLogTruncationErrorOffsetOutOfRangePartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ConsumerLogTruncationError_divergent_offsets_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerLogTruncationErrorDivergentOffsets(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ConsumerNoOffsetForPartitionError_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerNoOffsetForPartitionErrorPartitions(IntPtr self, ref IntPtr outErr);
}
