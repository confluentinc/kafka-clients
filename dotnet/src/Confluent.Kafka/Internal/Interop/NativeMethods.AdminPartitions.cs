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

using System;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

internal static partial class NativeMethods
{
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_create_partitions_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminCreatePartitionsWithOptions(
        SafeAdminClientHandle self, IntPtr newPartitions, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_increase_to_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NewPartitionsIncreaseTo(int totalCount, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_increase_to_with_new_assignments_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NewPartitionsIncreaseToWithNewAssignments(int totalCount, IntPtr newAssignments, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewPartitionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreatePartitionsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreatePartitionsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CreatePartitionsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreatePartitionsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_set_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreatePartitionsOptionsSetValidateOnly(IntPtr self, sbyte validateOnly, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_set_retry_on_quota_violation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreatePartitionsOptionsSetRetryOnQuotaViolation(
        IntPtr self, sbyte retryOnQuotaViolation, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CreatePartitionsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_delete_records_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDeleteRecordsWithOptions(
        SafeAdminClientHandle self, IntPtr recordsToDelete, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RecordsToDelete_with_offset_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RecordsToDeleteWithOffset(long offset, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RecordsToDelete_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RecordsToDeleteDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_low_watermarks_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteRecordsResultLowWatermarks(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteRecordsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeletedRecords_low_watermark_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long DeletedRecordsLowWatermark(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteRecordsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteRecordsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteRecordsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_offsets_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListOffsetsWithOptions(
        SafeAdminClientHandle self, IntPtr topicPartitionOffsets, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_earliest_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetSpecEarliest(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_latest_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetSpecLatest(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_max_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetSpecMaxTimestamp(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_earliest_local_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetSpecEarliestLocal(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_latest_tiered_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetSpecLatestTiered(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_earliest_pending_upload_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetSpecEarliestPendingUpload(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_for_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetSpecForTimestamp(long timestamp, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void OffsetSpecDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_partition_result", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListOffsetsResultPartitionResult(IntPtr self, IntPtr partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListOffsetsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ListOffsetsResultInfoOffset(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ListOffsetsResultInfoTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListOffsetsResultInfoLeaderEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListOffsetsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsOptions_with_isolation_level_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListOffsetsOptionsWithIsolationLevel(IntPtr isolationLevel, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListOffsetsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListOffsetsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_IsolationLevel_read_uncommitted_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr IsolationLevelReadUncommitted(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_IsolationLevel_read_committed_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr IsolationLevelReadCommitted(ref IntPtr outErr);
}
