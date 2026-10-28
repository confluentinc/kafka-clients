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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_consumer_group_offsets_with_group_id_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListConsumerGroupOffsetsWithGroupIdOptions(
        SafeAdminClientHandle self, IntPtr groupId, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListConsumerGroupOffsetsWithGroupSpecsOptions(
        SafeAdminClientHandle self, IntPtr groupSpecs, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_alter_consumer_group_offsets_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminAlterConsumerGroupOffsetsWithOptions(
        SafeAdminClientHandle self, IntPtr groupId, IntPtr offsets, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_delete_consumer_group_offsets_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDeleteConsumerGroupOffsetsWithOptions(
        SafeAdminClientHandle self, IntPtr groupId, IntPtr partitions, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_delete_consumer_groups_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDeleteConsumerGroupsWithOptions(
        SafeAdminClientHandle self, IntPtr groupIds, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_remove_members_from_consumer_group_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminRemoveMembersFromConsumerGroupWithOptions(
        SafeAdminClientHandle self, IntPtr groupId, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsResult_partitions_to_offset_and_metadata_with_group_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListConsumerGroupOffsetsResultPartitionsToOffsetAndMetadataWithGroupId(
        IntPtr self, IntPtr groupId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConsumerGroupOffsetsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsSpec_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListConsumerGroupOffsetsSpecNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConsumerGroupOffsetsSpecSetTopicPartitions(
        IntPtr self, IntPtr topicPartitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsSpec_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConsumerGroupOffsetsSpecDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConsumerGroupOffsetsResult_partition_result_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConsumerGroupOffsetsResultPartitionResult(
        IntPtr self, IntPtr partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConsumerGroupOffsetsResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConsumerGroupOffsetsResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConsumerGroupOffsetsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConsumerGroupOffsetsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupOffsetsResult_partition_result", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteConsumerGroupOffsetsResultPartitionResult(
        IntPtr self, IntPtr partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupOffsetsResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteConsumerGroupOffsetsResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupOffsetsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteConsumerGroupOffsetsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupsResult_deleted_groups_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteConsumerGroupsResultDeletedGroups(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteConsumerGroupsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberToRemove_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MemberToRemoveNew(IntPtr groupInstanceId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_MemberToRemove_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MemberToRemoveDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RemoveMembersFromConsumerGroupResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupResult_member_result", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RemoveMembersFromConsumerGroupResultMemberResult(
        IntPtr self, IntPtr member, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RemoveMembersFromConsumerGroupResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListConsumerGroupOffsetsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConsumerGroupOffsetsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsOptions_set_require_stable_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConsumerGroupOffsetsOptionsSetRequireStable(IntPtr self, sbyte requireStable, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListConsumerGroupOffsetsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListConsumerGroupOffsetsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConsumerGroupOffsetsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterConsumerGroupOffsetsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConsumerGroupOffsetsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConsumerGroupOffsetsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterConsumerGroupOffsetsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterConsumerGroupOffsetsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupOffsetsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteConsumerGroupOffsetsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupOffsetsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteConsumerGroupOffsetsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupOffsetsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteConsumerGroupOffsetsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DeleteConsumerGroupsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteConsumerGroupsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DeleteConsumerGroupsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DeleteConsumerGroupsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RemoveMembersFromConsumerGroupOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_with_members", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RemoveMembersFromConsumerGroupOptionsWithMembers(IntPtr members, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RemoveMembersFromConsumerGroupOptionsSetReason(IntPtr self, IntPtr reason, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RemoveMembersFromConsumerGroupOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_RemoveMembersFromConsumerGroupOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RemoveMembersFromConsumerGroupOptionsDestroy(IntPtr self, ref IntPtr outErr);
}
