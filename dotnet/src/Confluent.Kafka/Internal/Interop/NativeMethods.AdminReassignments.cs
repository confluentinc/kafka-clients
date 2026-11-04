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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_elect_leaders_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminElectLeadersWithOptions(
        SafeAdminClientHandle self, IntPtr electionType, IntPtr partitions, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ElectionType_preferred_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ElectionTypePreferred(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_ElectionType_unclean_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ElectionTypeUnclean(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersResult_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ElectLeadersResultPartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ElectLeadersResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ElectLeadersOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ElectLeadersOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ElectLeadersOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ElectLeadersOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_alter_partition_reassignments_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminAlterPartitionReassignmentsWithOptions(
        SafeAdminClientHandle self, IntPtr reassignments, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitionReassignment_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NewPartitionReassignmentNew(IntPtr targetReplicas, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitionReassignment_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NewPartitionReassignmentDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterPartitionReassignmentsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterPartitionReassignmentsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterPartitionReassignmentsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterPartitionReassignmentsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsOptions_set_allow_replication_factor_change_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterPartitionReassignmentsOptionsSetAllowReplicationFactorChange(
        IntPtr self, sbyte allow, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterPartitionReassignmentsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterPartitionReassignmentsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_partition_reassignments_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListPartitionReassignmentsWithOptions(
        SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_partition_reassignments_with_partitions_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListPartitionReassignmentsWithPartitionsOptions(
        SafeAdminClientHandle self, IntPtr partitions, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsResult_reassignments_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListPartitionReassignmentsResultReassignments(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListPartitionReassignmentsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListPartitionReassignmentsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListPartitionReassignmentsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListPartitionReassignmentsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListPartitionReassignmentsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionReassignmentReplicas(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_adding_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionReassignmentAddingReplicas(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_PartitionReassignment_removing_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionReassignmentRemovingReplicas(IntPtr self, ref IntPtr outErr);
}
