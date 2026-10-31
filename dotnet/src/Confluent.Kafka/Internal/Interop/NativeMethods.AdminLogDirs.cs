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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_log_dirs_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeLogDirsWithOptions(
        SafeAdminClientHandle self, IntPtr brokers, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsResult_descriptions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeLogDirsResultDescriptions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeLogDirsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeLogDirsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeLogDirsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeLogDirsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LogDirDescriptionErrorOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_replica_infos_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LogDirDescriptionReplicaInfos(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_total_bytes_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long LogDirDescriptionTotalBytesOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_usable_bytes_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long LogDirDescriptionUsableBytesOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_is_cordoned_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte LogDirDescriptionIsCordonedOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaInfo_size_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ReplicaInfoSize(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaInfo_offset_lag_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ReplicaInfoOffsetLag(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaInfo_is_future_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ReplicaInfoIsFuture(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_alter_replica_log_dirs_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminAlterReplicaLogDirsWithOptions(
        SafeAdminClientHandle self, IntPtr replicaAssignment, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterReplicaLogDirsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterReplicaLogDirsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AlterReplicaLogDirsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterReplicaLogDirsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AlterReplicaLogDirsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_replica_log_dirs_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeReplicaLogDirsWithOptions(
        SafeAdminClientHandle self, IntPtr replicas, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_values_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeReplicaLogDirsResultValues(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeReplicaLogDirsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ReplicaLogDirInfoCurrentReplicaLogDirOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_offset_lag_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ReplicaLogDirInfoCurrentReplicaOffsetLagOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ReplicaLogDirInfoFutureReplicaLogDirOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_offset_lag_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ReplicaLogDirInfoFutureReplicaOffsetLagOf(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeReplicaLogDirsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeReplicaLogDirsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeReplicaLogDirsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionReplica_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionReplicaNew(IntPtr topic, int partition, int brokerId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionReplica_topic_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionReplicaTopic(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionReplica_partition_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionReplicaPartition(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionReplica_broker_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionReplicaBrokerId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartitionReplica_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TopicPartitionReplicaDestroy(IntPtr self, ref IntPtr outErr);
}
