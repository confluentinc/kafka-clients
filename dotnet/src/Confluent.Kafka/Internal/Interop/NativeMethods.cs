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

/// <summary>
/// The P/Invoke boundary over the Rust core's C ABI: classic Cdecl <c>[DllImport]</c> declarations, as the
/// netstandard2.0 floor requires, each naming its full ABI symbol as its <c>EntryPoint</c>, together with a callback
/// delegate, the metric value-kind constants and small wrappers over the error accessors. A string crosses as an
/// <see cref="IntPtr"/> to UTF-8 bytes, a C <c>bool</c> as <c>[MarshalAs(UnmanagedType.I1)]</c>, and a handle as an
/// <see cref="IntPtr"/>, or as its <see cref="SafeHandle"/> where the marshaller should hold a reference for the call
/// or own a returned handle. The admin declarations are in the other part of the class, <c>NativeMethods.Admin.cs</c>.
/// </summary>
internal static partial class NativeMethods
{
    private const string DllName = "confluent_kafka";

    [DllImport(DllName, EntryPoint = "kafka_common_Error__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ErrorEnum(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_message_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorMessage(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_source_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorSource(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_is_retriable_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ErrorIsRetriableError(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_is_api_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte ErrorIsApiError(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ErrorDestroy(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_kafka_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorKafka(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_kafka_message_source_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorKafkaMessageSource(IntPtr message, IntPtr source, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_local_illegal_argument_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorLocalIllegalArgument(IntPtr message, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_local_illegal_state_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorLocalIllegalState(IntPtr message, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_local_concurrent_modification_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorLocalConcurrentModification(IntPtr message, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_local_timeout_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorLocalTimeout(IntPtr message, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_timeout_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorTimeout(IntPtr message, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_local_callback_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorLocalCallback(
        IntPtr message, IntPtr opaque, ManagedExceptionCarrier.ReleaseCallback release, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_local_callback_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ErrorLocalCallbackError(IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_LocalCallbackError_opaque_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr LocalCallbackErrorOpaque(IntPtr self, ref IntPtr outErr);

    // A panic while freeing an error leaks the error that reports it, as Python's binding does:
    // freeing that one could panic the same way.
    internal static void ErrorDestroy(IntPtr error)
    {
        IntPtr outErr = IntPtr.Zero;
        ErrorDestroy(error, ref outErr);
    }

    internal static int Code(IntPtr error)
    {
        IntPtr outErr = IntPtr.Zero;
        int code = ErrorEnum(error, ref outErr);
        KafkaException.ThrowIfFailed(ref outErr);
        return code;
    }

    internal static IntPtr Message(IntPtr error)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr message = ErrorMessage(error, ref outErr);
        KafkaException.ThrowIfFailed(ref outErr);
        return message;
    }

    /// <summary>The completion of <c>kafka_common_KafkaFuture_get_cb</c>: <c>value</c> is borrowed (NULL for a Void future or on failure); <c>error</c> is NULL on success, else owned and freed by the callee.</summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void KafkaFutureGetCallback(IntPtr value, IntPtr error, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_string_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void StringDestroy(IntPtr s, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_Map_size_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MapSize(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_Map_key_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MapKey(IntPtr self, int index, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_Map_value_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MapValue(IntPtr self, int index, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_Map_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MapDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_List_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_List_add_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListAdd(IntPtr self, IntPtr element, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_List_size_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ListSize(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_List_get_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListGet(IntPtr self, int index, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_List_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricName_name_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricNameName(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricName_group_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricNameGroup(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricName_description_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricNameDescription(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricName_tags_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricNameTags(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_metrics_KafkaMetric__as_Metric_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaMetricAsMetric(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Metric_metric_value_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricMetricValue(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricValue__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MetricValueEnum(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricValue_as_double_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern double MetricValueAsDouble(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricValue_as_string_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MetricValueAsString(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricValue_as_long_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long MetricValueAsLong(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricValue_as_int_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int MetricValueAsInt(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_MetricValue_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MetricValueDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Node_with_rack_is_fenced_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NodeWithRackIsFenced(
        int id, IntPtr host, int port, IntPtr rack, sbyte isFenced, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Node_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int NodeId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Node_host_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NodeHost(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Node_port_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int NodePort(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Node_rack_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NodeRack(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Node_is_fenced_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte NodeIsFenced(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Node_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void NodeDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_with_offline_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoWithOfflineReplicas(
        IntPtr topic,
        int partition,
        IntPtr leader,
        IntPtr replicas,
        IntPtr inSyncReplicas,
        IntPtr offlineReplicas,
        ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_topic_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoTopic(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_partition_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int PartitionInfoPartition(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_leader_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoLeader(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoReplicas(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_in_sync_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoInSyncReplicas(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_offline_replicas_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionInfoOfflineReplicas(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_PartitionInfo_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void PartitionInfoDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Cluster_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClusterNew(
        IntPtr clusterId,
        IntPtr nodes,
        IntPtr partitions,
        IntPtr unauthorizedTopics,
        IntPtr internalTopics,
        ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Cluster_partitions_for_topic_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ClusterPartitionsForTopic(IntPtr self, IntPtr topic, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Cluster_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ClusterDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaFuture_get_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void KafkaFutureGetCb(IntPtr self, KafkaFutureGetCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaFuture_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void KafkaFutureDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_Error_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaErrorNew(int code, IntPtr message);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerConfig_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerConfigNew(IntPtr props, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerConfig_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerConfigDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_KafkaConsumer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaConsumerNew(
        IntPtr config, IntPtr keyDeserializer, IntPtr valueDeserializer, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_KafkaConsumer__as_Consumer_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaConsumerAsConsumer(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_KafkaConsumer_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void KafkaConsumerDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerNew(IntPtr offsetResetStrategy, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer__as_Consumer_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockConsumerAsConsumer(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerClose(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer__execute_callbacks_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerExecuteCallbacks(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe_with_topics", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSubscribeWithTopics(SafeConsumerHandle self, IntPtr topics, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_unsubscribe", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerUnsubscribe(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assign", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerAssign(SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_pause", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPause(SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_resume", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerResume(SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_with_offset", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekWithOffset(
        SafeConsumerHandle self, IntPtr partition, long offset, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_with_offset_and_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekWithOffsetAndMetadata(
        SafeConsumerHandle self, IntPtr partition, IntPtr offsetAndMetadata, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_beginning", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekToBeginning(SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_end", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekToEnd(SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_position", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerPosition(SafeConsumerHandle self, IntPtr partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_position_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerPositionWithTimeout(
        SafeConsumerHandle self, IntPtr partition, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSync(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncWithTimeout(SafeConsumerHandle self, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_with_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncWithOffsets(
        SafeConsumerHandle self, IntPtr offsets, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_with_offsets_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncWithOffsetsTimeout(
        SafeConsumerHandle self, IntPtr offsets, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitAsync(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_committed", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommitted(SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_committed_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerCommittedWithTimeout(
        SafeConsumerHandle self, IntPtr partitions, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_offsets_for_times", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerOffsetsForTimes(
        SafeConsumerHandle self, IntPtr timestampsToSearch, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_offsets_for_times_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerOffsetsForTimesWithTimeout(
        SafeConsumerHandle self, IntPtr timestampsToSearch, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_beginning_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerBeginningOffsets(
        SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_beginning_offsets_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerBeginningOffsetsWithTimeout(
        SafeConsumerHandle self, IntPtr partitions, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_end_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerEndOffsets(SafeConsumerHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_end_offsets_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerEndOffsetsWithTimeout(
        SafeConsumerHandle self, IntPtr partitions, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_partitions_for", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPartitionsFor(SafeConsumerHandle self, IntPtr topic, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_partitions_for_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPartitionsForWithTimeout(
        SafeConsumerHandle self, IntPtr topic, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_list_topics", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerListTopics(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_list_topics_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerListTopicsWithTimeout(
        SafeConsumerHandle self, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_enforce_rebalance", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerEnforceRebalance(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_enforce_rebalance_with_reason", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerEnforceRebalanceWithReason(
        SafeConsumerHandle self, IntPtr reason, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close_with_options", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCloseWithOptions(SafeConsumerHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_current_lag", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerCurrentLag(SafeConsumerHandle self, IntPtr topicPartition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_metrics_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerMetrics(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer__set_callbacks_notify_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSetCallbacksNotify(
        SafeConsumerHandle self, CompletionThread.NotifyCallback notify, IntPtr opaque, ref IntPtr outErr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ConsumerVoidCallback(IntPtr error, IntPtr opaque);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ConsumerScalarCallback(long value, IntPtr error, IntPtr opaque);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ConsumerPointerCallback(IntPtr value, IntPtr error, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe_with_topics_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSubscribeWithTopicsCb(
        SafeConsumerHandle self, IntPtr topics, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_unsubscribe_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerUnsubscribeCb(SafeConsumerHandle self, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assign_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerAssignCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_pause_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPauseCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_resume_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerResumeCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_beginning_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekToBeginningCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_to_end_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekToEndCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_with_offset_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekWithOffsetCb(
        SafeConsumerHandle self, IntPtr partition, long offset, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_poll_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPollCb(
        SafeConsumerHandle self, long timeout, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_position_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPositionCb(
        SafeConsumerHandle self, IntPtr partition, ConsumerScalarCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncCb(SafeConsumerHandle self, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_with_offsets_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncWithOffsetsCb(
        SafeConsumerHandle self, IntPtr offsets, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_committed_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommittedCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_offsets_for_times_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerOffsetsForTimesCb(
        SafeConsumerHandle self, IntPtr timestampsToSearch, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_beginning_offsets_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerBeginningOffsetsCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_end_offsets_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerEndOffsetsCb(
        SafeConsumerHandle self, IntPtr partitions, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_partitions_for_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPartitionsForCb(
        SafeConsumerHandle self, IntPtr topic, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_list_topics_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerListTopicsCb(SafeConsumerHandle self, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCloseCb(SafeConsumerHandle self, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_close_with_options_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCloseWithOptionsCb(
        SafeConsumerHandle self, IntPtr options, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_with_offset_and_metadata_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSeekWithOffsetAndMetadataCb(
        SafeConsumerHandle self, IntPtr partition, IntPtr offsetAndMetadata, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_current_lag_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCurrentLagCb(
        SafeConsumerHandle self, IntPtr topicPartition, ConsumerScalarCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitAsyncCb(SafeConsumerHandle self, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncWithTimeoutCb(
        SafeConsumerHandle self, long timeout, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitSyncWithOffsetsTimeoutCb(
        SafeConsumerHandle self, IntPtr offsets, long timeout, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_position_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPositionWithTimeoutCb(
        SafeConsumerHandle self, IntPtr partition, long timeout, ConsumerScalarCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_committed_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommittedWithTimeoutCb(
        SafeConsumerHandle self, IntPtr partitions, long timeout, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_partitions_for_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPartitionsForWithTimeoutCb(
        SafeConsumerHandle self, IntPtr topic, long timeout, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_list_topics_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerListTopicsWithTimeoutCb(
        SafeConsumerHandle self, long timeout, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_offsets_for_times_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerOffsetsForTimesWithTimeoutCb(
        SafeConsumerHandle self, IntPtr timestampsToSearch, long timeout, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_beginning_offsets_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerBeginningOffsetsWithTimeoutCb(
        SafeConsumerHandle self, IntPtr partitions, long timeout, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_end_offsets_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerEndOffsetsWithTimeoutCb(
        SafeConsumerHandle self, IntPtr partitions, long timeout, ConsumerPointerCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartition_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionNew(IntPtr topic, int partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartition_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TopicPartitionDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetAndMetadataWithLeaderEpochMetadata(
        long offset, int leaderEpoch, IntPtr metadata, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_offset_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long OffsetAndMetadataOffset(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_metadata_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetAndMetadataMetadata(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_leader_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int OffsetAndMetadataLeaderEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndMetadata_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void OffsetAndMetadataDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestamp_offset_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long OffsetAndTimestampOffset(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestamp_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long OffsetAndTimestampTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetAndTimestamp_leader_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int OffsetAndTimestampLeaderEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_CloseOptions_GroupMembershipOperation_leave_group_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CloseOptionsLeaveGroup(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CloseOptionsRemainInGroup(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_CloseOptions_GroupMembershipOperation_default_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CloseOptionsDefault(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_CloseOptions_with_operation_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CloseOptionsWithOperation(IntPtr operation, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_CloseOptions_with_timeout_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CloseOptionsWithTimeout(IntPtr self, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_CloseOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void CloseOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_update_beginning_offsets_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerUpdateBeginningOffsets(IntPtr self, IntPtr newOffsets, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_update_end_offsets_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerUpdateEndOffsets(IntPtr self, IntPtr newOffsets, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_update_partitions", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerUpdatePartitions(
        IntPtr self, IntPtr topic, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_wakeup_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerWakeup(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_group_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadata(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_group_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadataGroupId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_generation_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerGroupMetadataGenerationId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_member_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadataMemberId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_group_instance_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerGroupMetadataGroupInstanceId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerGroupMetadata_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerGroupMetadataDestroy(IntPtr self, ref IntPtr outErr);

    internal const int MetricValueKindString = 1;

    internal const int MetricValueKindLong = 2;

    internal const int MetricValueKindInt = 3;

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_count_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordsCount(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordsPartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_records_with_partition_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordsRecordsWithPartition(IntPtr self, IntPtr partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecords_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRecordsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordNew(IntPtr topic, int partition, long offset, IntPtr key, IntPtr value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_offset_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerRecordOffset(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerRecordTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_timestamp_type_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordTimestampType(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_serialized_key_size_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordSerializedKeySize(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_serialized_value_size_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordSerializedValueSize(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_key_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordKey(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_value_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordValue(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_headers_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRecordHeaders(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_leader_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ConsumerRecordLeaderEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRecord_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRecordDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_record_TimestampType__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TimestampTypeEnum(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_header_internals_RecordHeaders__as_Headers_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RecordHeadersAsHeaders(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_header_Headers_to_array_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr HeadersToArray(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_header_internals_RecordHeader__as_Header_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RecordHeaderAsHeader(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_header_Header_key_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr HeaderKey(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_header_Header_value_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern KafkaBytes HeaderValue(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_add_record", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerAddRecord(IntPtr self, IntPtr record, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_set_poll_error_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerSetPollError(IntPtr self, IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_rebalance", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerRebalance(IntPtr self, IntPtr newAssignment, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_rebalance_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerRebalanceCb(
        IntPtr self, IntPtr newAssignment, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_lose_partitions", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerLosePartitions(IntPtr self, IntPtr partitionsLost, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_MockConsumer_lose_partitions_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockConsumerLosePartitionsCb(
        IntPtr self, IntPtr partitionsLost, ConsumerVoidCallback cb, IntPtr opaque);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ConsumerRebalanceListenerMethod(IntPtr self, IntPtr partitions, long callbackId);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRebalanceListener_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerRebalanceListenerNew(
        IntPtr self,
        ConsumerRebalanceListenerMethod onPartitionsRevoked,
        ConsumerRebalanceListenerMethod onPartitionsAssigned,
        ConsumerRebalanceListenerMethod onPartitionsLost,
        ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRebalanceListenerOnPartitionsRevoked(
        IntPtr self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRebalanceListenerOnPartitionsAssigned(
        IntPtr self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRebalanceListener_on_partitions_lost", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRebalanceListenerOnPartitionsLost(
        IntPtr self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerRebalanceListener_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerRebalanceListenerDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe_with_topics_listener", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSubscribeWithTopicsListener(
        SafeConsumerHandle self, IntPtr topics, IntPtr listener, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscribe_with_topics_listener_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSubscribeWithTopicsListenerCb(
        SafeConsumerHandle self, IntPtr topics, IntPtr listener, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer__set_callback_result_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerSetCallbackResult(
        IntPtr self, long callbackId, IntPtr result, IntPtr error, ref IntPtr outErr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void OffsetCommitCallbackOnComplete(IntPtr self, IntPtr offsets, IntPtr error, long callbackId);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetCommitCallback_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OffsetCommitCallbackNew(
        IntPtr self, OffsetCommitCallbackOnComplete onComplete, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetCommitCallback_on_complete", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void OffsetCommitCallbackInvokeOnComplete(
        IntPtr self, IntPtr offsets, IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_OffsetCommitCallback_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void OffsetCommitCallbackDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitAsyncWithCallback(
        SafeConsumerHandle self, IntPtr callback, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async_with_callback_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitAsyncWithCallbackCb(
        SafeConsumerHandle self, IntPtr callback, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async_with_offsets_callback", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitAsyncWithOffsetsCallback(
        SafeConsumerHandle self, IntPtr offsets, IntPtr callback, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async_with_offsets_callback_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerCommitAsyncWithOffsetsCallbackCb(
        SafeConsumerHandle self, IntPtr offsets, IntPtr callback, ConsumerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_assignment", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerAssignment(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_subscription", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerSubscription(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_paused", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPaused(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_poll", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPoll(SafeConsumerHandle self, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartition_topic_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TopicPartitionTopic(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_TopicPartition_partition_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TopicPartitionPartition(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_Map_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MapNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_Map_put_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MapPut(IntPtr self, IntPtr key, IntPtr value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerConfigNew(IntPtr props, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerConfigDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_KafkaProducer_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaProducerNew(
        IntPtr config, IntPtr keySerializer, IntPtr valueSerializer, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_KafkaProducer__as_Producer_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaProducerAsProducer(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_KafkaProducer_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void KafkaProducerDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer__execute_callbacks_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerExecuteCallbacks(SafeProducerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer__set_callbacks_notify_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerSetCallbacksNotify(
        SafeProducerHandle self, CompletionThread.NotifyCallback notify, IntPtr opaque, ref IntPtr outErr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ProducerVoidCallback(IntPtr error, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_close_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerCloseCb(SafeProducerHandle self, ProducerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_close_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerCloseWithTimeoutCb(
        SafeProducerHandle self, long timeout, ProducerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_flush_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerFlushCb(SafeProducerHandle self, ProducerVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_flush", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerFlush(SafeProducerHandle self, ref IntPtr outErr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ProducerListCallback(IntPtr value, IntPtr error, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_partitions_for_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerPartitionsForCb(
        SafeProducerHandle self, IntPtr topic, ProducerListCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_partitions_for", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerPartitionsFor(SafeProducerHandle self, IntPtr topic, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_metrics_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerMetrics(SafeProducerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_with_partition_timestamp_key", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerRecordWithPartitionTimestampKey(
        IntPtr topic, int partition, long timestamp, IntPtr key, IntPtr value, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_ProducerRecord_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerRecordDestroy(IntPtr self, ref IntPtr outErr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CallbackOnCompletion(IntPtr self, IntPtr metadata, IntPtr error, long callbackId);

    [DllImport(DllName, EntryPoint = "kafka_producer_Callback_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CallbackNew(IntPtr self, CallbackOnCompletion onCompletion, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer__set_callback_result_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerSetCallbackResult(
        SafeProducerHandle self, long callbackId, IntPtr result, IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_send_with_callback", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ProducerSendWithCallback(
        SafeProducerHandle self, IntPtr record, IntPtr callback, ref IntPtr outErr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ProducerSendCallback(IntPtr future, IntPtr error, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_Producer_send_with_callback_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ProducerSendWithCallbackCb(
        SafeProducerHandle self, IntPtr record, IntPtr callback, ProducerSendCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_offset_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long RecordMetadataOffset(IntPtr metadata, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_partition_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int RecordMetadataPartition(IntPtr metadata, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_topic_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr RecordMetadataTopic(IntPtr metadata, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_RecordMetadata_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long RecordMetadataTimestamp(IntPtr metadata, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_with_auto_complete_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockProducerWithAutoComplete(sbyte autoComplete, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer__as_Producer_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockProducerAsProducer(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockProducerDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_history_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr MockProducerHistory(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_clear_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void MockProducerClear(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_complete_next_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte MockProducerCompleteNext(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_error_next_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte MockProducerErrorNext(IntPtr self, IntPtr error, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_handle_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern SafeConsumerReentrancyHandle ConsumerGetHandle(SafeConsumerHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_wakeup_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleWakeup(SafeConsumerReentrancyHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_assignment_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleAssignment(SafeConsumerReentrancyHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_paused_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandlePaused(SafeConsumerReentrancyHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek_with_offset", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleSeekWithOffset(
        SafeConsumerReentrancyHandle self, IntPtr partition, long offset, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleSeekWithOffsetAndMetadata(
        SafeConsumerReentrancyHandle self, IntPtr partition, IntPtr offsetAndMetadata, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek_to_beginning", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleSeekToBeginning(
        SafeConsumerReentrancyHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_seek_to_end", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleSeekToEnd(SafeConsumerReentrancyHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_pause", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandlePause(SafeConsumerReentrancyHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_resume", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleResume(SafeConsumerReentrancyHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_position", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerHandlePosition(SafeConsumerReentrancyHandle self, IntPtr partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_position_with_timeout", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerHandlePositionWithTimeout(
        SafeConsumerReentrancyHandle self, IntPtr partition, long timeout, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_committed", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleCommitted(SafeConsumerReentrancyHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_beginning_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleBeginningOffsets(
        SafeConsumerReentrancyHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_end_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleEndOffsets(SafeConsumerReentrancyHandle self, IntPtr partitions, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_offsets_for_times", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerHandleOffsetsForTimes(
        SafeConsumerReentrancyHandle self, IntPtr timestampsToSearch, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_sync", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleCommitSync(SafeConsumerReentrancyHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_sync_with_offsets", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleCommitSyncWithOffsets(
        SafeConsumerReentrancyHandle self, IntPtr offsets, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_async", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleCommitAsync(SafeConsumerReentrancyHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_async_with_callback", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleCommitAsyncWithCallback(
        SafeConsumerReentrancyHandle self, IntPtr callback, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerHandleCommitAsyncWithOffsetsCallback(
        SafeConsumerReentrancyHandle self, IntPtr offsets, IntPtr callback, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerHandle_current_lag", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ConsumerHandleCurrentLag(
        SafeConsumerReentrancyHandle self, IntPtr topicPartition, ref IntPtr outErr);
}
