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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_producers_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeProducersWithOptions(
        SafeAdminClientHandle self, IntPtr partitions, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersResult_partition_result", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeProducersResultPartitionResult(IntPtr self, IntPtr partition, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeProducersResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersResult_PartitionProducerState_active_producers_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr PartitionProducerStateActiveProducers(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_producer_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ProducerStateProducerId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_producer_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerStateProducerEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_last_sequence_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerStateLastSequence(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_last_timestamp_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ProducerStateLastTimestamp(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_coordinator_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ProducerStateCoordinatorEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ProducerState_current_transaction_start_offset_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long ProducerStateCurrentTransactionStartOffset(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeProducersOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeProducersOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersOptions_set_broker_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeProducersOptionsSetBrokerId(IntPtr self, int brokerId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeProducersOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeProducersOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_describe_transactions_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminDescribeTransactionsWithOptions(
        SafeAdminClientHandle self, IntPtr transactionalIds, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTransactionsResult_description", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTransactionsResultDescription(IntPtr self, IntPtr transactionalId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTransactionsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTransactionsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_coordinator_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TransactionDescriptionCoordinatorId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_state_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TransactionDescriptionState(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_producer_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long TransactionDescriptionProducerId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_producer_epoch_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TransactionDescriptionProducerEpoch(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_transaction_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long TransactionDescriptionTransactionTimeoutMs(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_transaction_start_time_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long TransactionDescriptionTransactionStartTimeMs(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionDescription_topic_partitions_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TransactionDescriptionTopicPartitions(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState__enum_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int TransactionStateEnum(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTransactionsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr DescribeTransactionsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTransactionsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTransactionsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_DescribeTransactionsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void DescribeTransactionsOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_abort_transaction_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminAbortTransactionWithOptions(
        SafeAdminClientHandle self, IntPtr spec, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AbortTransactionResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AbortTransactionResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionSpec_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AbortTransactionSpecNew(
        IntPtr topicPartition, long producerId, short producerEpoch, int coordinatorEpoch, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionSpec_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AbortTransactionSpecDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AbortTransactionOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AbortTransactionOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AbortTransactionOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AbortTransactionOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_fence_producers_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminFenceProducersWithOptions(
        SafeAdminClientHandle self, IntPtr transactionalIds, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FenceProducersResult_producer_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr FenceProducersResultProducerId(IntPtr self, IntPtr transactionalId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FenceProducersResult_epoch_id", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr FenceProducersResultEpochId(IntPtr self, IntPtr transactionalId, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FenceProducersResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FenceProducersResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FenceProducersOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr FenceProducersOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FenceProducersOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FenceProducersOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_FenceProducersOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FenceProducersOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_force_terminate_transaction_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminForceTerminateTransactionWithOptions(
        SafeAdminClientHandle self, IntPtr transactionalId, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TerminateTransactionResult_result_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TerminateTransactionResultResult(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TerminateTransactionResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TerminateTransactionResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TerminateTransactionOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TerminateTransactionOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TerminateTransactionOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TerminateTransactionOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TerminateTransactionOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void TerminateTransactionOptionsDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_list_transactions_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminListTransactionsWithOptions(SafeAdminClientHandle self, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsResult_by_broker_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListTransactionsResultByBrokerId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTransactionsResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionListing_transactional_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TransactionListingTransactionalId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionListing_producer_id_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long TransactionListingProducerId(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionListing_state_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TransactionListingState(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_TransactionState_parse_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr TransactionStateParse(IntPtr name, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ListTransactionsOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTransactionsOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filter_states_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTransactionsOptionsFilterStates(IntPtr self, IntPtr states, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filter_producer_ids_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTransactionsOptionsFilterProducerIds(IntPtr self, IntPtr producerIds, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filter_on_duration_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTransactionsOptionsFilterOnDuration(IntPtr self, long durationMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_filter_on_transactional_id_pattern_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTransactionsOptionsFilterOnTransactionalIdPattern(
        IntPtr self, IntPtr pattern, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_ListTransactionsOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ListTransactionsOptionsDestroy(IntPtr self, ref IntPtr outErr);
}
